package internal

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"os"
	"path/filepath"
	"time"
)

type UploadProvider interface {
	UploadFile(context context.Context, reader io.Reader, remoteKey string) error
}

type S3Provider struct {
	S3Client *s3.Client
	Bucket   string
}

func (p *S3Provider) UploadFile(context context.Context, reader io.Reader, remoteKey string) error {
	_, err := p.S3Client.PutObject(context, &s3.PutObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(remoteKey),
		Body:   reader,
	})
	return err
}

func NewS3Provider(ctx context.Context, bucket string) (UploadProvider, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)
	return &S3Provider{
		S3Client: client,
		Bucket:   bucket,
	}, nil
}

// ProcessUploadingFilesFromChannel listens for messages on uploadch and upload files to the UploadProvider
// once a file is uploaded its satus is persisted to the DB and the local file removed.
func ProcessUploadingFilesFromChannel(context context.Context,
	provider UploadProvider,
	tracker CollectionFileTracker,
	uploadch <-chan string) error {

	for {
		select {
		case <-context.Done():
			return context.Err()
		case localFile, ok := <-uploadch:
			if !ok {
				return nil
			}
			if localFile == "" {
				continue
			}
			// check if file exists
			err := processFileToUpload(context, provider, tracker, localFile)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func processFileToUpload(ctx context.Context, provider UploadProvider, tracker CollectionFileTracker, metaDataFileName string) error {
	metaFileData, err := ParseUploadFileName(metaDataFileName)
	if err != nil {
		return err
	}
	t := time.Unix(0, metaFileData.Nanos)
	remoteFile := fmt.Sprintf("%s/%s/%s/%s",
		metaFileData.DB,
		metaFileData.Collection,
		t.Format("year=2006/month=01/day=02/hour=15"),
		filepath.Base(metaFileData.SourceFileName),
	)

	trackingRecord := CollectionFileTrackingRecord{
		DB:             metaFileData.DB,
		Collection:     metaFileData.Collection,
		RemoteFile:     remoteFile,
		Status:         "pending",
		Message:        "",
		StartTokenB64:  metaFileData.StartTokenB64,
		ResumeTokenB64: metaFileData.ResumeTokenB64,
	}

	f, uploadErr := os.Open(metaFileData.SourceFileName)
	if uploadErr != nil {
		trackingRecord.Status = "error"
		trackingRecord.Message = uploadErr.Error()
	} else {
		defer func() {
			errClose := f.Close()
			if errClose != nil {
				fmt.Println(TraceErr(errClose))
			}
		}()

		uploadErr = provider.UploadFile(ctx, f, remoteFile)
		if uploadErr != nil {
			trackingRecord.Status = "error"
			trackingRecord.Message = uploadErr.Error()
		}

	}

	if uploadErr == nil {
		trackingRecord.Status = "success"
	}
	err = tracker.UpdateFileStatus(trackingRecord)
	if err != nil {
		return TraceErr(err)
	}
	if uploadErr == nil {
		err = os.Remove(metaFileData.SourceFileName)
		if err != nil {
			fmt.Println("error removing file", metaFileData.SourceFileName, err)
		}
		err = os.Remove(metaDataFileName)
		if err != nil {
			fmt.Println("error removing file", metaDataFileName, err)
		}
	}
	return nil
}
