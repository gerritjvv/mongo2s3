package internal

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"os"
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

func uploadToS3(context context.Context, config Config,
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
			err := processFileToUpload(context, config, provider, tracker, localFile)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func processFileToUpload(ctx context.Context, config Config, provider UploadProvider, tracker CollectionFileTracker, file string) error {

	metaFileData, err := ParseUploadFileName(file)
	if err != nil {
		return err
	}

	remoteFile := TODO Make remote file name here
	trackingRecord := CollectionFileTrackingRecord{
		DB:             metaFileData.DB,
		Collection:     metaFileData.Collection,
		RemoteFile:     remoteFile,
		Status:         "success",
		Message:        "",
		StartTokenB64:  metaFileData.StartTokenB64,
		ResumeTokenB64: metaFileData.ResumeTokenB64,
	}

	f, err := os.Open(metaFileData.SourceFileName)

	if err != nil {
		trackingRecord.Status = "error"
		trackingRecord.Message = err.Error()
	} else {
		defer func() {
			errClose := f.Close()
			if errClose != nil {
				fmt.Println(TraceErr(errClose))
			}
		}()

		err = provider.UploadFile(ctx, f, remoteFile)
		if err != nil {
			trackingRecord.Status = "error"
			trackingRecord.Message = err.Error()
		}

	}
	err = tracker.UpdateFileStatus(trackingRecord)
	if err != nil {
		return TraceErr(err)
	}
	return nil
}
