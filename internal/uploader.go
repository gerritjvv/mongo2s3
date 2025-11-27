package internal

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	region := os.Getenv("S3_REGION")
	if region == "" {
		region = "us-east-1"
	}

	// If an endpoint is provided, use it (SeaweedFS, MinIO, Localstack, etc.)
	if endpoint != "" {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		)
		if err != nil {
			return nil, err
		}

		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)

			// Required by SeaweedFS / MinIO
			o.UsePathStyle = true
		})

		return &S3Provider{
			S3Client: client,
			Bucket:   bucket,
		}, nil
	}

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
	uploadch <-chan string,
	errorch chan<- error) error {

	for {
		select {
		case <-context.Done():
			Log.Info("Uploading context done")
			return context.Err()
		case localFile, ok := <-uploadch:
			if !ok {
				Log.Info("Uploading channel closed")
				return nil
			}
			if localFile == "" {
				Log.Info("Uploading local file is empty")
				continue
			}
			// check if file exists
			Log.Info("Processing upload for metadata file", "file", localFile)
			err := processFileToUpload(context, provider, tracker, localFile, errorch)
			Log.Info("completed upload for metadata file", "file", localFile)
			if err != nil {
				Log.Error("Uploading failed for metadata file", "file", localFile, "error", err.Error())
				errorch <- TraceErr(err)
			}
		}
	}
}

func processFileToUpload(ctx context.Context, provider UploadProvider, tracker CollectionFileTracker, metaDataFileName string, errorCh chan<- error) error {
	metaFileData, err := ParseUploadFileName(metaDataFileName)
	if err != nil {
		Log.Error("Cannot read metadata file for upload", "file", metaDataFileName, "error", err.Error())
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
		errorCh <- uploadErr
	} else {
		defer func() {
			errClose := f.Close()
			if errClose != nil {
				Log.Error("error closing file", "file", f.Name(), "error", TraceErr(errClose).Error())
			}
		}()
		uploadErr = provider.UploadFile(ctx, f, remoteFile)
		if uploadErr != nil {
			trackingRecord.Status = "error"
			trackingRecord.Message = uploadErr.Error()
			errorCh <- TraceErr(uploadErr)
		}

	}

	if uploadErr == nil {
		trackingRecord.Status = "success"
	}
	err = tracker.UpdateFileStatus(ctx, trackingRecord)
	if err != nil {
		return TraceErr(err)
	}
	if uploadErr == nil {
		err = os.Remove(metaFileData.SourceFileName)
		if err != nil {
			errorCh <- TraceErr(err)
		}
		err = os.Remove(metaDataFileName)
		if err != nil {
			errorCh <- TraceErr(err)
		}
	}
	return nil
}
