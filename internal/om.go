package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"os"
)

type CollectionFileTrackingRecord struct {
	DB             string
	Collection     string
	RemoteFile     string
	Status         string
	Message        string
	StartTokenB64  string
	ResumeTokenB64 string
}

type CollectionFileTracker interface {
	UpdateFileStatus(context context.Context, record CollectionFileTrackingRecord) error
	GetLastUpload(context context.Context, db string, collection string) (record *CollectionFileTrackingRecord, found bool, err error)
}

type ChangeEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  bson.M `bson:"fullDocument"`
	Namespace     struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey struct {
		ID interface{} `bson:"_id"`
	} `bson:"documentKey"`
}

type ChangeEventCombo struct {
	Event       *ChangeEvent
	ResumeToken bson.Raw
}

type UploadFileName struct {
	DB             string `bson:"db"`
	Collection     string `json:"collection"`
	SourceFileName string `json:"source_file_name"`
	Nanos          int64  `json:"nanos"`
	ResumeTokenB64 string `json:"resume_token"`
	StartTokenB64  string `json:"start_token"`
}

func (u *UploadFileName) FileName() string {
	return fmt.Sprintf("%s_%d.gzip", u.Collection, u.Nanos)
}

func ParseUploadFileName(metaDataFile string) (*UploadFileName, error) {
	/// collection__nanos__resumetoken_b64.gzip
	bts, err := os.ReadFile(metaDataFile)
	if err != nil {
		return nil, err
	}
	uploadFileName := UploadFileName{}
	json.Unmarshal(bts, &uploadFileName)
	return &uploadFileName, nil
}
