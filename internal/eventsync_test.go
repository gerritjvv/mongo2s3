package internal

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"
)

func TestNewRollingFile(t *testing.T) {

	collections := []MongoCollection{
		{
			DB:   "test",
			Name: "test",
		},
	}

	config := Config{
		MONGO_URL:              "",
		LocalBaseDir:           "/tmp/",
		S3Bucket:               "",
		S3Region:               "",
		S3Prefix:               "",
		FileWriteTimeoutSecond: 60,
		FileWriteSizeBytes:     100,
		Collections:            collections,
	}
	rollingFile, err := NewRollingFile(config, collections[0])

	if err != nil {
		t.Error(err)
		return
	}

	_, err = rollingFile.Writer.Write([]byte("test"))
	if err != nil {
		t.Error(err)
		return
	}
	err = rollingFile.Close()
	if err != nil {
		t.Error(err)
		return
	}

	resumeToken := []byte("mytoken")
	uploadCh := make(chan string, 10)

	newRollingFile, err := RollFile(rollingFile, config, collections[0], resumeToken, "", uploadCh)
	if err != nil {
		t.Error(err)
		return
	}

	select {
	case uploadFileName := <-uploadCh:

		fileNameStruct, err := ParseUploadFileName(uploadFileName)
		if err != nil {
			t.Error(err)
			return
		}

		resumeTokenB64 := fileNameStruct.ResumeTokenB64

		resumeTokenBts, err := base64.URLEncoding.DecodeString(resumeTokenB64)
		if err != nil {
			t.Errorf("Unexpected b64 data %s for filename: %s", resumeTokenB64, fileNameStruct.FileName())
			return
		}
		if !bytes.Equal(resumeTokenBts, resumeToken) {
			t.Errorf("resume token not match got %s, expected %s", string(resumeTokenBts), string(resumeToken))
			return
		}
	case <-time.After(time.Second * 10):
		t.Fatal("upload channel timeout")
	}

	rollingFile.Delete()
	newRollingFile.Close()
	newRollingFile.Delete()
}
