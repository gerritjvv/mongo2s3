package internal

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"os"
	"sync"
	"time"
)

// SyncEvents opens a Watch cursor on a database and collection
//
//	on each event the data is written using json gzip to a temporary file.
//	once the data reaches a thresh hold or time deadline, the file is rolled and the metadata file is sent to the s3 uploader
//	a new file is started and new events will be written to this file
func SyncEvents(context context.Context,
	conf Config,
	conn *mongo.Client,
	resumeToken bson.Raw,
	collection MongoCollection,
	uploaderCh chan<- string,
	errorCh chan<- error) error {

	db := conn.Database(collection.DB)
	opts := options.ChangeStream()
	opts = opts.SetFullDocument(options.UpdateLookup)

	if resumeToken != nil && len(resumeToken) > 0 {
		opts = opts.SetResumeAfter(resumeToken)
	}

	cs, err := db.Collection(collection.Name).Watch(context, mongo.Pipeline{}, opts)

	if err != nil {
		return TraceErr(fmt.Errorf("error trying to watch collection : %s; %s", collection.Name, err))
	}
	defer func() {
		err := cs.Close(context)
		if err != nil {
			errorCh <- TraceErr(err)
		}
	}()
	writerCh := make(chan *ChangeEventCombo, 100)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := writeEvents(context, conf, collection, writerCh, uploaderCh)
		if err != nil {
			errorCh <- TraceErr(err)
		}
	}()
	for cs.Next(context) {

		if err := cs.Err(); err != nil {
			errorCh <- TraceErr(err)
		}
		raw := cs.Current

		event := ChangeEvent{}
		err := bson.Unmarshal(raw, &event)
		if err != nil {
			errorCh <- TraceErr(err)
		}
		fmt.Printf("Event Op Type: %s\n", event.OperationType)
		if event.OperationType == "update" || event.OperationType == "insert" {
			// send to writer the event and the resume token
			writerCh <- &ChangeEventCombo{Event: &event, ResumeToken: cs.ResumeToken()}
		}
	}

	close(writerCh)
	wg.Wait()
	return nil
}

func writeEvents(
	ctx context.Context,
	conf Config,
	collection MongoCollection,
	chevents <-chan *ChangeEventCombo,
	uploadCh chan<- string) error {

	timer := time.NewTimer(conf.FileWriteTimeoutSecond)
	defer timer.Stop()

	sizeCheckTicker := time.NewTicker(500 * time.Millisecond)
	defer sizeCheckTicker.Stop()

	var resumeToken bson.Raw
	var err error
	newLine := []byte("\n")
	rollingFile, err := NewRollingFile(conf, collection)
	if err != nil {
		return TraceErr(err)
	}
	defer func() {
		// ensure we clean up the temp file if still around
		if rollingFile != nil {
			err := rollingFile.Close()

			if err != nil {
				fmt.Printf("error closing rolling file on defer: %s\n", TraceErr(err))
			}
			rollingFile.Delete()
		}
	}()

	// ensure we only roll the file if something was written
	// avoids ending up with lots of empty files
	bytesWritten := false

	for {
		select {
		case <-ctx.Done():
			// context is done, we should bail
			if bytesWritten {
				rollingFile, err = RollFile(rollingFile, conf, collection, resumeToken, uploadCh)
				if err != nil {
					return TraceErr(err)
				}
				bytesWritten = false
			}
			return ctx.Err()
		case eventCombo, ok := <-chevents:
			if !ok {
				// the channel is closed, we should finish up and return
				fmt.Println("events channel closed")
				if bytesWritten {
					rollingFile, err = RollFile(rollingFile, conf, collection, resumeToken, uploadCh)
					if err != nil {
						return TraceErr(err)
					}
					bytesWritten = false
				}
				return nil
			}

			event := eventCombo.Event
			resumeToken = eventCombo.ResumeToken

			if !bytesWritten {
				rollingFile.StartToken = resumeToken
			}

			err = writeToFile(rollingFile.Writer, event, newLine)
			if err != nil {
				return TraceErr(err)
			}
			bytesWritten = true
		case <-sizeCheckTicker.C:
			// time to check file size
			if bytesWritten && isSizeThresholdReached(rollingFile, conf) {
				fmt.Printf("rolling file %s because size threshold reached \n", rollingFile.String())
				rollingFile, err = RollFile(rollingFile, conf, collection, resumeToken, uploadCh)
				if err != nil {
					return TraceErr(err)
				}
				bytesWritten = false
			}
		case <-timer.C:
			// write to file timeout, we need to roll if any data
			if bytesWritten {
				fmt.Printf("rolling file %s because time threshold reached \n", rollingFile)

				rollingFile, err = RollFile(rollingFile, conf, collection, resumeToken, uploadCh)
				if err != nil {
					return TraceErr(err)
				}
				bytesWritten = false
			}
			timer.Reset(conf.FileWriteTimeoutSecond)
		}
	}
}

// isSizeThresholdReached returns true or false if the threshold is reached
// if stat error we return false and print the error
func isSizeThresholdReached(rollingFile *RollingFile, conf Config) bool {
	stat, err := rollingFile.Stat()
	if err != nil {
		// try again if we can't stat the size
		fmt.Printf("file %s stat error: %s\n", rollingFile.FileName, err)
		return false
	}
	if stat.Size() >= conf.FileWriteSizeBytes {
		return true
	}
	return false
}

// writeToFile writes the event marshalled as json to the writer + a newline.
func writeToFile(writer *gzip.Writer, event *ChangeEvent, newLine []byte) error {
	bts, err := bson.MarshalExtJSON(event.FullDocument, false, true)
	if err != nil {
		return err
	}
	_, err = writer.Write(bts)
	if err != nil {
		return err
	}
	_, err = writer.Write(newLine)
	if err != nil {
		return err
	}
	return nil
}

// RollFile forces the current rollingFile object to close, the moveFile is called which will move the file to its final name
// and a new rollingFile struct is created
func RollFile(rollingFile *RollingFile, conf Config, collection MongoCollection, resumeToken bson.Raw, uploadCh chan<- string) (*RollingFile, error) {
	// close and roll
	err := rollingFile.Close()
	if err != nil {
		fmt.Printf("file %s close error: %s\n", rollingFile.FileName, err)
		return nil, err
	}
	movedFile, err := moveFile(conf, collection, rollingFile.FileName)
	if err != nil {
		return nil, err
	}
	metaFile, err := os.Create(fmt.Sprintf("%s.meta", movedFile))
	if err != nil {
		return nil, err
	}

	uploadfileName := UploadFileName{
		Collection:     collection.Name,
		SourceFileName: rollingFile.FileName,
		Nanos:          time.Now().UnixNano(),
		ResumeTokenB64: base64.URLEncoding.EncodeToString(resumeToken),
		StartTokenB64:  base64.StdEncoding.EncodeToString(rollingFile.StartToken),
	}

	bts, err := json.Marshal(uploadfileName)
	if err != nil {
		return nil, err
	}
	_, err = metaFile.Write(bts)
	if err != nil {
		return nil, err
	}
	err = metaFile.Close()
	if err != nil {
		return nil, err
	}

	// queue file for uploading
	uploadCh <- metaFile.Name()

	rollingFile, err = NewRollingFile(conf, collection)
	if err != nil {
		return nil, err
	}
	return rollingFile, nil
}

// moveFile create a file with the resumetoken name in it and queue the file for uploading.
// The uploader can get the file's collection name nannos and resumetoken
func moveFile(conf Config, collection MongoCollection, fileName string) (string, error) {
	// we roll the file
	// give it a name collection__nanos__resumetoken_b64.gzip
	newFileName := makeFileName(collection.Name)
	newFilePath := fmt.Sprintf("%s/%s", conf.LocalBaseDir, newFileName)
	err := os.Rename(fileName, newFilePath)
	if err != nil {
		return "", err
	}
	return newFilePath, nil
}

// makeFileName return the filename string collection__nanos__resumetoken_b64.gzip
func makeFileName(collectionName string) string {
	return fmt.Sprintf("%s__%d.gz", collectionName, time.Now().UnixNano())
}

type RollingFile struct {
	StartToken bson.Raw
	File       *os.File
	Writer     *gzip.Writer
	FileName   string
}

func (r *RollingFile) String() string {
	bts, err := json.Marshal(r)
	if err != nil {
		return err.Error()
	}
	return string(bts)
}

func (r *RollingFile) Delete() {
	if _, err := os.Stat(r.FileName); errors.Is(err, os.ErrNotExist) {
		return
	}

	err := os.Remove(r.FileName)
	if err != nil {
		fmt.Printf("file %s delete error: %s\n", r.FileName, err)
	}
}

func (r *RollingFile) Stat() (os.FileInfo, error) {
	return r.File.Stat()
}

func (r *RollingFile) Close() error {
	err1 := r.Writer.Close()
	err2 := r.File.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil && !errors.Is(err2, os.ErrClosed) {
		return err2
	}
	return nil
}

func NewRollingFile(conf Config, collection MongoCollection) (*RollingFile, error) {
	file, err := os.Create(fmt.Sprintf("%s/%s.tmp", conf.LocalBaseDir, collection.Name))
	if err != nil {
		return nil, err
	}
	writer := gzip.NewWriter(file)

	return &RollingFile{
		File:     file,
		Writer:   writer,
		FileName: file.Name(),
	}, nil
}
