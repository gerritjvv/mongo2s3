package internal

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"sync"
	"testing"
	"time"
)

func TestChangeRequestsFromMongo(t *testing.T) {

	ctx, stopTest := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stopTest()

	mongoContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("failed to connect to mongo: %s", err)
		return
	}
	defer mongoContainer.Terminate(ctx)

	mongoUri, err := mongoContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to connect to mongo: %s", err)
		return
	}

	client, err := mongo.Connect(options.Client().ApplyURI(mongoUri))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			fmt.Printf("failed to disconnect to mongo: %s", err)
		}
	}()

	db := client.Database("test")
	coll := db.Collection("test")
	if coll == nil {
		t.Fatalf("failed to connect to mongo: %s", err)
		return
	}
	for range 100 {
		_, err := coll.InsertOne(ctx, bson.M{"name": "test", "age": 30})
		if err != nil {
			fmt.Printf("failed to insert one to mongo: %s\n", err)
			continue
		}
	}
	if err != nil {
		t.Fatalf("failed to insert to mongo: %s", err)
		return
	}

	time.Sleep(time.Second * 1)
	fmt.Println("Start listining to Mongo events")

	collections := []MongoCollection{
		{
			DB:   "test",
			Name: "test",
		},
	}
	config := Config{
		MONGO_URL:              mongoUri,
		LocalBaseDir:           "/tmp/",
		S3Bucket:               "",
		S3Region:               "",
		S3Prefix:               "",
		FileWriteTimeoutSecond: 1 * time.Second,
		FileWriteSizeBytes:     100,
		Collections:            collections,
	}

	uploaderCh := make(chan string, 100)
	defer close(uploaderCh)
	errorCh := make(chan error, 1000)
	defer close(errorCh)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		fmt.Println("Start SyncEvents")
		if err := SyncEvents(ctx, config, client, []byte{}, collections[0], uploaderCh, errorCh); err != nil {
			errorCh <- err
		}
		fmt.Println("SyncEvents finished")
	})

	wg.Go(func() {
	outer:
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context closed Stop inserting records")
				return
			default:
				fmt.Println("Writing 100 records")
				var insertOneResult *mongo.InsertOneResult
				for range 100 {
					insertOneResult, err = coll.InsertOne(ctx, bson.M{"name": "test", "age": 30})
					if err != nil {
						fmt.Printf("failed to insert to mongo: %s\n", err)
						break outer
					}
					update := bson.M{
						"$set": bson.M{"age": 31},
					}
					_, err = coll.UpdateByID(ctx, insertOneResult.InsertedID, update)
					if err != nil {
						fmt.Printf("failed to update  to mongo: %s\n", err)
					}
				}

				if insertOneResult != nil {
					_, err = coll.DeleteOne(ctx, bson.M{"_id": insertOneResult.InsertedID})
					if err != nil {
						fmt.Printf("failed to delete  to mongo: %s\n", err)
					}
				}

				time.Sleep(500 * time.Millisecond)
			}
		}
	})

	filesToUpload := []string{}
	errors := []error{}
	testWait := time.NewTimer(5 * time.Second)

outer:
	for {
		select {
		case <-testWait.C:
			// end all components and check results
			// this will stop all channels
			fmt.Println("Signal stop test")
			stopTest()
			break outer
		case file := <-uploaderCh:
			filesToUpload = append(filesToUpload, file)
			fmt.Printf("uploading %s\n", file)
		case err := <-errorCh:
			errors = append(errors, err)
			fmt.Printf("error from error channel : %s\n", err)
		}
	}

	fmt.Println("Waiting for channels to complete")
	wg.Wait()

	fmt.Println("Checking test output")
	fmt.Println(filesToUpload)
	fmt.Println(errors)

	if len(errors) > 0 {
		for _, err2 := range errors {
			t.Errorf("error from error channel : %s", err2)
		}
		return
	}

	if len(filesToUpload) == 0 {
		t.Fatalf("no files to upload")
		return
	}
}
