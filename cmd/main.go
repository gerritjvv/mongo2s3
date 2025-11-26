package cmd

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"gopkg.in/yaml.v3"
	"mongo2s3/internal"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture CTRL+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	configFile := flag.String("config", "", "config file")
	flag.Parse()
	if *configFile == "" {
		fmt.Println("--config file is required")
		flag.Usage()
		os.Exit(1)
		return
	}

	bts, err := os.ReadFile(*configFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
		return
	}
	config := internal.Config{}

	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
		return
	}

	var wg sync.WaitGroup
	var errWg sync.WaitGroup

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(config.MONGO_URL))
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
		return
	}
	defer func() {
		if err = mongoClient.Disconnect(ctx); err != nil {
			fmt.Printf("failed to disconnect to mongo: %s", err)
		}
	}()

	uploadProvider, err := internal.NewS3Provider(ctx, config.S3Bucket)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
		return
	}

	db, err := internal.NewDBConnect(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(6)
		return
	}
	defer func() {
		closeErr := db.Connection.Close(ctx)
		if closeErr != nil {
			fmt.Println(closeErr)
		}
	}()
	err = internal.RunMigrations(db, ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(7)
		return
	}

	tracker := &internal.DBCollectionFileTracker{DB: db}
	errorCh := make(chan error, 100)

	uploadChannels := make([]chan string, 0)
	specificErrorChannels := make([]chan error, 0)

	for _, _coll := range config.Collections {
		coll := _coll
		uploaderCh := make(chan string, 100)
		uploadChannels = append(uploadChannels, uploaderCh)
		specificErrorCh := make(chan error, 100)
		specificErrorChannels = append(specificErrorChannels, specificErrorCh)

		var resumeToken bson.Raw
		lastTracking, found, getLastUploadErrr := tracker.GetLastUpload(ctx, coll.DB, coll.Name)
		if getLastUploadErrr != nil {
			fmt.Printf("failed to get last tracking token : %s for %s %s\n", getLastUploadErrr, coll.DB, coll.Name)
			os.Exit(8)
			return
		}
		if found {
			resumeToken, err = base64.URLEncoding.DecodeString(lastTracking.ResumeTokenB64)
			if err != nil {
				fmt.Printf("failed to base64 decode resume token : %s %s %s\n", err, coll.DB, coll.Name)
				os.Exit(9)
			}
		}
		wg.Go(func() {
			fmt.Printf("Starting sync for %s %s from %s\n", coll.DB, coll.Name, base64.URLEncoding.EncodeToString(resumeToken))
			if syncErr := internal.SyncEvents(ctx, config, mongoClient, resumeToken, coll, uploaderCh, specificErrorCh); syncErr != nil {
				specificErrorCh <- syncErr
			}
			fmt.Printf("Stopped  sync for %s %s\n", coll.DB, coll.Name)
		})

		wg.Go(func() {
			uploadError := internal.ProcessUploadingFilesFromChannel(ctx, uploadProvider, tracker, uploaderCh)
			if uploadError != nil {
				specificErrorCh <- uploadError
			}
		})

		errWg.Go(func() {
			for {
				select {
				case err1, ok := <-specificErrorCh:
					if !ok {
						return
					}
					errorCh <- fmt.Errorf("error while processing: %s %s %s\n", err1, coll.DB, coll.Name)
				}
			}
		})
	}

	fmt.Printf("Started for collections\n")

	var finalErrWg sync.WaitGroup

	finalErrWg.Go(func() {
		for {
			select {
			case appErr, ok := <-errorCh:
				if !ok {
					return
				}
				fmt.Printf("%s\n", appErr)
			}
		}
	})

	for {
		select {
		case <-sigCh:
			fmt.Println("\nShutting down...")
			cancel()
		case <-ctx.Done():
			for _, ch := range uploadChannels {
				close(ch)
			}
			wg.Wait()
			for _, ch := range specificErrorChannels {
				close(ch)
			}
			errWg.Wait()
			close(errorCh)
			finalErrWg.Wait()
			fmt.Printf("Bye\n")
			os.Exit(0)
		}
	}
}
