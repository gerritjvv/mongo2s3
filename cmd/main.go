package main

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
	"strings"
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
	fullLoad := flag.Bool("full-load", false, "indicate if we should do a full load")

	flag.Parse()
	if *configFile == "" {
		internal.Log.Info("--config file is required")
		flag.Usage()
		os.Exit(1)
		return
	}

	bts, err := os.ReadFile(*configFile)
	if err != nil {
		internal.Log.Error("failed to read config file", "err", err.Error(), "file", *configFile)
		os.Exit(2)
		return
	}
	config := internal.Config{}

	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		internal.Log.Error("check your config file has the expected format and is a valid yaml file", "err", err.Error(), "file", *configFile)
		os.Exit(3)
		return
	}

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(config.MONGO_URL))
	if err != nil {
		internal.Log.Error("unable to connect to mongo", "err", err)
		os.Exit(4)
		return
	}
	defer func() {
		if err = mongoClient.Disconnect(ctx); err != nil {
			internal.Log.Info("failed to disconnect to mongo", "err", err.Error())
		}
	}()

	uploadProvider, err := internal.NewS3Provider(ctx, config.S3Bucket)
	if err != nil {
		internal.Log.Error("error creating s3 provider: ", "error", err.Error())
		os.Exit(5)
		return
	}

	// let's make sure we can write to s3 before we start syncing
	internal.Log.Info("Test provider is valid")
	reader := strings.NewReader("testfile")
	uploadError := uploadProvider.UploadFile(ctx, reader, fmt.Sprintf("%s/%s", config.S3Bucket, ".test.txt"))
	if uploadError != nil {
		internal.Log.Error("please check your credentials and endpoint config for s3", "error", uploadError)
		os.Exit(5)
		return
	}
	internal.Log.Info("Provider is valid")
	db, err := internal.NewDBConnect(config)
	if err != nil {
		internal.Log.Error("unable to connect to sync's postgres db, please check your config", "error: ", err.Error())
		os.Exit(6)
		return
	}
	defer func() {
		closeErr := db.Connection.Close(ctx)
		if closeErr != nil {
			internal.Log.Error("error closing sync's postgres connection", "error: ", closeErr.Error())
		}
	}()
	err = internal.RunMigrations(db, ctx)
	if err != nil {
		internal.Log.Error("error while running migrations to sync's postgres", "error: ", err.Error())
		os.Exit(7)
		return
	}

	tracker := &internal.DBCollectionFileTracker{DB: db}

	if *fullLoad {
		doFullLoad(config, tracker, ctx, mongoClient, uploadProvider, sigCh, cancel)
	} else {
		doCDC(config, tracker, ctx, err, mongoClient, uploadProvider, sigCh, cancel)
	}

}

func doFullLoad(config internal.Config, tracker *internal.DBCollectionFileTracker, ctx context.Context, mongoClient *mongo.Client, uploadProvider internal.UploadProvider, sigCh chan os.Signal, cancel context.CancelFunc) {

	var wg sync.WaitGroup
	var errWg sync.WaitGroup
	errorCh := make(chan error, 100)
	for _, _coll := range config.Collections {
		coll := _coll
		uploaderCh := make(chan string, 100)
		specificErrorCh := make(chan error, 100)

		var uploadWg sync.WaitGroup
		var fullLoadWg sync.WaitGroup

		fullLoadWg.Go(func() {
			internal.Log.Info("Starting full load for %s %s", coll.DB, coll.Name)
			if syncErr := internal.FullLoad(ctx, config, mongoClient, coll, uploaderCh, specificErrorCh); syncErr != nil {
				internal.Log.Error("error while running a full load", "error: ", syncErr.Error(), "db", coll.DB, "collection", coll.Name)
				specificErrorCh <- syncErr
			}
			internal.Log.Info("Completed full load for %s %s", coll.DB, coll.Name)

		})

		uploadWg.Go(func() {
			internal.Log.Info("Starting upload for %s %s", coll.DB, coll.Name)
			uploadError := internal.ProcessUploadingFilesFromChannel(ctx, uploadProvider, tracker, uploaderCh, specificErrorCh)
			if uploadError != nil {
				specificErrorCh <- uploadError
			}
			internal.Log.Info("Sopped sync for %s %s", coll.DB, coll.Name)
		})

		wg.Go(func() {
			fullLoadWg.Wait()
			internal.Log.Info("Completed full load for %s %s", coll.DB, coll.Name)
			close(uploaderCh)
			uploadWg.Wait()
			internal.Log.Info("Completed upload for %s %s", coll.DB, coll.Name)
			close(specificErrorCh)
		})

		errWg.Go(func() {
			for {
				select {
				case err1, ok := <-specificErrorCh:
					if !ok {
						return
					}
					errorCh <- fmt.Errorf("error while processing: %s %s %s", err1, coll.DB, coll.Name)
				}
			}
		})
	}

	internal.Log.Info("Started full load on all configured collections")

	var finalErrWg sync.WaitGroup

	finalErrWg.Go(func() {
		for {
			select {
			case appErr, ok := <-errorCh:
				if !ok {
					return
				}
				internal.Log.Error("app error", "error", appErr)
			}
		}
	})

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

outer:
	for {
		select {
		case <-sigCh:
			internal.Log.Info("Shutting down...")
			cancel()
		case <-done:
			break outer
		case <-ctx.Done():
			break outer
		}
	}

	wg.Wait()
	errWg.Wait()
	close(errorCh)
	finalErrWg.Wait()
	internal.Log.Info("Bye")
	os.Exit(0)
}

func doCDC(config internal.Config, tracker *internal.DBCollectionFileTracker, ctx context.Context, err error, mongoClient *mongo.Client, uploadProvider internal.UploadProvider, sigCh chan os.Signal, cancel context.CancelFunc) {
	var wg sync.WaitGroup
	var errWg sync.WaitGroup
	uploadChannels := make([]chan string, 0)
	specificErrorChannels := make([]chan error, 0)

	errorCh := make(chan error, 100)
	for _, _coll := range config.Collections {
		coll := _coll
		uploaderCh := make(chan string, 100)
		uploadChannels = append(uploadChannels, uploaderCh)
		specificErrorCh := make(chan error, 100)
		specificErrorChannels = append(specificErrorChannels, specificErrorCh)

		var resumeToken bson.Raw
		lastTracking, found, getLastUploadErrr := tracker.GetLastUpload(ctx, coll.DB, coll.Name)
		if getLastUploadErrr != nil {
			internal.Log.Info("failed to get last tracking token", "db", coll.DB, "collection", coll.Name, "error", getLastUploadErrr.Error())
			os.Exit(8)
			return
		}

		if found {
			resumeToken, err = base64.URLEncoding.DecodeString(lastTracking.ResumeTokenB64)
			if err != nil {
				internal.Log.Info("failed to base64 decode resume token", "error", err.Error(), "db", coll.DB, "collection", coll.Name)
				os.Exit(9)
			}
		}

		wg.Go(func() {
			internal.Log.Info("Starting sync", "db", coll.DB, "collection", coll.Name, "resumeToken", base64.URLEncoding.EncodeToString(resumeToken))
			if syncErr := internal.SyncEvents(ctx, config, mongoClient, resumeToken, coll, uploaderCh, specificErrorCh); syncErr != nil {
				specificErrorCh <- syncErr
			}
			internal.Log.Info("Stopped sync", "db", coll.DB, "collection", coll.Name)
		})

		wg.Go(func() {
			internal.Log.Info("Starting uploads", "db", coll.DB, "collection", coll.Name)
			uploadError := internal.ProcessUploadingFilesFromChannel(ctx, uploadProvider, tracker, uploaderCh, specificErrorCh)
			if uploadError != nil {
				specificErrorCh <- uploadError
			}
			internal.Log.Info("Stopped uploads", "db", coll.DB, "collection", coll.Name)
		})

		errWg.Go(func() {
			for {
				select {
				case err1, ok := <-specificErrorCh:
					if !ok {
						return
					}
					errorCh <- fmt.Errorf("error while processing: %s %s %s", err1, coll.DB, coll.Name)
				}
			}
		})
	}

	var finalErrWg sync.WaitGroup

	finalErrWg.Go(func() {
		for {
			select {
			case appErr, ok := <-errorCh:
				if !ok {
					return
				}
				internal.Log.Info("app error", "error", appErr.Error())
			}
		}
	})

	for {
		select {
		case <-sigCh:
			internal.Log.Info("Shutting down...")
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
			internal.Log.Info("Bye")
			os.Exit(0)
		}
	}
}
