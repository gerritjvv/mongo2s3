package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-connections/nat"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type TestUploadProvider struct {
	UploadFiles map[string]bool
}

func (p *TestUploadProvider) UploadFile(_ context.Context, _ io.Reader, remoteKey string) error {
	p.UploadFiles[remoteKey] = true
	return nil
}

func TestFileUploadToS3(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForSQL(
			"5432/tcp",
			"pgx",
			func(host string, port nat.Port) string {
				return fmt.Sprintf(
					"postgres://test:test@%s:%s/testdb?sslmode=disable",
					host, port.Port(),
				)
			}).WithQuery("select 1").WithStartupTimeout(10 * time.Second),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = pgContainer.Terminate(ctx)
		if err != nil {
			fmt.Printf("error terminating container: %s\n", err)
		}
	}()

	host, _ := pgContainer.Host(ctx)
	port, _ := pgContainer.MappedPort(ctx, "5432")

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
	t.Log("DSN:", dsn)

	conf := Config{
		DB_URL:                 dsn,
		MONGO_URL:              "test",
		LocalBaseDir:           "/tmp",
		S3Bucket:               "test",
		S3Region:               "test",
		S3Prefix:               "test",
		FileWriteTimeoutSecond: 10,
		FileWriteSizeBytes:     100,
		Collections:            nil,
	}
	db, err := NewDBConnect(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	
	err = RunMigrations(db, ctx)
	if err != nil {
		t.Fatal(err)
	}

	tracker := &DBCollectionFileTracker{DB: db}
	// start uploading files to a fake provder
	provider := &TestUploadProvider{
		UploadFiles: make(map[string]bool),
	}
	uploadch := make(chan string, 10)

	errorch := make(chan error, 10)

	wg := sync.WaitGroup{}
	testDoneCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

	wg.Go(func() {
		err1 := ProcessUploadingFilesFromChannel(
			testDoneCtx,
			provider,
			tracker,
			uploadch,
			errorch,
		)
		if err1 != nil {
			errorch <- err1
		}
	})

	file, err := os.CreateTemp("/tmp", "upload-*.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err1 := os.Remove(file.Name())
		if err1 != nil {
			fmt.Println(err1)
		}
	}()

	metaFile := fmt.Sprintf("%s.meta.gz", file.Name())
	metaFileData := &UploadFileName{
		DB:             "text",
		Collection:     "test",
		SourceFileName: file.Name(),
		Nanos:          time.Now().UnixNano(),
		ResumeTokenB64: "test",
		StartTokenB64:  "test",
	}

	jsonBytes, err := json.Marshal(metaFileData)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(metaFile, jsonBytes, 0644)
	if err != nil {
		t.Fatal(err)
	}

	uploadch <- metaFile
outer:
	for {
		select {
		case <-testDoneCtx.Done():
			close(uploadch)
			break outer
		case err := <-errorch:
			cancel()
			t.Fatal(err)
			return
		}
	}

	cancel()

	fmt.Printf("Uploading %s to s3\n", metaFile)
	fmt.Printf("Uploading %s to s3\n", file.Name())
	fmt.Printf("Provider uploadFiles: %v\n", provider.UploadFiles)

	if len(provider.UploadFiles) != 1 {
		t.Fatalf("Expected 1 file to be uploaded, but got %d", len(provider.UploadFiles))
	}
}
