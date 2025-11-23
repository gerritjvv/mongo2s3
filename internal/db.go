package internal

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type DB struct {
	Connection *pgx.Conn
}

func NewDBConnect(conf Config) (*DB, error) {
	conn, err := pgx.Connect(context.Background(), conf.DB_URL)
	if err != nil {
		return nil, TraceErr(err)
	}
	return &DB{Connection: conn}, nil
}

func RunMigrations(db *DB, ctx context.Context) error {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}

	for _, e := range entries {
		contents, err := migrationFiles.ReadFile("migrations/" + e.Name())
		if err != nil {
			return err
		}

		fmt.Printf("found migration file: %s\n", e.Name())
		_, execErr := db.Connection.Exec(ctx, string(contents))
		if execErr != nil {
			return fmt.Errorf("migration %s failed: %w", e.Name(), execErr)
		}
		fmt.Println("applied migration:", e.Name())
	}
	return nil
}

type DBCollectionFileTracker struct {
	DB *DB
}

func (t *DBCollectionFileTracker) UpdateFileStatus(context context.Context, record CollectionFileTrackingRecord) error {
	_, err := t.DB.Connection.Exec(context,
		`
INSERT INTO sync_file_tracking (
    db,
    collection,
    remote_file,
    status,
    message,
    start_token_b64,
    resume_token_b64,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, now()
)
ON CONFLICT (db, collection, remote_file)
DO UPDATE SET
    status = EXCLUDED.status,
    message = EXCLUDED.message
`,
		record.DB,
		record.Collection,
		record.RemoteFile,
		record.Status,
		record.Message,
		record.StartTokenB64,
		record.ResumeTokenB64,
	)
	if err != nil {
		fmt.Printf("error updating file status: %s\n", err)
		return TraceErr(err)
	}
	return nil
}

func (t *DBCollectionFileTracker) GetLastUpload(context context.Context, db string, collection string) (record *CollectionFileTrackingRecord, found bool, err error) {
	row := t.DB.Connection.QueryRow(context, `
	select db,
    collection,
    remote_file,
    status,
    message,
    start_token_b64,
    resume_token_b64
	from sync_file_tracking  where db = $1  and collection = $2
	order by created_at desc limit 1
`, db, collection)

	record = &CollectionFileTrackingRecord{}
	err = row.Scan(&record.DB, &record.Collection, &record.RemoteFile, &record.Status, &record.Message, &record.StartTokenB64, &record.ResumeTokenB64)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, TraceErr(err)
	}
	return record, true, nil
}
