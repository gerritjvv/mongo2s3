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
		contents, err1 := migrationFiles.ReadFile("migrations/" + e.Name())
		if err1 != nil {
			return err1
		}

		Log.Info("found migration file", "file", e.Name())
		_, execErr := db.Connection.Exec(ctx, string(contents))
		if execErr != nil {
			return fmt.Errorf("migration %s failed: %w", e.Name(), execErr)
		}
		Log.Info("applied migration:", "file", e.Name())
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
    start_mongo_id,
    end_mongo_id,                                
    start_token_b64,
    resume_token_b64,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, now()
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
		record.StartMongoId,
		record.EndMongoId,
		record.StartTokenB64,
		record.ResumeTokenB64,
	)
	if err != nil {
		Log.Error("error updating file status", "error", err)
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
    start_mongo_id,
    end_mongo_id,
    start_token_b64,
    resume_token_b64
	from sync_file_tracking  where db = $1  and collection = $2
	order by created_at desc limit 1
`, db, collection)

	record = &CollectionFileTrackingRecord{}
	err = row.Scan(&record.DB, &record.Collection, &record.RemoteFile, &record.Status, &record.Message,
		&record.StartMongoId, &record.EndMongoId,
		&record.StartTokenB64, &record.ResumeTokenB64)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, TraceErr(err)
	}
	return record, true, nil
}
