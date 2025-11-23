package internal

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

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
    message = EXCLUDED.message,
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
