CREATE TABLE IF NOT EXISTS sync_file_tracking (
    id serial primary key,
    db text,
    collection text,
    remote_file text,
    status text,
    message text,
    start_token_b64 text,
    resume_token_b64 text,
    created_at timestamp with time zone DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS sync_file_tracking_unique_idx
    ON sync_file_tracking (db, collection, remote_file);

CREATE INDEX IF NOT EXISTS sync_file_tracking_status_idx
    ON sync_file_tracking (status);

CREATE INDEX IF NOT EXISTS sync_file_tracking_created_at_idx
    ON sync_file_tracking (created_at);