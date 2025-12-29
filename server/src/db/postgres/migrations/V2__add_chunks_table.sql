CREATE TABLE chunks (
    sequence_number BIGSERIAL PRIMARY KEY,
    store_job_id UUID REFERENCES store_jobs(id) ON DELETE SET NULL;
    bucket TEXT NOT NULL,
    prefix TEXT,
    key TEXT NOT NULL,
    stream TEXT NOT NULL,
    consumer TEXT,
    subject TEXT NOT NULL,
    timestamp_start TIMESTAMPTZ NOT NULL,
    timestamp_end TIMESTAMPTZ NOT NULL,
    message_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    codec encoding_codec NOT NULL,
    hash BYTEA NOT NULL,
    version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    
    CONSTRAINT chunks_bucket_key_unique UNIQUE (bucket, prefix, key),
    CONSTRAINT chunks_timestamp_range CHECK (timestamp_end >= timestamp_start),
    CONSTRAINT chunks_message_count_positive CHECK (message_count > 0),
    CONSTRAINT chunks_size_bytes_positive CHECK (size_bytes > 0)
);

CREATE INDEX idx_store_job_id ON chunks(store_job_id)
WHERE deleted_at IS NULL;

CREATE INDEX idx_chunks_load_query ON chunks(stream, consumer, subject, bucket, prefix, timestamp_start, timestamp_end) 
WHERE deleted_at IS NULL;

CREATE INDEX idx_chunks_deleted ON chunks(deleted_at) 
WHERE deleted_at IS NOT NULL;
