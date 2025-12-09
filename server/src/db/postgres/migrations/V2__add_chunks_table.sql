CREATE TABLE chunks (
    sequence_number BIGSERIAL PRIMARY KEY,
    bucket TEXT NOT NULL,
    prefix TEXT NOT NULL,
    key TEXT NOT NULL,
    stream TEXT NOT NULL,
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

-- Primary query: load jobs finding chunks to download
CREATE INDEX idx_chunks_load_query ON chunks(stream, subject, bucket, prefix, timestamp_start, timestamp_end) 
WHERE deleted_at IS NULL;

-- Maintenance: cleanup soft-deleted chunks
CREATE INDEX idx_chunks_deleted ON chunks(deleted_at) 
WHERE deleted_at IS NOT NULL;
