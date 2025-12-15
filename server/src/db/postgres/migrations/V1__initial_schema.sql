CREATE TYPE load_job_status AS ENUM ('created', 'running', 'success', 'failure');

CREATE TYPE store_job_status AS ENUM ('created', 'running', 'success', 'failure');

CREATE TYPE encoding_codec AS ENUM ('json', 'binary');

CREATE TABLE load_jobs (
    id TEXT PRIMARY KEY,
    status load_job_status NOT NULL,
    bucket TEXT NOT NULL,
    prefix TEXT,
    read_stream TEXT NOT NULL,
    read_consumer TEXT,
    read_subject TEXT NOT NULL,
    write_subject TEXT NOT NULL,
    poll_interval BIGINT,
    delete_chunks BOOLEAN NOT NULL,
    from_time TIMESTAMPTZ,
    to_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_load_jobs_status ON load_jobs(status);

CREATE TABLE store_jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status store_job_status NOT NULL,
    stream TEXT NOT NULL,
    consumer TEXT,
    subject TEXT NOT NULL,
    bucket TEXT NOT NULL,
    prefix TEXT,
    batch_max_bytes BIGINT NOT NULL,
    batch_max_count BIGINT NOT NULL,
    encoding_codec encoding_codec NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_store_jobs_status ON store_jobs(status);
