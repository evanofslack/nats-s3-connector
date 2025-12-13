use bytes::Bytes;
use postgres_types::{FromSql, ToSql};
use tokio_postgres::Row;

use nats3_types::{Batch, Codec, Encoding, LoadJob, LoadJobStatus, StoreJob, StoreJobStatus};

use crate::db::{ChunkMetadata, ChunkMetadataError, CreateChunkMetadata, JobStoreError};

#[derive(Debug, Clone, ToSql, FromSql)]
#[postgres(name = "load_job_status")]
pub enum LoadJobStatusEnum {
    #[postgres(name = "created")]
    Created,
    #[postgres(name = "running")]
    Running,
    #[postgres(name = "success")]
    Success,
    #[postgres(name = "failure")]
    Failure,
}

impl From<LoadJobStatus> for LoadJobStatusEnum {
    fn from(status: LoadJobStatus) -> Self {
        match status {
            LoadJobStatus::Created => Self::Created,
            LoadJobStatus::Running => Self::Running,
            LoadJobStatus::Success => Self::Success,
            LoadJobStatus::Failure => Self::Failure,
        }
    }
}

impl From<LoadJobStatusEnum> for LoadJobStatus {
    fn from(status: LoadJobStatusEnum) -> Self {
        match status {
            LoadJobStatusEnum::Created => Self::Created,
            LoadJobStatusEnum::Running => Self::Running,
            LoadJobStatusEnum::Success => Self::Success,
            LoadJobStatusEnum::Failure => Self::Failure,
        }
    }
}

pub struct LoadJobRow {
    pub id: String,
    pub status: LoadJobStatusEnum,
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub write_subject: String,
    pub delete_chunks: bool,
    pub start_pos: Option<i64>,
    pub end_pos: Option<i64>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl LoadJobRow {
    pub fn from_row(row: &Row) -> Result<Self, JobStoreError> {
        Ok(Self {
            id: row.try_get("id")?,
            status: row.try_get("status")?,
            bucket: row.try_get("bucket")?,
            prefix: row.try_get("prefix")?,
            read_stream: row.try_get("read_stream")?,
            read_consumer: row.try_get("read_consumer")?,
            read_subject: row.try_get("read_subject")?,
            write_subject: row.try_get("write_subject")?,
            delete_chunks: row.try_get("delete_chunks")?,
            start_pos: row.try_get("start_pos")?,
            end_pos: row.try_get("end_pos")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl From<LoadJobRow> for LoadJob {
    fn from(row: LoadJobRow) -> Self {
        Self {
            id: row.id,
            status: row.status.into(),
            bucket: row.bucket,
            prefix: row.prefix,
            read_stream: row.read_stream,
            read_consumer: row.read_consumer,
            read_subject: row.read_subject,
            write_subject: row.write_subject,
            delete_chunks: row.delete_chunks,
            start: row.start_pos.map(|v| v as usize),
            end: row.end_pos.map(|v| v as usize),
        }
    }
}

impl From<LoadJob> for LoadJobRow {
    fn from(job: LoadJob) -> Self {
        Self {
            id: job.id,
            status: job.status.into(),
            bucket: job.bucket,
            prefix: job.prefix,
            read_stream: job.read_stream,
            read_consumer: job.read_consumer,
            read_subject: job.read_subject,
            write_subject: job.write_subject,
            delete_chunks: job.delete_chunks,
            start_pos: job.start.map(|v| v as i64),
            end_pos: job.end.map(|v| v as i64),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
#[postgres(name = "store_job_status")]
pub enum StoreJobStatusEnum {
    #[postgres(name = "created")]
    Created,
    #[postgres(name = "running")]
    Running,
    #[postgres(name = "success")]
    Success,
    #[postgres(name = "failure")]
    Failure,
}

impl From<StoreJobStatus> for StoreJobStatusEnum {
    fn from(status: StoreJobStatus) -> Self {
        match status {
            StoreJobStatus::Created => Self::Created,
            StoreJobStatus::Running => Self::Running,
            StoreJobStatus::Success => Self::Success,
            StoreJobStatus::Failure => Self::Failure,
        }
    }
}

impl From<StoreJobStatusEnum> for StoreJobStatus {
    fn from(status: StoreJobStatusEnum) -> Self {
        match status {
            StoreJobStatusEnum::Created => Self::Created,
            StoreJobStatusEnum::Running => Self::Running,
            StoreJobStatusEnum::Success => Self::Success,
            StoreJobStatusEnum::Failure => Self::Failure,
        }
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
#[postgres(name = "encoding_codec")]
pub enum EncodingCodec {
    #[postgres(name = "json")]
    Json,
    #[postgres(name = "binary")]
    Binary,
}

impl From<Codec> for EncodingCodec {
    fn from(codec: Codec) -> Self {
        match codec {
            Codec::Json => Self::Json,
            Codec::Binary => Self::Binary,
        }
    }
}

impl From<EncodingCodec> for Codec {
    fn from(codec: EncodingCodec) -> Self {
        match codec {
            EncodingCodec::Json => Self::Json,
            EncodingCodec::Binary => Self::Binary,
        }
    }
}

pub struct StoreJobRow {
    pub id: String,
    pub name: String,
    pub status: StoreJobStatusEnum,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch_max_bytes: i64,
    pub batch_max_count: i64,
    pub encoding_codec: EncodingCodec,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// model when creating a new store job (doesn't yet have timestamps)
pub struct StoreJobRowCreate {
    pub id: String,
    pub name: String,
    pub status: StoreJobStatusEnum,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch_max_bytes: i64,
    pub batch_max_count: i64,
    pub encoding_codec: EncodingCodec,
}

impl StoreJobRow {
    pub fn from_row(row: &Row) -> Result<Self, JobStoreError> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            status: row.try_get("status")?,
            stream: row.try_get("stream")?,
            consumer: row.try_get("consumer")?,
            subject: row.try_get("subject")?,
            bucket: row.try_get("bucket")?,
            prefix: row.try_get("prefix")?,
            batch_max_bytes: row.try_get("batch_max_bytes")?,
            batch_max_count: row.try_get("batch_max_count")?,
            encoding_codec: row.try_get("encoding_codec")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl From<StoreJobRow> for StoreJob {
    fn from(row: StoreJobRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            status: row.status.into(),
            stream: row.stream,
            consumer: row.consumer,
            subject: row.subject,
            bucket: row.bucket,
            prefix: row.prefix,
            batch: Batch {
                max_bytes: row.batch_max_bytes,
                max_count: row.batch_max_count,
            },
            encoding: Encoding {
                codec: row.encoding_codec.into(),
            },
        }
    }
}

impl From<StoreJob> for StoreJobRowCreate {
    fn from(job: StoreJob) -> Self {
        Self {
            id: job.id,
            name: job.name,
            status: job.status.into(),
            stream: job.stream,
            consumer: job.consumer,
            subject: job.subject,
            bucket: job.bucket,
            prefix: job.prefix,
            batch_max_bytes: job.batch.max_bytes,
            batch_max_count: job.batch.max_count,
            encoding_codec: job.encoding.codec.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkMetadataRow {
    pub sequence_number: i64,
    pub bucket: String,
    pub prefix: Option<String>,
    pub key: String,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub timestamp_start: chrono::DateTime<chrono::Utc>,
    pub timestamp_end: chrono::DateTime<chrono::Utc>,
    pub message_count: i64,
    pub size_bytes: i64,
    pub codec: EncodingCodec,
    pub hash: Vec<u8>,
    pub version: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ChunkMetadataRow {
    pub fn from_row(row: &Row) -> Result<Self, ChunkMetadataError> {
        Ok(Self {
            sequence_number: row.try_get("sequence_number")?,
            bucket: row.try_get("bucket")?,
            prefix: row.try_get("prefix")?,
            key: row.try_get("key")?,
            stream: row.try_get("stream")?,
            consumer: row.try_get("consumer")?,
            subject: row.try_get("subject")?,
            timestamp_start: row.try_get("timestamp_start")?,
            timestamp_end: row.try_get("timestamp_end")?,
            message_count: row.try_get("message_count")?,
            size_bytes: row.try_get("size_bytes")?,
            codec: row.try_get("codec")?,
            hash: row.try_get("hash")?,
            version: row.try_get("version")?,
            created_at: row.try_get("created_at")?,
            deleted_at: row.try_get("deleted_at")?,
        })
    }
}

impl From<ChunkMetadataRow> for ChunkMetadata {
    fn from(row: ChunkMetadataRow) -> Self {
        Self {
            sequence_number: row.sequence_number,
            bucket: row.bucket,
            prefix: row.prefix,
            key: row.key,
            stream: row.stream,
            consumer: row.consumer,
            subject: row.subject,
            timestamp_start: row.timestamp_start,
            timestamp_end: row.timestamp_end,
            message_count: row.message_count,
            size_bytes: row.size_bytes,
            codec: row.codec.into(),
            hash: Bytes::from(row.hash),
            version: row.version,
            created_at: row.created_at,
            deleted_at: row.deleted_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateChunkMetadataRow {
    pub bucket: String,
    pub prefix: Option<String>,
    pub key: String,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub timestamp_start: chrono::DateTime<chrono::Utc>,
    pub timestamp_end: chrono::DateTime<chrono::Utc>,
    pub message_count: i64,
    pub size_bytes: i64,
    pub codec: EncodingCodec,
    pub hash: Vec<u8>,
    pub version: String,
}

impl From<CreateChunkMetadata> for CreateChunkMetadataRow {
    fn from(chunk: CreateChunkMetadata) -> Self {
        Self {
            bucket: chunk.bucket,
            prefix: chunk.prefix,
            key: chunk.key,
            stream: chunk.stream,
            consumer: chunk.consumer,
            subject: chunk.subject,
            timestamp_start: chunk.timestamp_start,
            timestamp_end: chunk.timestamp_end,
            message_count: chunk.message_count,
            size_bytes: chunk.size_bytes,
            codec: chunk.codec.into(),
            hash: chunk.hash.to_vec(),
            version: chunk.version,
        }
    }
}
