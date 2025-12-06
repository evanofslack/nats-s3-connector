use postgres_types::{FromSql, ToSql};
use tokio_postgres::Row;

use crate::jobs;
// use crate::jobs::Batch;

use super::JobStoreError;

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

impl From<crate::jobs::LoadJobStatus> for LoadJobStatusEnum {
    fn from(status: crate::jobs::LoadJobStatus) -> Self {
        match status {
            crate::jobs::LoadJobStatus::Created => Self::Created,
            crate::jobs::LoadJobStatus::Running => Self::Running,
            crate::jobs::LoadJobStatus::Success => Self::Success,
            crate::jobs::LoadJobStatus::Failure => Self::Failure,
        }
    }
}

impl From<LoadJobStatusEnum> for crate::jobs::LoadJobStatus {
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
    pub read_subject: String,
    pub write_stream: String,
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
            read_subject: row.try_get("read_subject")?,
            write_stream: row.try_get("write_stream")?,
            write_subject: row.try_get("write_subject")?,
            delete_chunks: row.try_get("delete_chunks")?,
            start_pos: row.try_get("start_pos")?,
            end_pos: row.try_get("end_pos")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl From<LoadJobRow> for crate::jobs::LoadJob {
    fn from(row: LoadJobRow) -> Self {
        Self {
            id: row.id,
            status: row.status.into(),
            bucket: row.bucket,
            prefix: row.prefix,
            read_stream: row.read_stream,
            read_subject: row.read_subject,
            write_stream: row.write_stream,
            write_subject: row.write_subject,
            delete_chunks: row.delete_chunks,
            start: row.start_pos.map(|v| v as usize),
            end: row.end_pos.map(|v| v as usize),
        }
    }
}

impl From<crate::jobs::LoadJob> for LoadJobRow {
    fn from(job: crate::jobs::LoadJob) -> Self {
        Self {
            id: job.id,
            status: job.status.into(),
            bucket: job.bucket,
            prefix: job.prefix,
            read_stream: job.read_stream,
            read_subject: job.read_subject,
            write_stream: job.write_stream,
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

impl From<crate::jobs::StoreJobStatus> for StoreJobStatusEnum {
    fn from(status: crate::jobs::StoreJobStatus) -> Self {
        match status {
            crate::jobs::StoreJobStatus::Created => Self::Created,
            crate::jobs::StoreJobStatus::Running => Self::Running,
            crate::jobs::StoreJobStatus::Success => Self::Success,
            crate::jobs::StoreJobStatus::Failure => Self::Failure,
        }
    }
}

impl From<StoreJobStatusEnum> for crate::jobs::StoreJobStatus {
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

impl From<crate::encoding::Codec> for EncodingCodec {
    fn from(codec: crate::encoding::Codec) -> Self {
        match codec {
            crate::encoding::Codec::Json => Self::Json,
            crate::encoding::Codec::Binary => Self::Binary,
        }
    }
}

impl From<EncodingCodec> for crate::encoding::Codec {
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

impl From<StoreJobRow> for crate::jobs::StoreJob {
    fn from(row: StoreJobRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            status: row.status.into(),
            stream: row.stream,
            subject: row.subject,
            bucket: row.bucket,
            prefix: row.prefix,
            batch: jobs::Batch {
                max_bytes: row.batch_max_bytes,
                max_count: row.batch_max_count,
            },
            encoding: jobs::Encoding {
                codec: row.encoding_codec.into(),
            },
        }
    }
}

impl From<crate::jobs::StoreJob> for StoreJobRowCreate {
    fn from(job: crate::jobs::StoreJob) -> Self {
        Self {
            id: job.id,
            name: job.name,
            status: job.status.into(),
            stream: job.stream,
            subject: job.subject,
            bucket: job.bucket,
            prefix: job.prefix,
            batch_max_bytes: job.batch.max_bytes,
            batch_max_count: job.batch.max_count,
            encoding_codec: job.encoding.codec.into(),
        }
    }
}
