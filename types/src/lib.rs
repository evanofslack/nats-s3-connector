use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time};
use strum_macros::Display;
use thiserror::Error;

const DEFAULT_MAX_BYTES: i64 = 1_000_000;
const DEFAULT_MAX_COUNT: i64 = 1000;
const DEFAULT_CODEC: Codec = Codec::Binary;

#[derive(Serialize, Deserialize, Clone, Debug, Display, Eq, PartialEq)]
pub enum Codec {
    #[serde(alias = "json", alias = "JSON")]
    Json,
    #[serde(alias = "binary", alias = "bin")]
    Binary,
}

impl Codec {
    pub fn to_extension(&self) -> &str {
        match self {
            Codec::Json => "json",
            Codec::Binary => "bin",
        }
    }
}

#[derive(Debug)]
pub struct CodecParseError(String);

impl std::fmt::Display for CodecParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CodecParseError {}

impl FromStr for Codec {
    type Err = CodecParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(Self::Json),
            "bin" => Ok(Self::Binary),
            "binary" => Ok(Self::Binary),
            _ => Err(CodecParseError(format!(
                "Invalid codec '{}'. Valid options: json, bin",
                s
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StoreJobCreate {
    pub name: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub batch: Batch,
    pub encoding: Encoding,
}

#[derive(Clone, Debug, Default)]
pub struct ListStoreJobsQuery {
    pub statuses: Option<Vec<StoreJobStatus>>,
    pub stream: Option<String>,
    pub consumer: Option<String>,
    pub subject: Option<String>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<i64>,
}

impl ListStoreJobsQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(mut self, status: StoreJobStatus) -> Self {
        self.statuses = Some(vec![status]);
        self
    }

    pub fn with_statuses(mut self, statuses: Vec<StoreJobStatus>) -> Self {
        self.statuses = Some(statuses);
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StoreJob {
    pub id: String,
    pub name: String,
    pub status: StoreJobStatus,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch: Batch,
    pub encoding: Encoding,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Display, Eq, PartialEq)]
pub enum StoreJobStatus {
    Created,
    Running,
    Paused,
    Success,
    Failure,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Batch {
    #[serde(default = "max_bytes_default")]
    pub max_bytes: i64,
    #[serde(default = "max_count_default")]
    pub max_count: i64,
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            max_bytes: max_bytes_default(),
            max_count: max_count_default(),
        }
    }
}

fn max_bytes_default() -> i64 {
    DEFAULT_MAX_BYTES
}

fn max_count_default() -> i64 {
    DEFAULT_MAX_COUNT
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Encoding {
    #[serde(default = "codec_default")]
    pub codec: Codec,
}

impl Default for Encoding {
    fn default() -> Self {
        Self {
            codec: codec_default(),
        }
    }
}

fn codec_default() -> Codec {
    DEFAULT_CODEC
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadJobCreate {
    pub name: String,
    pub store_job_id: Option<String>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub read_stream: Option<String>,
    pub read_consumer: Option<String>,
    pub read_subject: Option<String>,
    pub write_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub delete_chunks: bool,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LoadJobCreateValidated {
    FromStore(LoadJobCreateFromStore),
    Direct(LoadJobCreateDirect),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadJobCreateFromStore {
    pub name: String,
    pub store_job_id: String,
    pub write_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub delete_chunks: bool,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadJobCreateDirect {
    pub name: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub write_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub delete_chunks: bool,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("load job with polling must delete chunks")]
    PollMustDelete,
    #[error("load job must provide either store job id, or bucket, stream and subject")]
    MissingRequiredFields,
}

impl LoadJobCreate {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.poll_interval.is_some() && !self.delete_chunks {
            return Err(ValidationError::PollMustDelete);
        }
        Ok(())
    }
}

impl TryFrom<LoadJobCreate> for LoadJobCreateValidated {
    type Error = ValidationError;

    fn try_from(c: LoadJobCreate) -> Result<Self, ValidationError> {
        if c.poll_interval.is_some() && !c.delete_chunks {
            return Err(ValidationError::PollMustDelete);
        }
        if let Some(store_id) = c.store_job_id {
            return Ok(Self::FromStore(LoadJobCreateFromStore {
                name: c.name,
                store_job_id: store_id.to_string(),
                write_subject: c.write_subject,
                poll_interval: c.poll_interval,
                delete_chunks: c.delete_chunks,
                from_time: c.from_time,
                to_time: c.to_time,
            }));
        }
        if let (Some(bucket), Some(read_stream), Some(read_subject)) =
            (c.bucket, c.read_stream, c.read_subject)
        {
            return Ok(Self::Direct(LoadJobCreateDirect {
                name: c.name,
                bucket,
                prefix: c.prefix,
                read_stream,
                read_subject,
                read_consumer: c.read_consumer,
                write_subject: c.write_subject,
                poll_interval: c.poll_interval,
                delete_chunks: c.delete_chunks,
                from_time: c.from_time,
                to_time: c.to_time,
            }));
        }
        Err(ValidationError::MissingRequiredFields)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ListLoadJobsQuery {
    pub statuses: Option<Vec<LoadJobStatus>>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub read_stream: Option<String>,
    pub read_consumer: Option<String>,
    pub read_subject: Option<String>,
    pub write_subject: Option<String>,
    pub limit: Option<i64>,
}

impl ListLoadJobsQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(mut self, status: LoadJobStatus) -> Self {
        self.statuses = Some(vec![status]);
        self
    }

    pub fn with_statuses(mut self, statuses: Vec<LoadJobStatus>) -> Self {
        self.statuses = Some(statuses);
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadJob {
    pub id: String,
    pub name: String,
    pub status: LoadJobStatus,
    pub store_job_id: Option<String>,
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub write_subject: String,
    pub delete_chunks: bool,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Display, Eq, PartialEq)]
pub enum LoadJobStatus {
    Created,
    Running,
    Paused,
    Success,
    Failure,
}
