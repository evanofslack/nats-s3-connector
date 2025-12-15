use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time;
use strum_macros::Display;
use ulid::Ulid;

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
pub struct CreateStoreJob {
    pub name: String,
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch: Option<Batch>,
    pub encoding: Option<Encoding>,
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
}

#[allow(clippy::too_many_arguments)]
impl StoreJob {
    pub fn new(
        name: String,
        stream: String,
        consumer: Option<String>,
        subject: String,
        bucket: String,
        prefix: Option<String>,
        batch: Batch,
        encoding: Encoding,
    ) -> Self {
        let id = Ulid::new().to_string();
        let status = StoreJobStatus::Created;
        Self {
            id,
            name,
            status,
            stream,
            consumer,
            subject,
            bucket,
            prefix,
            batch,
            encoding,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Display, Eq, PartialEq)]
pub enum StoreJobStatus {
    Created,
    Running,
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
pub struct CreateLoadJob {
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub write_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub delete_chunks: bool,
    pub start: Option<usize>,
    pub end: Option<usize>,
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
    pub status: LoadJobStatus,
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub poll_interval: Option<time::Duration>,
    pub write_subject: String,
    pub delete_chunks: bool,
    pub start: Option<usize>,
    pub end: Option<usize>,
}

impl LoadJob {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: String,
        prefix: Option<String>,
        read_stream: String,
        read_consumer: Option<String>,
        read_subject: String,
        write_subject: String,
        poll_interval: Option<time::Duration>,
        delete_chunks: bool,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Self {
        let id = Ulid::new().to_string();
        let status = LoadJobStatus::Created;
        Self {
            id,
            status,
            bucket,
            prefix,
            read_stream,
            read_consumer,
            read_subject,
            write_subject,
            poll_interval,
            delete_chunks,
            start,
            end,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Display, Eq, PartialEq)]
pub enum LoadJobStatus {
    Created,
    Running,
    Success,
    Failure,
}
