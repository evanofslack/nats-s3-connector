use serde::{Deserialize, Serialize};
use std::string::ToString;
use strum_macros::Display;
use tracing::debug;
use ulid::Ulid;

use crate::encoding;

const DEFAULT_MAX_BYTES: i64 = 1_000_000;
const DEFAULT_MAX_COUNT: i64 = 1000;
const DEFAULT_CODEC: encoding::Codec = encoding::Codec::Binary;

#[derive(Deserialize, Clone, Debug)]
pub struct CreateStoreJob {
    pub name: String,
    pub stream: String,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch: Option<Batch>,
    pub encoding: Option<Encoding>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StoreJob {
    pub id: String,
    pub name: String,
    pub status: StoreJobStatus,
    pub stream: String,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub batch: Batch,
    pub encoding: Encoding,
}

impl StoreJob {
    pub fn new(
        name: String,
        stream: String,
        subject: String,
        bucket: String,
        prefix: Option<String>,
        batch: Batch,
        encoding: Encoding,
    ) -> Self {
        let id = Ulid::new().to_string();
        let status = StoreJobStatus::Created;
        debug!(id = id, status = status.to_string(), "new store job");
        Self {
            id,
            name,
            status,
            stream,
            subject,
            bucket,
            prefix,
            batch,
            encoding,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Display)]
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
    pub codec: encoding::Codec,
}

impl Default for Encoding {
    fn default() -> Self {
        Self {
            codec: codec_default(),
        }
    }
}

fn codec_default() -> encoding::Codec {
    DEFAULT_CODEC
}

#[derive(Deserialize, Clone, Debug)]
pub struct CreateLoadJob {
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_subject: String,
    pub write_stream: String,
    pub write_subject: String,
    pub delete_chunks: bool,
    pub start: Option<usize>,
    pub end: Option<usize>,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoadJob {
    pub id: String,
    pub status: LoadJobStatus,
    pub bucket: String,
    pub prefix: Option<String>,
    pub read_stream: String,
    pub read_subject: String,
    pub write_stream: String,
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
        read_subject: String,
        write_stream: String,
        write_subject: String,
        delete_chunks: bool,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Self {
        let id = Ulid::new().to_string();
        let status = LoadJobStatus::Created;
        debug!(id = id, status = status.to_string(), "new load job");
        Self {
            id,
            status,
            bucket,
            prefix,
            read_stream,
            read_subject,
            write_stream,
            write_subject,
            delete_chunks,
            start,
            end,
        }
    }
}

#[derive(Serialize, Clone, Debug, Display)]
pub enum LoadJobStatus {
    Created,
    Running,
    Success,
    Failure,
}
