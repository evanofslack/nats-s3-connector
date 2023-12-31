use serde::{Deserialize, Serialize};
use std::string::ToString;
use strum_macros::Display;
use tracing::debug;
use ulid::Ulid;

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
