use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Debug)]
pub struct CreateLoadJob {
    pub bucket: String,
    pub read_stream: String,
    pub read_subject: String,
    pub write_stream: String,
    pub write_subject: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct LoadJob {
    pub id: String,
    pub status: LoadJobStatus,
    pub bucket: String,
    pub read_stream: String,
    pub read_subject: String,
    pub write_stream: String,
    pub write_subject: String,
}

impl LoadJob {
    pub fn new(
        bucket: String,
        read_stream: String,
        read_subject: String,
        write_stream: String,
        write_subject: String,
    ) -> Self {
        // TODO: ULID
        let id = "".to_string();
        let status = LoadJobStatus::Pending;
        Self {
            id,
            status,
            bucket,
            read_stream,
            read_subject,
            write_stream,
            write_subject,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub enum LoadJobStatus {
    Pending,
    _Finished,
    _Unknown,
}
