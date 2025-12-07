use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use thiserror::Error;

use nats3_types::Codec;

#[derive(Error, Debug)]
pub enum ChunkMetadataError {
    #[error("chunk not found, id: {id}")]
    NotFound { id: String },

    #[error("chunk not found at location, bucket: {bucket}, key: {key}")]
    NotFoundAtLocation { bucket: String, key: String },

    #[error("duplicate chunk, bucket: {bucket}, key: {key}")]
    Duplicate { bucket: String, key: String },

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("connection pool error: {0}")]
    Pool(String),

    #[error("invalid timestamp range: start={start} end={end}")]
    InvalidTimestampRange { start: i64, end: i64 },
}

#[derive(Clone, Debug)]
pub struct ChunkMetadata {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub stream: String,
    pub subject: String,
    pub sequence_number: i64,
    pub timestamp_start: chrono::DateTime<chrono::Utc>,
    pub timestamp_end: chrono::DateTime<chrono::Utc>,
    pub message_count: i64,
    pub size_bytes: i64,
    pub codec: Codec,
    pub hash: Bytes,
    pub version: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug)]
pub struct CreateChunkMetadata {
    pub bucket: String,
    pub key: String,
    pub stream: String,
    pub subject: String,
    pub sequence_number: Option<i64>,
    pub timestamp_start: chrono::DateTime<chrono::Utc>,
    pub timestamp_end: chrono::DateTime<chrono::Utc>,
    pub message_count: i64,
    pub size_bytes: i64,
    pub codec: Codec,
    pub hash: Bytes,
    pub version: String,
}

#[derive(Clone, Debug)]
pub struct ListChunksQuery {
    pub stream: String,
    pub subject: String,
    pub timestamp_start: Option<i64>,
    pub timestamp_end: Option<i64>,
    pub limit: Option<i32>,
    pub include_deleted: bool,
}

#[async_trait]
pub trait ChunkMetadataStore: Sync + Send + Debug {
    async fn create(&self, chunk: CreateChunkMetadata) -> Result<ChunkMetadata, ChunkMetadataError>;
    
    async fn get(&self, id: String) -> Result<ChunkMetadata, ChunkMetadataError>;
    
    async fn get_by_location(
        &self,
        bucket: String,
        key: String,
    ) -> Result<ChunkMetadata, ChunkMetadataError>;
    
    async fn list(&self, query: ListChunksQuery) -> Result<Vec<ChunkMetadata>, ChunkMetadataError>;
    
    async fn soft_delete(&self, id: String) -> Result<ChunkMetadata, ChunkMetadataError>;
    
    async fn hard_delete(&self, id: String) -> Result<(), ChunkMetadataError>;
    
    async fn get_next_sequence(&self, stream: &str, subject: &str) -> Result<i64, ChunkMetadataError>;
}
