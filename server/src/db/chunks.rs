use async_trait::async_trait;
use bytes::Bytes;
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

use nats3_types::Codec;

#[derive(Error, Debug)]
pub enum ChunkMetadataError {
    #[error("chunk not found: sequence_number={sequence_number}")]
    NotFound { sequence_number: i64 },

    #[error("duplicate chunk at location: bucket={bucket}, key={key}")]
    Duplicate { bucket: String, key: String },

    #[error("invalid timestamp range: start={start} end={end}")]
    InvalidTimestampRange {
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    },

    #[error("database error: {0}")]
    Postgres(#[from] crate::db::postgres::PostgresError),

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),
}

#[derive(Clone, Debug)]
pub struct ChunkMetadata {
    pub sequence_number: i64,
    pub bucket: String,
    pub prefix: String,
    pub key: String,
    pub stream: String,
    pub subject: String,
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
    pub prefix: String,
    pub key: String,
    pub stream: String,
    pub subject: String,
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
    pub bucket: String,
    pub prefix: String,
    pub timestamp_start: Option<chrono::DateTime<chrono::Utc>>,
    pub timestamp_end: Option<chrono::DateTime<chrono::Utc>>,
    pub limit: Option<i64>,
    pub include_deleted: bool,
}

#[async_trait]
pub trait ChunkMetadataStorer: Sync + Send + Debug {
    async fn create_chunk(
        &self,
        chunk: CreateChunkMetadata,
    ) -> Result<ChunkMetadata, ChunkMetadataError>;

    async fn get_chunk(&self, sequence_number: i64) -> Result<ChunkMetadata, ChunkMetadataError>;

    /// List chunks matching query criteria.
    /// Results ordered by: timestamp_start ASC, timestamp_end ASC
    async fn list_chunks(
        &self,
        query: ListChunksQuery,
    ) -> Result<Vec<ChunkMetadata>, ChunkMetadataError>;

    /// Soft delete chunk (sets deleted_at). Returns updated metadata.
    async fn soft_delete_chunk(
        &self,
        sequence_number: i64,
    ) -> Result<ChunkMetadata, ChunkMetadataError>;

    /// Hard delete chunk (removes from database)
    async fn hard_delete_chunk(&self, sequence_number: i64) -> Result<(), ChunkMetadataError>;
}

pub type DynChunkStorer = Arc<dyn ChunkMetadataStorer + Send + Sync>;
