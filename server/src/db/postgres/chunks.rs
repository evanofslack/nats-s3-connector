use async_trait::async_trait;
use tracing::debug;

use super::models::{ChunkMetadataRow, CreateChunkMetadataRow};
use super::postgres::PostgresStore;
use crate::db::{
    ChunkMetadata, ChunkMetadataError, ChunkMetadataStorer, CreateChunkMetadata, ListChunksQuery,
};

#[async_trait]
impl ChunkMetadataStorer for PostgresStore {
    async fn create_chunk(
        &self,
        chunk: CreateChunkMetadata,
    ) -> Result<ChunkMetadata, ChunkMetadataError> {
        debug!(
            bucket = chunk.bucket,
            key = chunk.key,
            prefix = chunk.prefix,
            stream = chunk.stream,
            consumer = chunk.consumer,
            subject = chunk.subject,
            message_count = chunk.message_count,
            bytes = chunk.size_bytes,
            "start create chunk"
        );
        if chunk.timestamp_start > chunk.timestamp_end {
            return Err(ChunkMetadataError::InvalidTimestampRange {
                start: chunk.timestamp_start,
                end: chunk.timestamp_end,
            });
        }

        let client = self.get_client().await?;
        let row: CreateChunkMetadataRow = chunk.into();

        let result = client
            .query_one(
                "INSERT INTO chunks 
                 (bucket, prefix, key, stream, consumer, subject, timestamp_start,
                 timestamp_end, message_count, size_bytes, codec, hash, version)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 RETURNING sequence_number, bucket, prefix, key, stream, consumer, subject,
                           timestamp_start, timestamp_end, message_count, size_bytes,
                           codec, hash, version, created_at, deleted_at",
                &[
                    &row.bucket,
                    &row.prefix,
                    &row.key,
                    &row.stream,
                    &row.consumer,
                    &row.subject,
                    &row.timestamp_start,
                    &row.timestamp_end,
                    &row.message_count,
                    &row.size_bytes,
                    &row.codec,
                    &row.hash,
                    &row.version,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code().code() == "23505" {
                        return ChunkMetadataError::Duplicate {
                            bucket: row.bucket.clone(),
                            key: row.key.clone(),
                        };
                    }
                }
                ChunkMetadataError::Database(e)
            })?;

        let chunk_row = ChunkMetadataRow::from_row(&result)?;
        Ok(chunk_row.into())
    }

    async fn get_chunk(&self, sequence_number: i64) -> Result<ChunkMetadata, ChunkMetadataError> {
        debug!(sequence_number = sequence_number, "start get chunk");
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT sequence_number, bucket, prefix, key, stream, consumer, subject,
                        timestamp_start, timestamp_end, message_count, size_bytes,
                        codec, hash, version, created_at, deleted_at
                 FROM chunks
                 WHERE sequence_number = $1",
                &[&sequence_number],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => ChunkMetadataError::Database(e),
                None => ChunkMetadataError::NotFound { sequence_number },
            })?;

        let chunk_row = ChunkMetadataRow::from_row(&row)?;
        Ok(chunk_row.into())
    }

    async fn list_chunks(
        &self,
        query: ListChunksQuery,
    ) -> Result<Vec<ChunkMetadata>, ChunkMetadataError> {
        debug!(
            bucket = query.bucket,
            stream = query.stream,
            subject = query.subject,
            prefix = query.prefix,
            limit = query.limit,
            "start list chunks"
        );
        let client = self.get_client().await?;

        let mut sql = String::from(
            "SELECT sequence_number, bucket, prefix, key, stream, consumer, subject,
                    timestamp_start, timestamp_end, message_count, size_bytes,
                    codec, hash, version, created_at, deleted_at
             FROM chunks
             WHERE stream = $1 AND subject = $2 AND bucket = $3",
        );

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            vec![&query.stream, &query.subject, &query.bucket];
        let mut param_idx = 4;

        if let Some(ref prefix) = query.prefix {
            sql.push_str(&format!(" AND prefix = ${}", param_idx));
            params.push(prefix);
            param_idx += 1;
        }

        if let Some(ref consumer) = query.consumer {
            sql.push_str(&format!(" AND consumer = ${}", param_idx));
            params.push(consumer);
            param_idx += 1;
        }

        if let Some(ref ts_start) = query.timestamp_start {
            sql.push_str(&format!(" AND timestamp_start >= ${}", param_idx));
            params.push(ts_start);
            param_idx += 1;
        }

        if let Some(ref ts_end) = query.timestamp_end {
            sql.push_str(&format!(" AND timestamp_end <= ${}", param_idx));
            params.push(ts_end);
            param_idx += 1;
        }

        if !query.include_deleted {
            sql.push_str(" AND deleted_at IS NULL");
        }

        sql.push_str(" ORDER BY timestamp_start, timestamp_end, sequence_number");

        if let Some(ref limit) = query.limit {
            sql.push_str(&format!(" LIMIT ${}", param_idx));
            params.push(limit);
        }

        let rows = client.query(&sql, &params).await?;

        rows.iter()
            .map(|row| ChunkMetadataRow::from_row(row).map(Into::into))
            .collect()
    }

    async fn soft_delete_chunk(
        &self,
        sequence_number: i64,
    ) -> Result<ChunkMetadata, ChunkMetadataError> {
        debug!(sequence_number = sequence_number, "start soft delete chunk");
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "UPDATE chunks
                 SET deleted_at = NOW()
                 WHERE sequence_number = $1
                 RETURNING sequence_number, bucket, prefix, key, stream, consumer,
                        subject, timestamp_start, timestamp_end, message_count,
                        size_bytes, codec, hash, version, created_at, deleted_at",
                &[&sequence_number],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => ChunkMetadataError::Database(e),
                None => ChunkMetadataError::NotFound { sequence_number },
            })?;

        let chunk_row = ChunkMetadataRow::from_row(&row)?;
        Ok(chunk_row.into())
    }

    async fn hard_delete_chunk(&self, sequence_number: i64) -> Result<(), ChunkMetadataError> {
        debug!(sequence_number = sequence_number, "start hard delete chunk");
        let client = self.get_client().await?;

        let rows_affected = client
            .execute(
                "DELETE FROM chunks WHERE sequence_number = $1",
                &[&sequence_number],
            )
            .await?;

        if rows_affected == 0 {
            return Err(ChunkMetadataError::NotFound { sequence_number });
        }

        Ok(())
    }
}
