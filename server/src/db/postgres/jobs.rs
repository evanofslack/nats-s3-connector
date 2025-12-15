use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

use super::{
    models::{LoadJobRow, LoadJobStatusEnum, StoreJobRow, StoreJobRowCreate, StoreJobStatusEnum},
    postgres::PostgresStore,
};
use crate::db::{JobStoreError, JobStorer, LoadJobStorer, StoreJobStorer};
use nats3_types::{
    ListLoadJobsQuery, ListStoreJobsQuery, LoadJob, LoadJobStatus, StoreJob, StoreJobStatus,
};

#[async_trait]
impl LoadJobStorer for PostgresStore {
    async fn get_load_job(&self, id: String) -> Result<LoadJob, JobStoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT id, status, bucket, prefix, read_stream, read_consumer,
                        read_subject, write_subject, poll_interval, delete_chunks, start_pos,
                        end_pos, created_at, updated_at
                 FROM load_jobs WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => JobStoreError::Database(e),
                None => JobStoreError::NotFound { id: id.clone() },
            })?;

        let job_row = LoadJobRow::from_row(&row)?;
        Ok(job_row.into())
    }

    async fn get_load_jobs(
        &self,
        query: Option<ListLoadJobsQuery>,
    ) -> Result<Vec<LoadJob>, JobStoreError> {
        let client = self.get_client().await?;

        let mut sql = String::from("SELECT * FROM load_jobs WHERE 1=1");

        let statuses: Option<Vec<LoadJobStatusEnum>> = query.as_ref().and_then(|q| {
            q.statuses
                .as_ref()
                .map(|s| s.iter().map(|st| st.clone().into()).collect())
        });

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
        let mut param_idx = 1;

        if let Some(ref statuses) = statuses {
            if !statuses.is_empty() {
                let placeholders: Vec<String> = (0..statuses.len())
                    .map(|i| format!("${}", param_idx + i))
                    .collect();
                sql.push_str(&format!(" AND status IN ({})", placeholders.join(", ")));
                for status in statuses {
                    params.push(status);
                }
                param_idx += statuses.len();
            }
        }

        if let Some(ref q) = query {
            if let Some(ref bucket) = q.bucket {
                sql.push_str(&format!(" AND bucket = ${}", param_idx));
                params.push(bucket);
                param_idx += 1;
            }

            if let Some(ref prefix) = q.prefix {
                sql.push_str(&format!(" AND prefix = ${}", param_idx));
                params.push(prefix);
                param_idx += 1;
            }

            if let Some(ref read_stream) = q.read_stream {
                sql.push_str(&format!(" AND read_stream = ${}", param_idx));
                params.push(read_stream);
                param_idx += 1;
            }

            if let Some(ref read_consumer) = q.read_consumer {
                sql.push_str(&format!(" AND read_consumer = ${}", param_idx));
                params.push(read_consumer);
                param_idx += 1;
            }

            if let Some(ref read_subject) = q.read_subject {
                sql.push_str(&format!(" AND read_subject = ${}", param_idx));
                params.push(read_subject);
                param_idx += 1;
            }

            if let Some(ref write_subject) = q.write_subject {
                sql.push_str(&format!(" AND write_subject = ${}", param_idx));
                params.push(write_subject);
                param_idx += 1;
            }
        }

        sql.push_str(" ORDER BY created_at DESC");

        if let Some(ref q) = query {
            if let Some(ref limit) = q.limit {
                sql.push_str(&format!(" LIMIT ${}", param_idx));
                params.push(limit);
            }
        }

        let rows = client.query(&sql, &params).await?;

        rows.iter()
            .map(|row| LoadJobRow::from_row(row).map(Into::into))
            .collect()
    }

    async fn create_load_job(&self, job: LoadJob) -> Result<(), JobStoreError> {
        let client = self.get_client().await?;
        let row: LoadJobRow = job.into();

        client
            .execute(
                "INSERT INTO load_jobs 
             (id, status, bucket, prefix, read_stream, read_consumer,
              read_subject, write_subject, poll_interval, delete_chunks, start_pos, end_pos)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                &[
                    &row.id,
                    &row.status,
                    &row.bucket,
                    &row.prefix,
                    &row.read_stream,
                    &row.read_consumer,
                    &row.read_subject,
                    &row.write_subject,
                    &row.poll_interval,
                    &row.delete_chunks,
                    &row.start_pos,
                    &row.end_pos,
                ],
            )
            .await?;
        Ok(())
    }

    async fn update_load_job(
        &self,
        id: String,
        status: LoadJobStatus,
    ) -> Result<LoadJob, JobStoreError> {
        let client = self.get_client().await?;
        let status_row: LoadJobStatusEnum = status.into();

        let row = client
            .query_one(
                "UPDATE load_jobs 
             SET status = $1, updated_at = NOW() 
             WHERE id = $2
             RETURNING *",
                &[&status_row, &id],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => JobStoreError::Database(e),
                None => JobStoreError::NotFound { id: id.clone() },
            })?;

        let job_row = LoadJobRow::from_row(&row)?;
        Ok(job_row.into())
    }

    async fn delete_load_job(&self, id: String) -> Result<(), JobStoreError> {
        debug!(job_id = id, "delete load job postgres");
        let client = self.get_client().await?;

        let rows_affected = client
            .execute("DELETE FROM load_jobs WHERE id = $1", &[&id])
            .await?;

        if rows_affected == 0 {
            return Err(JobStoreError::NotFound { id });
        }

        Ok(())
    }
}

#[async_trait]
impl StoreJobStorer for PostgresStore {
    async fn get_store_job(&self, id: String) -> Result<StoreJob, JobStoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT id, name, status, stream, consumer, subject,
                 bucket, prefix, batch_max_bytes, batch_max_count,
                 encoding_codec, created_at, updated_at
                 FROM store_jobs WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => JobStoreError::Database(e),
                None => JobStoreError::NotFound { id: id.clone() },
            })?;

        let job_row = StoreJobRow::from_row(&row)?;
        Ok(job_row.into())
    }

    async fn get_store_jobs(
        &self,
        query: Option<ListStoreJobsQuery>,
    ) -> Result<Vec<StoreJob>, JobStoreError> {
        let client = self.get_client().await?;

        let mut sql = String::from("SELECT * FROM store_jobs WHERE 1=1");

        let statuses: Option<Vec<StoreJobStatusEnum>> = query.as_ref().and_then(|q| {
            q.statuses
                .as_ref()
                .map(|s| s.iter().map(|st| st.clone().into()).collect())
        });

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
        let mut param_idx = 1;

        if let Some(ref statuses) = statuses {
            if !statuses.is_empty() {
                let placeholders: Vec<String> = (0..statuses.len())
                    .map(|i| format!("${}", param_idx + i))
                    .collect();
                sql.push_str(&format!(" AND status IN ({})", placeholders.join(", ")));
                for status in statuses {
                    params.push(status);
                }
                param_idx += statuses.len();
            }
        }

        if let Some(ref q) = query {
            if let Some(ref stream) = q.stream {
                sql.push_str(&format!(" AND stream = ${}", param_idx));
                params.push(stream);
                param_idx += 1;
            }

            if let Some(ref consumer) = q.consumer {
                sql.push_str(&format!(" AND consumer = ${}", param_idx));
                params.push(consumer);
                param_idx += 1;
            }

            if let Some(ref subject) = q.subject {
                sql.push_str(&format!(" AND subject = ${}", param_idx));
                params.push(subject);
                param_idx += 1;
            }

            if let Some(ref bucket) = q.bucket {
                sql.push_str(&format!(" AND bucket = ${}", param_idx));
                params.push(bucket);
                param_idx += 1;
            }

            if let Some(ref prefix) = q.prefix {
                sql.push_str(&format!(" AND prefix = ${}", param_idx));
                params.push(prefix);
                param_idx += 1;
            }
        }

        sql.push_str(" ORDER BY created_at DESC");

        if let Some(ref q) = query {
            if let Some(ref limit) = q.limit {
                sql.push_str(&format!(" LIMIT ${}", param_idx));
                params.push(limit);
            }
        }

        let rows = client.query(&sql, &params).await?;

        rows.iter()
            .map(|row| StoreJobRow::from_row(row).map(Into::into))
            .collect()
    }

    async fn create_store_job(&self, job: StoreJob) -> Result<(), JobStoreError> {
        let client = self.get_client().await?;
        let row: StoreJobRowCreate = job.into();

        client
            .execute(
                "INSERT INTO store_jobs 
                (id, name, status, stream, consumer, subject, bucket,
                prefix, batch_max_bytes, batch_max_count, encoding_codec)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &row.id,
                    &row.name,
                    &row.status,
                    &row.stream,
                    &row.consumer,
                    &row.subject,
                    &row.bucket,
                    &row.prefix,
                    &row.batch_max_bytes,
                    &row.batch_max_count,
                    &row.encoding_codec,
                ],
            )
            .await?;

        Ok(())
    }

    async fn update_store_job(
        &self,
        id: String,
        status: StoreJobStatus,
    ) -> Result<StoreJob, JobStoreError> {
        let client = self.get_client().await?;
        let status_row: StoreJobStatusEnum = status.into();

        let row = client
            .query_one(
                "UPDATE store_jobs 
             SET status = $1, updated_at = NOW() 
             WHERE id = $2
             RETURNING *",
                &[&status_row, &id],
            )
            .await
            .map_err(|e| match e.as_db_error() {
                Some(_) => JobStoreError::Database(e),
                None => JobStoreError::NotFound { id: id.clone() },
            })?;

        let job_row = StoreJobRow::from_row(&row)?;
        Ok(job_row.into())
    }

    async fn delete_store_job(&self, id: String) -> Result<(), JobStoreError> {
        debug!(job_id = id, "delete store job postgres");
        let client = self.get_client().await?;

        let rows_affected = client
            .execute("DELETE FROM store_jobs WHERE id = $1", &[&id])
            .await?;

        if rows_affected == 0 {
            return Err(JobStoreError::NotFound { id });
        }
        Ok(())
    }
}

impl JobStorer for PostgresStore {}
