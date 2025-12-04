mod models;

use anyhow::Result;
use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use refinery::embed_migrations;
use tokio_postgres::{Config as PgConfig, NoTls};

use super::{JobStoreError, JobStorer, LoadJobStorer, StoreJobStorer};
use crate::jobs;
use models::{LoadJobRow, StoreJobRow, StoreJobRowCreate};

embed_migrations!("src/db/postgres/migrations");

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> Result<Self, JobStoreError> {
        let config: PgConfig = database_url
            .parse()
            .map_err(|e| JobStoreError::Pool(format!("invalid config: {}", e)))?;

        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(|e| JobStoreError::Pool(e.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), JobStoreError> {
        let mut client = self.get_client().await?;
        migrations::runner()
            .run_async(&mut *client)
            .await
            .map_err(|e| JobStoreError::Pool(format!("migration failed: {}", e)))?;
        Ok(())
    }

    async fn get_client(
        &self,
    ) -> Result<PooledConnection<'_, PostgresConnectionManager<NoTls>>, JobStoreError> {
        self.pool
            .get()
            .await
            .map_err(|e| JobStoreError::Pool(e.to_string()))
    }
}

#[async_trait]
impl LoadJobStorer for PostgresStore {
    async fn get_load_job(&self, id: String) -> Result<jobs::LoadJob, JobStoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT id, status, bucket, prefix, read_stream, read_subject,
                        write_stream, write_subject, delete_chunks, start_pos, end_pos,
                        created_at, updated_at
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

    async fn get_load_jobs(&self) -> Result<Vec<jobs::LoadJob>, JobStoreError> {
        let client = self.get_client().await?;

        let rows = client
            .query("SELECT * FROM load_jobs ORDER BY created_at DESC", &[])
            .await?;

        rows.iter()
            .map(|row| LoadJobRow::from_row(row).map(Into::into))
            .collect()
    }

    async fn create_load_job(&self, job: jobs::LoadJob) -> Result<(), JobStoreError> {
        let client = self.get_client().await?;
        let row: LoadJobRow = job.into();

        client
            .execute(
                "INSERT INTO load_jobs 
             (id, status, bucket, prefix, read_stream, read_subject,
              write_stream, write_subject, delete_chunks, start_pos, end_pos)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &row.id,
                    &row.status,
                    &row.bucket,
                    &row.prefix,
                    &row.read_stream,
                    &row.read_subject,
                    &row.write_stream,
                    &row.write_subject,
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
        status: jobs::LoadJobStatus,
    ) -> Result<jobs::LoadJob, JobStoreError> {
        let client = self.get_client().await?;
        let status_row: models::LoadJobStatusEnum = status.into();

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
    async fn get_store_job(&self, id: String) -> Result<jobs::StoreJob, JobStoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT id, name, status, stream, subject, bucket, prefix,
                 batch_max_bytes, batch_max_count, encoding_codec,
                 created_at, updated_at,
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

    async fn get_store_jobs(&self) -> Result<Vec<jobs::StoreJob>, JobStoreError> {
        let client = self.get_client().await?;

        let rows = client
            .query("SELECT * FROM store_jobs ORDER BY created_at DESC", &[])
            .await?;

        rows.iter()
            .map(|row| StoreJobRow::from_row(row).map(Into::into))
            .collect()
    }

    async fn create_store_job(&self, job: jobs::StoreJob) -> Result<(), JobStoreError> {
        let client = self.get_client().await?;
        let row: StoreJobRowCreate = job.into();

        client
            .execute(
                "INSERT INTO store_jobs 
                (id, name, status, stream, subject, bucket, prefix,
                batch_max_bytes, batch_max_count, encoding_codec,
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                &[
                    &row.id,
                    &row.name,
                    &row.status,
                    &row.stream,
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
        status: jobs::StoreJobStatus,
    ) -> Result<jobs::StoreJob, JobStoreError> {
        let client = self.get_client().await?;
        let status_row: models::StoreJobStatusEnum = status.into();

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
