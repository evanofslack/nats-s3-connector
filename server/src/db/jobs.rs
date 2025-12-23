use anyhow::Result;
use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

use nats3_types::{
    ListLoadJobsQuery, ListStoreJobsQuery, LoadJob, LoadJobCreate, LoadJobStatus, StoreJob,
    StoreJobCreate, StoreJobStatus,
};

#[derive(Error, Debug)]
pub enum JobStoreError {
    #[error("job not found, id: {id}")]
    NotFound { id: String },

    #[error(transparent)]
    Postgres(#[from] crate::db::postgres::PostgresError),

    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),

    #[error("connection pool error: {0}")]
    Pool(String),
}

#[allow(dead_code)]
#[async_trait]
pub trait LoadJobStorer: Sync + Debug {
    async fn get_load_job(&self, id: String) -> Result<LoadJob, JobStoreError>;
    async fn get_load_jobs(
        &self,
        query: Option<ListLoadJobsQuery>,
    ) -> Result<Vec<LoadJob>, JobStoreError>;
    async fn create_load_job(&self, job: LoadJobCreate) -> Result<LoadJob, JobStoreError>;
    async fn update_load_job(
        &self,
        id: String,
        status: LoadJobStatus,
    ) -> Result<LoadJob, JobStoreError>;
    async fn delete_load_job(&self, id: String) -> Result<(), JobStoreError>;
}

#[allow(dead_code)]
#[async_trait]
pub trait StoreJobStorer: Sync + Debug {
    async fn get_store_job(&self, id: String) -> Result<StoreJob, JobStoreError>;
    async fn get_store_jobs(
        &self,
        query: Option<ListStoreJobsQuery>,
    ) -> Result<Vec<StoreJob>, JobStoreError>;
    async fn create_store_job(&self, job: StoreJobCreate) -> Result<StoreJob, JobStoreError>;
    async fn update_store_job(
        &self,
        id: String,
        status: StoreJobStatus,
    ) -> Result<StoreJob, JobStoreError>;
    async fn delete_store_job(&self, id: String) -> Result<(), JobStoreError>;
}

#[async_trait]
pub trait JobStorer: LoadJobStorer + StoreJobStorer {}

pub type DynJobStorer = Arc<dyn JobStorer + Send + Sync>;
