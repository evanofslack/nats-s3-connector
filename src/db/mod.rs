use anyhow::Result;
use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::jobs;

pub mod inmem;
pub mod postgres;

pub use postgres::PostgresStore;

#[derive(Error, Debug)]
pub enum JobStoreError {
    #[error("job not found, id: {id}")]
    NotFound { id: String },

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("connection pool error: {0}")]
    Pool(String),
}

#[allow(dead_code)]
#[async_trait]
pub trait LoadJobStorer: Sync + Debug {
    async fn get_load_job(&self, id: String) -> Result<jobs::LoadJob, JobStoreError>;
    async fn get_load_jobs(&self) -> Result<Vec<jobs::LoadJob>, JobStoreError>;
    async fn create_load_job(&self, job: jobs::LoadJob) -> Result<(), JobStoreError>;
    async fn update_load_job(
        &self,
        id: String,
        status: jobs::LoadJobStatus,
    ) -> Result<jobs::LoadJob, JobStoreError>;
    async fn delete_load_job(&self, id: String) -> Result<(), JobStoreError>;
}

#[allow(dead_code)]
#[async_trait]
pub trait StoreJobStorer: Sync + Debug {
    async fn get_store_job(&self, id: String) -> Result<jobs::StoreJob, JobStoreError>;
    async fn get_store_jobs(&self) -> Result<Vec<jobs::StoreJob>, JobStoreError>;
    async fn create_store_job(&self, job: jobs::StoreJob) -> Result<(), JobStoreError>;
    async fn update_store_job(
        &self,
        id: String,
        status: jobs::StoreJobStatus,
    ) -> Result<jobs::StoreJob, JobStoreError>;
    async fn delete_store_job(&self, id: String) -> Result<(), JobStoreError>;
}

#[async_trait]
pub trait JobStorer: LoadJobStorer + StoreJobStorer {}

pub type DynStorer = Arc<dyn JobStorer + Send + Sync>;
