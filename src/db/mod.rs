use anyhow::Result;
use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

use crate::jobs;

pub mod inmem;

#[derive(Error, Debug)]
pub enum JobStoreError {
    #[error("job not found, id: {id}")]
    NotFound { id: String },
}

#[async_trait]
pub trait LoadJobStorer: Sync + Debug {
    async fn get_load_job(&self, id: String) -> Result<jobs::LoadJob, JobStoreError>;
    async fn get_load_jobs(&self) -> Result<Vec<jobs::LoadJob>, JobStoreError>;
    async fn update_load_job(
        &self,
        id: String,
        status: jobs::LoadJobStatus,
    ) -> Result<jobs::LoadJob, JobStoreError>;
    async fn create_load_job(&self, job: jobs::LoadJob) -> Result<(), JobStoreError>;
    async fn delete_load_job(&self, id: String) -> Result<(), JobStoreError>;
}

#[async_trait]
pub trait StoreJobStorer: Sync + Debug {
    async fn get_store_job(&self, id: String) -> Result<jobs::LoadJob, JobStoreError>;
    async fn get_store_jobs(&self) -> Result<Vec<jobs::LoadJob>, JobStoreError>;
    async fn create_store_job(&self, job: jobs::LoadJob) -> Result<(), JobStoreError>;
    async fn delete_store_job(&self, id: String) -> Result<(), JobStoreError>;
}

pub type DynStorer = Arc<dyn LoadJobStorer + Send + Sync>;
