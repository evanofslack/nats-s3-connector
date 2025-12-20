use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};

use nats3_types::{LoadJob, LoadJobStatus, StoreJob, StoreJobStatus};

use crate::{db, error, io, registry};

#[derive(Debug, Clone)]
pub struct Coordinator {
    registry: Arc<registry::Registry>,
    io: io::IO,
    db: db::DynJobStorer,
}

impl Coordinator {
    pub fn new(registry: Arc<registry::Registry>, io: io::IO, db: db::DynJobStorer) -> Self {
        debug!("create new coordinator");
        Self { registry, io, db }
    }

    pub async fn start_new_load_job(
        &self,
        job: LoadJob,
        config: io::PublishConfig,
    ) -> Result<LoadJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_load_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        self.db.create_load_job(job.clone()).await?;
        let io = self.io.clone();
        let registry_config = config.clone();

        // Cancel token is child of shutdown (for stop/shutdown)
        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();

        // Pause token is independent (for pause)
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            io.publish_stream(registry_config, cancel_token, pause_token)
                .await
        });

        let registered = self
            .registry
            .try_register_load_job(
                job_id.clone(),
                handle,
                cancel_token_clone,
                pause_token_clone,
            )
            .await;

        let status = if registered {
            LoadJobStatus::Running
        } else {
            LoadJobStatus::Failure
        };

        self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn pause_load_job(&self, job_id: String) -> Result<LoadJob, error::AppError> {
        self.registry.pause_load_job(&job_id).await;
        let status = LoadJobStatus::Paused;
        let job = self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn resume_load_job(&self, job_id: String) -> Result<LoadJob, error::AppError> {
        let job = self.db.get_load_job(job_id.clone()).await?;
        if job.status != LoadJobStatus::Paused {
            return Ok(job);
        }
        let config: io::PublishConfig = job.clone().into();
        self.restart_load_job(job, config).await?;
        let status = LoadJobStatus::Running;
        let job = self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn restart_load_job(
        &self,
        job: LoadJob,
        config: io::PublishConfig,
    ) -> Result<LoadJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_load_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        let io = self.io.clone();
        let registry_config = config.clone();

        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            io.publish_stream(registry_config, cancel_token, pause_token)
                .await
        });

        let registered = self
            .registry
            .try_register_load_job(
                job_id.clone(),
                handle,
                cancel_token_clone,
                pause_token_clone,
            )
            .await;

        let status = if registered {
            LoadJobStatus::Running
        } else {
            LoadJobStatus::Failure
        };

        self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn stop_load_job(&self, job_id: String) {
        self.registry.cancel_load_job(&job_id).await
    }

    pub async fn start_new_store_job(
        &self,
        job: StoreJob,
        config: io::ConsumeConfig,
    ) -> Result<StoreJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_store_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        self.db.create_store_job(job.clone()).await?;
        let io = self.io.clone();
        let registry_config = config.clone();

        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            io.consume_stream(registry_config, cancel_token, pause_token)
                .await
        });

        let registered = self
            .registry
            .try_register_store_job(
                job_id.clone(),
                handle,
                cancel_token_clone,
                pause_token_clone,
            )
            .await;

        let status = if registered {
            StoreJobStatus::Running
        } else {
            StoreJobStatus::Failure
        };

        self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn pause_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        self.registry.pause_store_job(&job_id).await;
        let status = StoreJobStatus::Paused;
        let job = self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn resume_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        let job = self.db.get_store_job(job_id.clone()).await?;
        if job.status != StoreJobStatus::Paused {
            return Ok(job);
        }
        let config: io::ConsumeConfig = job.clone().into();
        self.restart_store_job(job, config).await?;
        let status = StoreJobStatus::Running;
        let job = self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn restart_store_job(
        &self,
        job: StoreJob,
        config: io::ConsumeConfig,
    ) -> Result<StoreJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_store_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        let io = self.io.clone();
        let registry_config = config.clone();

        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            io.consume_stream(registry_config, cancel_token, pause_token)
                .await
        });

        let registered = self
            .registry
            .try_register_store_job(
                job_id.clone(),
                handle,
                cancel_token_clone,
                pause_token_clone,
            )
            .await;

        let status = if registered {
            StoreJobStatus::Running
        } else {
            StoreJobStatus::Failure
        };

        self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn stop_store_job(&self, job_id: String) {
        self.registry.cancel_store_job(&job_id).await
    }
}

#[async_trait]
impl registry::LoadJobCompletionHandler for Coordinator {
    async fn handle_job_completion(&self, job_id: &str, result: registry::JobResult) -> Result<()> {
        let status = match result {
            registry::JobResult::Success => {
                debug!(job_id = job_id, "load job handle completed successfully");
                LoadJobStatus::Success
            }
            registry::JobResult::Failed(e) => {
                warn!(
                    job_id = job_id,
                    error = e,
                    "load job handle completed with error"
                );
                LoadJobStatus::Failure
            }
            registry::JobResult::Panicked(e) => {
                warn!(job_id = job_id, error = e, "load job handle panicked");
                LoadJobStatus::Failure
            }
        };
        self.db.update_load_job(job_id.to_string(), status).await?;
        Ok(())
    }
}

#[async_trait]
impl registry::StoreJobCompletionHandler for Coordinator {
    async fn handle_job_completion(&self, job_id: &str, result: registry::JobResult) -> Result<()> {
        let status = match result {
            registry::JobResult::Success => {
                debug!(job_id = job_id, "store job handle completed successfully");
                StoreJobStatus::Success
            }
            registry::JobResult::Failed(e) => {
                warn!(job_id = job_id, error = %e, "store job handle completed with error");
                StoreJobStatus::Failure
            }
            registry::JobResult::Panicked(e) => {
                warn!(job_id = job_id, error = %e, "store job handle panicked");
                StoreJobStatus::Failure
            }
        };
        self.db.update_store_job(job_id.to_string(), status).await?;
        Ok(())
    }
}
