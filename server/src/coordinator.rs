use anyhow::Result;
use std::sync::Arc;
use tracing::debug;

use nats3_types::{
    LoadJob, LoadJobCreate, LoadJobStatus, StoreJob, StoreJobCreate, StoreJobStatus,
};

use crate::{db, error, io, metrics, registry};

#[derive(Debug, Clone)]
pub struct Coordinator {
    registry: Arc<registry::Registry>,
    io: io::IO,
    db: db::DynJobStorer,
    metrics: metrics::Metrics,
}

impl Coordinator {
    pub fn new(
        registry: Arc<registry::Registry>,
        io: io::IO,
        db: db::DynJobStorer,
        metrics: metrics::Metrics,
    ) -> Self {
        debug!("create new coordinator");
        Self {
            registry,
            io,
            db,
            metrics,
        }
    }

    async fn start_load_job(&self, job: LoadJob) -> Result<LoadJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_load_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        self.metrics
            .jobs
            .jobs_current
            .get_or_create(&metrics::JobTypeLabel {
                job_type: metrics::JOB_TYPE_LOAD.to_string(),
            })
            .inc();

        let io = self.io.clone();
        let config: io::PublishConfig = job.clone().into();
        let registry_config = config.clone();

        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();
        let exit_tx = self.registry.create_exit_channel();
        let exit_tx_clone = exit_tx.clone();
        let job_id_clone = job_id.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let result = io
                .publish_stream(
                    job_id.clone(),
                    registry_config,
                    cancel_token,
                    pause_token,
                    exit_tx,
                )
                .await;

            if let Err(e) = &result {
                let _ = exit_tx_clone.send(registry::TaskExitInfo {
                    reason: registry::TaskExitReason::Completed(Err(e.to_string())),
                    job_id,
                });
            }
            result
        });

        let registered = self
            .registry
            .try_register_load_job(
                job_id_clone.clone(),
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

        self.db.update_load_job(job_id_clone, status).await?;
        Ok(job)
    }

    pub async fn start_new_load_job(&self, job: LoadJobCreate) -> Result<LoadJob, error::AppError> {
        let out = self.db.create_load_job(job.clone()).await?;
        self.start_load_job(out).await
    }

    pub async fn pause_load_job(&self, job_id: String) -> Result<LoadJob, error::AppError> {
        self.registry.pause_load_job(&job_id).await;
        let status = LoadJobStatus::Paused;
        let job = self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn resume_load_job(&self, job_id: String) -> Result<LoadJob, error::AppError> {
        let job = self.db.get_load_job(job_id.clone()).await?;
        if job.status == LoadJobStatus::Success || job.status == LoadJobStatus::Failure {
            return Ok(job);
        }
        self.start_load_job(job).await?;
        let status = LoadJobStatus::Running;
        let job = self.db.update_load_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn stop_load_job(&self, job_id: String) {
        self.registry.cancel_load_job(&job_id).await
    }

    async fn start_store_job(&self, job: StoreJob) -> Result<StoreJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_store_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        self.metrics
            .jobs
            .jobs_current
            .get_or_create(&metrics::JobTypeLabel {
                job_type: metrics::JOB_TYPE_STORE.to_string(),
            })
            .inc();

        let io = self.io.clone();
        let config: io::ConsumeConfig = job.clone().into();
        let registry_config = config.clone();

        let cancel_token = self.registry.create_cancel_token();
        let cancel_token_clone = cancel_token.clone();
        let pause_token = self.registry.create_pause_token();
        let pause_token_clone = pause_token.clone();
        let exit_tx = self.registry.create_exit_channel();
        let exit_tx_clone = exit_tx.clone();
        let job_id_clone = job_id.clone();

        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let result = io
                .consume_stream(
                    job_id.clone(),
                    registry_config,
                    cancel_token,
                    pause_token,
                    exit_tx,
                )
                .await;

            if let Err(e) = &result {
                let _ = exit_tx_clone.send(registry::TaskExitInfo {
                    reason: registry::TaskExitReason::Completed(Err(e.to_string())),
                    job_id,
                });
            }
            result
        });

        let registered = self
            .registry
            .try_register_store_job(
                job_id_clone.clone(),
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

        self.db.update_store_job(job_id_clone, status).await?;
        Ok(job)
    }

    pub async fn start_new_store_job(
        &self,
        job: StoreJobCreate,
    ) -> Result<StoreJob, error::AppError> {
        let out = self.db.create_store_job(job.clone()).await?;
        self.start_store_job(out).await
    }

    pub async fn pause_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        self.registry.pause_store_job(&job_id).await;
        let status = StoreJobStatus::Paused;
        let job = self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn resume_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        let job = self.db.get_store_job(job_id.clone()).await?;
        if job.status == StoreJobStatus::Success || job.status == StoreJobStatus::Failure {
            return Ok(job);
        }
        self.start_store_job(job).await?;
        let status = StoreJobStatus::Running;
        let job = self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn stop_store_job(&self, job_id: String) {
        self.registry.cancel_store_job(&job_id).await
    }
}
