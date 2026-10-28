use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle, time};
use tracing::{debug, warn};

use nats3_types::{
    LoadJob, LoadJobCreate, LoadJobStatus, StoreJob, StoreJobCreate, StoreJobStatus,
};

use crate::{db, error, io, metrics, registry};

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;

const NATS3_CONSUMER_PREFIX: &str = "nats3-";
const JOB_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

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

        let io = self.io.clone();
        let config: io::PublishConfig = job.into();
        let cancel_token = self.registry.create_cancel_token();
        let pause_token = self.registry.create_pause_token();
        let exit_tx = self.registry.create_exit_channel();
        let (go_tx, go_rx) = oneshot::channel::<()>();

        let handle = {
            let job_id = job_id.clone();
            let cancel_token = cancel_token.clone();
            let pause_token = pause_token.clone();
            tokio::spawn(async move {
                if go_rx.await.is_err() {
                    return Ok(());
                }
                let result = io
                    .publish_stream(
                        job_id.clone(),
                        config,
                        cancel_token,
                        pause_token,
                        exit_tx.clone(),
                    )
                    .await;

                if let Err(e) = &result {
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Completed(Err(e.to_string())),
                        job_id,
                        kind: registry::JobKind::Load,
                    });
                }
                result
            })
        };

        let registered = self
            .registry
            .try_register_load_job(job_id.clone(), handle, cancel_token, pause_token)
            .await;
        if !registered {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        let job = match self
            .db
            .update_load_job(job_id.clone(), LoadJobStatus::Running)
            .await
        {
            Ok(job) => job,
            Err(e) => {
                if let Some(handle) = self.registry.take_load_job(&job_id).await {
                    handle.abort();
                }
                return Err(e.into());
            }
        };

        self.inc_jobs_current(metrics::JOB_TYPE_LOAD);
        let _ = go_tx.send(());
        Ok(job)
    }

    pub async fn start_new_load_job(
        &self,
        mut job: LoadJobCreate,
    ) -> Result<LoadJob, error::AppError> {
        job.prefix = non_empty(job.prefix);
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
        let job = self.db.get_load_job(job_id).await?;
        if job.status == LoadJobStatus::Success {
            return Ok(job);
        }
        self.start_load_job(job).await
    }

    pub async fn delete_load_job(&self, job_id: String) -> Result<(), error::AppError> {
        self.registry.cancel_load_job(&job_id).await;
        if let Some(handle) = self.registry.take_load_job(&job_id).await {
            wait_for_exit(&job_id, handle).await;
        }
        self.db.delete_load_job(job_id).await?;
        Ok(())
    }

    async fn start_store_job(&self, job: StoreJob) -> Result<StoreJob, error::AppError> {
        let job_id = job.id.to_string();
        if self.registry.is_store_job_running(&job_id).await {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        let io = self.io.clone();
        let config: io::ConsumeConfig = job.into();
        let cancel_token = self.registry.create_cancel_token();
        let pause_token = self.registry.create_pause_token();
        let exit_tx = self.registry.create_exit_channel();
        let (go_tx, go_rx) = oneshot::channel::<()>();

        let handle = {
            let job_id = job_id.clone();
            let cancel_token = cancel_token.clone();
            let pause_token = pause_token.clone();
            tokio::spawn(async move {
                if go_rx.await.is_err() {
                    return Ok(());
                }
                let result = io
                    .consume_stream(
                        job_id.clone(),
                        config,
                        cancel_token,
                        pause_token,
                        exit_tx.clone(),
                    )
                    .await;

                if let Err(e) = &result {
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Completed(Err(e.to_string())),
                        job_id,
                        kind: registry::JobKind::Store,
                    });
                }
                result
            })
        };

        let registered = self
            .registry
            .try_register_store_job(job_id.clone(), handle, cancel_token, pause_token)
            .await;
        if !registered {
            return Err(registry::RegistryError::JobAlreadyRunning { job_id }.into());
        }

        let job = match self
            .db
            .update_store_job(job_id.clone(), StoreJobStatus::Running)
            .await
        {
            Ok(job) => job,
            Err(e) => {
                if let Some(handle) = self.registry.take_store_job(&job_id).await {
                    handle.abort();
                }
                return Err(e.into());
            }
        };

        self.inc_jobs_current(metrics::JOB_TYPE_STORE);
        let _ = go_tx.send(());
        Ok(job)
    }

    pub async fn start_new_store_job(
        &self,
        mut job: StoreJobCreate,
    ) -> Result<StoreJob, error::AppError> {
        job.prefix = non_empty(job.prefix);
        let mut out = self.db.create_store_job(job).await?;
        if out.consumer.is_none() {
            let consumer = format!("{}{}", NATS3_CONSUMER_PREFIX, out.id);
            out = self.db.set_store_job_consumer(out.id, consumer).await?;
        }
        self.start_store_job(out).await
    }

    pub async fn pause_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        self.registry.pause_store_job(&job_id).await;
        let status = StoreJobStatus::Paused;
        let job = self.db.update_store_job(job_id, status).await?;
        Ok(job)
    }

    pub async fn resume_store_job(&self, job_id: String) -> Result<StoreJob, error::AppError> {
        let job = self.db.get_store_job(job_id).await?;
        if job.status == StoreJobStatus::Success {
            return Ok(job);
        }
        self.start_store_job(job).await
    }

    pub async fn delete_store_job(&self, job_id: String) -> Result<(), error::AppError> {
        let job = self.db.get_store_job(job_id.clone()).await?;
        self.registry.cancel_store_job(&job_id).await;
        if let Some(handle) = self.registry.take_store_job(&job_id).await {
            wait_for_exit(&job_id, handle).await;
        }
        self.db.delete_store_job(job_id.clone()).await?;

        if let Some(consumer) = job
            .consumer
            .filter(|c| c.starts_with(NATS3_CONSUMER_PREFIX))
        {
            if let Err(e) = self
                .io
                .nats_client
                .delete_consumer(job.stream, consumer.clone())
                .await
            {
                warn!(job_id = job_id, consumer = consumer, error = ?e, "fail delete consumer");
            }
        }
        Ok(())
    }

    fn inc_jobs_current(&self, job_type: &str) {
        self.metrics
            .jobs
            .jobs_current
            .get_or_create(&metrics::JobTypeLabel {
                job_type: job_type.to_string(),
            })
            .inc();
    }
}

async fn wait_for_exit(job_id: &str, mut handle: JoinHandle<Result<()>>) {
    if time::timeout(JOB_EXIT_TIMEOUT, &mut handle).await.is_err() {
        warn!(job_id = job_id, "timeout waiting for job to exit, aborting");
        handle.abort();
    }
}

fn non_empty(prefix: Option<String>) -> Option<String> {
    prefix.filter(|p| !p.is_empty())
}
