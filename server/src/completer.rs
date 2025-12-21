use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{db, registry};
use nats3_types::{LoadJobStatus, StoreJobStatus};

#[derive(Clone, Debug)]
pub struct TaskCompleter {
    db: db::DynJobStorer,
    registry: Arc<registry::Registry>,
}

impl TaskCompleter {
    pub fn new(db: db::DynJobStorer, registry: Arc<registry::Registry>) -> Self {
        Self { db, registry }
    }

    pub fn start(self, shutdown_token: CancellationToken) {
        let exit_rx = self.registry.subscribe_to_exits();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(exit_info) = async {
                        exit_rx.lock().await.recv().await
                    } => {
                        if let Err(e) = self.handle_exit(exit_info).await {
                            warn!(error = ?e, "error handling task exit");
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        debug!("task completer shutting down");
                        self.drain_remaining_exits(exit_rx).await;
                        break;
                    }
                }
            }
        });
    }

    async fn handle_exit(&self, exit_info: registry::TaskExitInfo) -> Result<()> {
        let job_id = exit_info.job_id.clone();

        // Check which type of job this is
        let is_store = self.registry.is_store_job_running(&job_id).await;

        match exit_info.reason {
            registry::TaskExitReason::Completed(Ok(())) => {
                debug!(
                    job_id = job_id,
                    is_store = is_store,
                    "task completed successfully"
                );
                if is_store {
                    self.db
                        .update_store_job(job_id.clone(), StoreJobStatus::Success)
                        .await?;
                } else {
                    self.db
                        .update_load_job(job_id.clone(), LoadJobStatus::Success)
                        .await?;
                }
            }
            registry::TaskExitReason::Completed(Err(ref e)) => {
                warn!(
                    job_id = job_id,
                    is_store = is_store,
                    error = e,
                    "task failed"
                );
                if is_store {
                    self.db
                        .update_store_job(job_id.clone(), StoreJobStatus::Failure)
                        .await?;
                } else {
                    self.db
                        .update_load_job(job_id.clone(), LoadJobStatus::Failure)
                        .await?;
                }
            }
            registry::TaskExitReason::Paused => {
                debug!(job_id = job_id, is_store = is_store, "task paused");
                if is_store {
                    self.db
                        .update_store_job(job_id.clone(), StoreJobStatus::Paused)
                        .await?;
                } else {
                    self.db
                        .update_load_job(job_id.clone(), LoadJobStatus::Paused)
                        .await?;
                }
            }
            registry::TaskExitReason::Cancelled => {
                debug!(job_id = job_id, "task cancelled, skip DB update");
            }
        }

        // Remove from registry
        self.registry.remove_job(&job_id).await;

        Ok(())
    }

    async fn drain_remaining_exits(
        &self,
        exit_rx: Arc<Mutex<mpsc::UnboundedReceiver<registry::TaskExitInfo>>>,
    ) {
        let mut rx = exit_rx.lock().await;
        while let Ok(exit_info) = rx.try_recv() {
            if let Err(e) = self.handle_exit(exit_info).await {
                warn!(error = ?e, "error draining exit");
            }
        }
    }
}
