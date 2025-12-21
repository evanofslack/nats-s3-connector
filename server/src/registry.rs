use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio::{sync::RwLock, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("job already running: {job_id}")]
    JobAlreadyRunning { job_id: String },
}

#[derive(Debug, Clone)]
pub enum TaskExitReason {
    Completed(Result<(), String>),
    Paused,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct TaskExitInfo {
    pub reason: TaskExitReason,
    pub job_id: String,
}

#[derive(Debug)]
struct StoreJobHandle {
    handle: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
    pause_token: CancellationToken,
}

#[derive(Debug)]
struct LoadJobHandle {
    handle: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
    pause_token: CancellationToken,
}

#[derive(Clone, Debug)]
pub struct Registry {
    store_handles: Arc<RwLock<HashMap<String, StoreJobHandle>>>,
    load_handles: Arc<RwLock<HashMap<String, LoadJobHandle>>>,
    shutdown_token: CancellationToken,
    exit_tx: mpsc::UnboundedSender<TaskExitInfo>,
    exit_rx: Arc<Mutex<mpsc::UnboundedReceiver<TaskExitInfo>>>,
}

impl Registry {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        Self {
            store_handles: Arc::new(RwLock::new(HashMap::new())),
            load_handles: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token,
            exit_tx,
            exit_rx: Arc::new(Mutex::new(exit_rx)),
        }
    }

    pub fn create_cancel_token(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    pub fn create_pause_token(&self) -> CancellationToken {
        CancellationToken::new()
    }

    pub fn create_exit_channel(&self) -> mpsc::UnboundedSender<TaskExitInfo> {
        self.exit_tx.clone()
    }

    pub fn subscribe_to_exits(&self) -> Arc<Mutex<mpsc::UnboundedReceiver<TaskExitInfo>>> {
        self.exit_rx.clone()
    }

    pub async fn try_register_store_job(
        &self,
        job_id: String,
        handle: JoinHandle<Result<()>>,
        cancel_token: CancellationToken,
        pause_token: CancellationToken,
    ) -> bool {
        debug!(job_id = job_id, "try register store job handle");

        let mut tasks = self.store_handles.write().await;
        if tasks.contains_key(&job_id) {
            handle.abort();
            return false;
        }
        tasks.insert(
            job_id,
            StoreJobHandle {
                handle,
                cancel_token,
                pause_token,
            },
        );
        true
    }

    pub async fn try_register_load_job(
        &self,
        job_id: String,
        handle: JoinHandle<Result<()>>,
        cancel_token: CancellationToken,
        pause_token: CancellationToken,
    ) -> bool {
        debug!(job_id = job_id, "register load job handle");

        let mut handles = self.load_handles.write().await;
        if handles.contains_key(&job_id) {
            handle.abort();
            return false;
        }

        handles.insert(
            job_id,
            LoadJobHandle {
                handle,
                cancel_token,
                pause_token,
            },
        );
        true
    }

    pub async fn is_store_job_running(&self, job_id: &str) -> bool {
        let handles = self.store_handles.read().await;
        handles.contains_key(job_id)
    }

    pub async fn is_load_job_running(&self, job_id: &str) -> bool {
        let handles = self.load_handles.read().await;
        handles.contains_key(job_id)
    }

    pub async fn remove_job(&self, job_id: &str) {
        self.store_handles.write().await.remove(job_id);
        self.load_handles.write().await.remove(job_id);
    }

    pub async fn cancel_store_job(&self, job_id: &str) {
        let handles = self.store_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.cancel_token.cancel();
        }
    }

    pub async fn cancel_load_job(&self, job_id: &str) {
        let handles = self.load_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.cancel_token.cancel();
        }
    }

    pub async fn pause_store_job(&self, job_id: &str) {
        let handles = self.store_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.pause_token.cancel();
        }
    }

    pub async fn pause_load_job(&self, job_id: &str) {
        let handles = self.load_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.pause_token.cancel();
        }
    }

    pub async fn wait_for_all_jobs(&self) -> Result<()> {
        debug!("wait for all in progress jobs to complete");

        let store_handles: Vec<_> = {
            let mut handles = self.store_handles.write().await;
            handles.drain().collect()
        };

        let load_handles: Vec<_> = {
            let mut handles = self.load_handles.write().await;
            handles.drain().collect()
        };

        for (job_id, job_handle) in store_handles {
            trace!(job_id = job_id, "wait for store job to complete");
            let _ = job_handle.handle.await;
        }

        for (job_id, job_handle) in load_handles {
            trace!(job_id = job_id, "wait for load job to complete");
            let _ = job_handle.handle.await;
        }

        debug!("all tasks completed");
        Ok(())
    }
}
