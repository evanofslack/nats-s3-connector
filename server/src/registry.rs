use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{debug, warn};

use crate::io;

#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("job already running: {job_id}")]
    JobAlreadyRunning { job_id: String },
    #[error("job not found: {job_id}")]
    JobNotFound { job_id: String },
}

#[derive(Debug)]
struct StoreJobHandle {
    handle: JoinHandle<Result<()>>,
    started_at: chrono::DateTime<chrono::Utc>,
    config: io::ConsumeConfig,
}

#[derive(Debug)]
struct LoadJobHandle {
    handle: JoinHandle<Result<()>>,
    started_at: chrono::DateTime<chrono::Utc>,
    config: io::PublishConfig,
}

#[derive(Clone, Debug)]
pub struct Registry {
    store_handles: Arc<RwLock<HashMap<String, StoreJobHandle>>>,
    load_handles: Arc<RwLock<HashMap<String, LoadJobHandle>>>,
}

impl Registry {
    pub fn new() -> Self {
        debug!("creating new job registry");
        Self {
            store_handles: Arc::new(RwLock::new(HashMap::new())),
            load_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn try_register_store_job(
        &self,
        job_id: String,
        handle: JoinHandle<Result<()>>,
        config: io::ConsumeConfig,
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
                started_at: chrono::Utc::now(),
                config,
            },
        );
        true
    }

    pub async fn try_register_load_job(
        &self,
        job_id: String,
        handle: JoinHandle<Result<()>>,
        config: io::PublishConfig,
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
                started_at: chrono::Utc::now(),
                config,
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

    async fn cleanup_completed_store_job_handles(&self) -> Result<()> {
        let mut handles = self.store_handles.write().await;
        let mut to_remove = Vec::new();

        for (job_id, handle) in handles.iter() {
            if handle.handle.is_finished() {
                to_remove.push(job_id.clone());
            }
        }

        let mut removed_count = 0;
        for job_id in to_remove {
            if let Some(handle) = handles.remove(&job_id) {
                removed_count += 1;
                match handle.handle.await {
                    Ok(Ok(())) => {
                        debug!(job_id = job_id, "store job handle completed successfully");
                    }
                    Ok(Err(e)) => {
                        warn!(job_id = job_id, error = %e, "store job handle completed with error");
                    }
                    Err(e) => {
                        warn!(job_id = job_id, error = %e, "store job handle panicked");
                    }
                }
            }
        }
        if removed_count > 0 {
            debug!(
                count = removed_count,
                "cleaned up completed store job handles"
            );
        }
        Ok(())
    }

    async fn cleanup_completed_load_job_handles(&self) -> Result<()> {
        let mut handles = self.load_handles.write().await;
        let mut to_remove = Vec::new();

        for (job_id, handle) in handles.iter() {
            if handle.handle.is_finished() {
                to_remove.push(job_id.clone());
            }
        }

        let mut removed_count = 0;
        for job_id in to_remove {
            if let Some(handle) = handles.remove(&job_id) {
                removed_count += 1;
                match handle.handle.await {
                    Ok(Ok(())) => {
                        debug!(job_id = job_id, "load job handle completed successfully");
                    }
                    Ok(Err(e)) => {
                        warn!(job_id = job_id, error = %e, "load job handle completed with error");
                    }
                    Err(e) => {
                        warn!(job_id = job_id, error = %e, "load job handle panicked");
                    }
                }
            }
        }
        if removed_count > 0 {
            debug!(
                count = removed_count,
                "cleaned up completed load job handles"
            );
        }
        Ok(())
    }

    pub async fn cleanup_completed_jobs(&self) -> Result<()> {
        self.cleanup_completed_store_job_handles().await?;
        self.cleanup_completed_load_job_handles().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_register_and_check_running() {
        let manager = Registry::new();
        let job_id = "test-job-1".to_string();

        let handle = tokio::spawn(async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        let config = io::ConsumeConfig {
            stream: "test".to_string(),
            consumer: None,
            subject: "test".to_string(),
            bucket: "test".to_string(),
            prefix: None,
            bytes_max: 1000,
            messages_max: 100,
            codec: nats3_types::Codec::Json,
        };

        manager
            .try_register_store_job(job_id.clone(), handle, config)
            .await;

        assert!(manager.is_store_job_running(&job_id).await);
        assert!(!manager.is_store_job_running("nonexistent").await);
    }

    #[tokio::test]
    async fn test_duplicate_registration_fails() {
        let manager = Registry::new();
        let job_id = "test-job-2".to_string();

        let handle1 = tokio::spawn(async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        let handle2 = tokio::spawn(async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        let config = io::ConsumeConfig {
            stream: "test".to_string(),
            consumer: None,
            subject: "test".to_string(),
            bucket: "test".to_string(),
            prefix: None,
            bytes_max: 1000,
            messages_max: 100,
            codec: nats3_types::Codec::Json,
        };

        manager
            .try_register_store_job(job_id.clone(), handle1, config.clone())
            .await;

        let result = manager
            .try_register_store_job(job_id.clone(), handle2, config)
            .await;

        assert!(!result);
    }

    #[tokio::test]
    async fn test_cleanup_removes_completed() {
        let manager = Registry::new();
        let job_id = "test-job-3".to_string();

        let handle = tokio::spawn(async { Ok(()) });

        let config = io::ConsumeConfig {
            stream: "test".to_string(),
            consumer: None,
            subject: "test".to_string(),
            bucket: "test".to_string(),
            prefix: None,
            bytes_max: 1000,
            messages_max: 100,
            codec: nats3_types::Codec::Json,
        };

        manager
            .try_register_store_job(job_id.clone(), handle, config)
            .await;

        sleep(Duration::from_millis(50)).await;

        assert!(manager.is_store_job_running(&job_id).await);

        manager.cleanup_completed_jobs().await.unwrap();

        assert!(!manager.is_store_job_running(&job_id).await);
    }
}
