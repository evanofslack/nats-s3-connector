use anyhow::Result;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::{sync::RwLock, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

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
    cancel_token: CancellationToken,
    started_at: chrono::DateTime<chrono::Utc>,
    config: io::ConsumeConfig,
}

#[derive(Debug)]
struct LoadJobHandle {
    handle: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
    started_at: chrono::DateTime<chrono::Utc>,
    config: io::PublishConfig,
}

#[derive(Debug, Clone)]
pub enum JobResult {
    Success,
    Failed(String),   // io error
    Panicked(String), // join error
}

#[async_trait]
pub trait LoadJobCompletionHandler: Send + Sync + std::fmt::Debug {
    async fn handle_job_completion(&self, job_id: &str, result: JobResult) -> Result<()>;
}

#[async_trait]
pub trait StoreJobCompletionHandler: Send + Sync + std::fmt::Debug {
    async fn handle_job_completion(&self, job_id: &str, result: JobResult) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct Registry {
    store_handles: Arc<RwLock<HashMap<String, StoreJobHandle>>>,
    load_handles: Arc<RwLock<HashMap<String, LoadJobHandle>>>,
    shutdown_token: CancellationToken,
}

impl Registry {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            store_handles: Arc::new(RwLock::new(HashMap::new())),
            load_handles: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token,
        }
    }

    pub fn create_cancel_token(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    pub async fn try_register_store_job(
        &self,
        job_id: String,
        handle: JoinHandle<Result<()>>,
        cancel_token: CancellationToken,
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
                cancel_token,
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
        cancel_token: CancellationToken,
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
                cancel_token,
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

    async fn cleanup_completed_store_job_handles(
        &self,
        handler: Arc<dyn StoreJobCompletionHandler>,
    ) -> Result<()> {
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
                let result = match handle.handle.await {
                    Ok(Ok(())) => JobResult::Success,
                    Ok(Err(e)) => JobResult::Failed(e.to_string()),
                    Err(e) => JobResult::Panicked(e.to_string()),
                };
                handler.handle_job_completion(&job_id, result).await?;
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

    async fn cleanup_completed_load_job_handles(
        &self,
        handler: Arc<dyn LoadJobCompletionHandler>,
    ) -> Result<()> {
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
                let result = match handle.handle.await {
                    Ok(Ok(())) => JobResult::Success,
                    Ok(Err(e)) => JobResult::Failed(e.to_string()),
                    Err(e) => JobResult::Panicked(e.to_string()),
                };
                handler.handle_job_completion(&job_id, result).await?;
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

    pub async fn cleanup_completed_jobs(
        &self,
        load_job_handler: Arc<dyn LoadJobCompletionHandler>,
        store_job_handler: Arc<dyn StoreJobCompletionHandler>,
    ) -> Result<()> {
        self.cleanup_completed_store_job_handles(store_job_handler)
            .await?;
        self.cleanup_completed_load_job_handles(load_job_handler)
            .await?;
        Ok(())
    }

    pub async fn cancel_store_job(&self, job_id: &str) -> Result<(), RegistryError> {
        let handles = self.store_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.cancel_token.cancel();
            Ok(())
        } else {
            Err(RegistryError::JobNotFound {
                job_id: job_id.to_string(),
            })
        }
    }

    pub async fn cancel_load_job(&self, job_id: &str) -> Result<(), RegistryError> {
        let handles = self.load_handles.read().await;
        if let Some(job_handle) = handles.get(job_id) {
            job_handle.cancel_token.cancel();
            Ok(())
        } else {
            Err(RegistryError::JobNotFound {
                job_id: job_id.to_string(),
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::{
        sync::Mutex,
        time::{sleep, Duration},
    };

    #[derive(Clone, Default, Debug)]
    struct MockJobCompletionHandler {
        calls: Arc<Mutex<Vec<(String, JobResult)>>>,
    }

    impl MockJobCompletionHandler {
        fn new() -> Self {
            Self::default()
        }

        async fn get_calls(&self) -> Vec<(String, JobResult)> {
            self.calls.lock().await.clone()
        }

        async fn assert_called_with(&self, job_id: &str, expected: JobResult) {
            let calls = self.calls.lock().await;
            assert!(
                calls.iter().any(|(id, result)| {
                    id == job_id
                        && matches!(
                            (result, &expected),
                            (JobResult::Success, JobResult::Success)
                                | (JobResult::Failed(_), JobResult::Failed(_))
                                | (JobResult::Panicked(_), JobResult::Panicked(_))
                        )
                }),
                "Expected call with job_id={} and result={:?} not found in {:?}",
                job_id,
                expected,
                calls
            );
        }
    }

    #[async_trait]
    impl LoadJobCompletionHandler for MockJobCompletionHandler {
        async fn handle_job_completion(&self, job_id: &str, result: JobResult) -> Result<()> {
            self.calls.lock().await.push((job_id.to_string(), result));
            Ok(())
        }
    }

    #[async_trait]
    impl StoreJobCompletionHandler for MockJobCompletionHandler {
        async fn handle_job_completion(&self, job_id: &str, result: JobResult) -> Result<()> {
            self.calls.lock().await.push((job_id.to_string(), result));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_register_and_check_running() {
        let cancel_token = CancellationToken::new();
        let registry = Registry::new(cancel_token.clone());
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

        registry
            .try_register_store_job(job_id.clone(), handle, cancel_token, config)
            .await;

        assert!(registry.is_store_job_running(&job_id).await);
        assert!(!registry.is_store_job_running("nonexistent").await);
    }

    #[tokio::test]
    async fn test_duplicate_registration_fails() {
        let cancel_token = CancellationToken::new();
        let registry = Registry::new(cancel_token.clone());
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

        registry
            .try_register_store_job(
                job_id.clone(),
                handle1,
                cancel_token.clone(),
                config.clone(),
            )
            .await;

        let result = registry
            .try_register_store_job(job_id.clone(), handle2, cancel_token, config)
            .await;

        assert!(!result);
    }

    #[tokio::test]
    async fn test_cleanup_calls_handler_on_success() {
        let cancel_token = CancellationToken::new();
        let registry = Registry::new(cancel_token.clone());
        let job_id = "test-job-success".to_string();

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

        registry
            .try_register_store_job(job_id.clone(), handle, cancel_token, config)
            .await;

        sleep(Duration::from_millis(50)).await;

        let handler = MockJobCompletionHandler::new();
        registry
            .cleanup_completed_jobs(Arc::new(handler.clone()), Arc::new(handler.clone()))
            .await
            .unwrap();

        assert!(!registry.is_store_job_running(&job_id).await);
        handler
            .assert_called_with(&job_id, JobResult::Success)
            .await;
    }

    #[tokio::test]
    async fn test_cleanup_calls_handler_on_failure() {
        let cancel_token = CancellationToken::new();
        let registry = Registry::new(cancel_token.clone());
        let job_id = "test-job-fail".to_string();

        let handle = tokio::spawn(async { Err(anyhow::anyhow!("test error")) });

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

        registry
            .try_register_store_job(job_id.clone(), handle, cancel_token, config)
            .await;

        sleep(Duration::from_millis(50)).await;

        let handler = MockJobCompletionHandler::new();
        registry
            .cleanup_completed_jobs(Arc::new(handler.clone()), Arc::new(handler.clone()))
            .await
            .unwrap();

        handler
            .assert_called_with(&job_id, JobResult::Failed(String::new()))
            .await;
    }

    #[tokio::test]
    async fn test_cleanup_calls_handler_on_panic() {
        let cancel_token = CancellationToken::new();
        let handler = MockJobCompletionHandler::new();
        let registry = Registry::new(cancel_token.clone());
        let job_id = "test-job-panic".to_string();

        let handle = tokio::spawn(async {
            panic!("intentional panic");
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

        registry
            .try_register_store_job(job_id.clone(), handle, cancel_token, config)
            .await;

        sleep(Duration::from_millis(50)).await;
        registry
            .cleanup_completed_jobs(Arc::new(handler.clone()), Arc::new(handler.clone()))
            .await
            .unwrap();

        handler
            .assert_called_with(&job_id, JobResult::Panicked(String::new()))
            .await;
    }
}
