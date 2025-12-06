use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use nats3_types::{LoadJob, LoadJobStatus, StoreJob, StoreJobStatus};

use crate::db;
use crate::metrics;

use tracing::debug;

#[derive(Debug)]
pub struct InMemory {
    metrics: metrics::Metrics,
    store_db: RwLock<HashMap<String, StoreJob>>,
    load_db: RwLock<HashMap<String, LoadJob>>,
}

impl InMemory {
    pub fn new(metrics: metrics::Metrics) -> Self {
        debug!("creating new in-memory job store");
        InMemory {
            metrics,
            store_db: RwLock::new(HashMap::new()),
            load_db: RwLock::new(HashMap::new()),
        }
    }
}

// JobStorer is a supertrait comprised of
// StoreJobStorer and LoadJobStorer which
// are both implemented below.
impl db::JobStorer for InMemory {}

#[async_trait]
impl db::StoreJobStorer for InMemory {
    async fn get_store_job(&self, id: String) -> Result<StoreJob, db::JobStoreError> {
        debug!(id = id, "getting store job");
        let found_job: StoreJob;
        if let Some(job) = self.store_db.read().expect("lock not poisoned").get(&id) {
            found_job = job.clone();
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
        return Ok(found_job);
    }

    async fn get_store_jobs(&self) -> Result<Vec<StoreJob>, db::JobStoreError> {
        debug!("getting store jobs");
        let jobs = self
            .store_db
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect();
        return Ok(jobs);
    }

    async fn create_store_job(&self, job: StoreJob) -> Result<(), db::JobStoreError> {
        debug!(
            id = job.id,
            stream = job.stream,
            subject = job.subject,
            bucket = job.bucket,
            "creating store job"
        );
        self.store_db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());

        self.metrics
            .jobs
            .write()
            .await
            .store_jobs
            .get_or_create(&metrics::JobLabels {
                stream: job.stream,
                subject: job.subject,
                bucket: job.bucket,
            })
            .inc();
        return Ok(());
    }

    async fn update_store_job(
        &self,
        id: String,
        status: StoreJobStatus,
    ) -> Result<StoreJob, db::JobStoreError> {
        debug!(id = id, status = status.to_string(), "updating store job");

        // lookup existing job
        let mut job = self.get_store_job(id).await?;
        job.status = status.clone();

        // insert updated job
        self.store_db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());

        // decrement gauge if job status is terminal
        let label_job = job.clone();
        if let StoreJobStatus::Failure = status {
            self.metrics
                .jobs
                .write()
                .await
                .store_jobs
                .get_or_create(&metrics::JobLabels {
                    stream: label_job.stream,
                    subject: label_job.subject,
                    bucket: label_job.bucket,
                })
                .dec();
        }
        return Ok(job);
    }

    async fn delete_store_job(&self, id: String) -> Result<(), db::JobStoreError> {
        debug!(id = id, "deleting store job");
        if self
            .store_db
            .write()
            .expect("lock not poisoned")
            .remove(&id)
            .is_some()
        {
            return Ok(());
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
    }
}

#[async_trait]
impl db::LoadJobStorer for InMemory {
    async fn get_load_job(&self, id: String) -> Result<LoadJob, db::JobStoreError> {
        debug!(id = id, "getting load job");
        let found_job: LoadJob;
        if let Some(job) = self.load_db.read().expect("lock not poisoned").get(&id) {
            found_job = job.clone();
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
        return Ok(found_job);
    }

    async fn get_load_jobs(&self) -> Result<Vec<LoadJob>, db::JobStoreError> {
        debug!("getting load jobs");
        let jobs = self
            .load_db
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect();
        return Ok(jobs);
    }

    async fn create_load_job(&self, job: LoadJob) -> Result<(), db::JobStoreError> {
        debug!(
            id = job.id,
            stream = job.write_stream,
            subject = job.write_subject,
            bucket = job.bucket,
            "creating load job"
        );
        self.load_db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());

        self.metrics
            .jobs
            .write()
            .await
            .load_jobs
            .get_or_create(&metrics::JobLabels {
                stream: job.write_stream,
                subject: job.write_subject,
                bucket: job.bucket,
            })
            .inc();
        return Ok(());
    }

    async fn update_load_job(
        &self,
        id: String,
        status: LoadJobStatus,
    ) -> Result<LoadJob, db::JobStoreError> {
        debug!(id = id, status = status.to_string(), "updating load job");

        // lookup existing job
        let mut job = self.get_load_job(id).await?;
        job.status = status.clone();

        // insert updated job
        self.load_db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());

        // decrement gauge if job status is terminal
        let label_job = job.clone();
        match status {
            LoadJobStatus::Success | LoadJobStatus::Failure => {
                self.metrics
                    .jobs
                    .write()
                    .await
                    .load_jobs
                    .get_or_create(&metrics::JobLabels {
                        stream: label_job.write_stream,
                        subject: label_job.write_subject,
                        bucket: label_job.bucket,
                    })
                    .dec();
            }
            _ => {}
        }
        return Ok(job);
    }

    async fn delete_load_job(&self, id: String) -> Result<(), db::JobStoreError> {
        debug!(id = id, "deleting load job");
        if self
            .load_db
            .write()
            .expect("lock not poisoned")
            .remove(&id)
            .is_some()
        {
            return Ok(());
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
    }
}
