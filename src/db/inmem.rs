use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::db;
use crate::jobs;
use crate::metrics;

use tracing::debug;

#[derive(Debug)]
pub struct InMemory {
    metrics: metrics::Metrics,
    db: RwLock<HashMap<String, jobs::LoadJob>>,
}

impl InMemory {
    pub fn new(metrics: metrics::Metrics) -> Self {
        debug!("creating new in-memory job store");
        return InMemory {
            metrics,
            db: RwLock::new(HashMap::new()),
        };
    }
}

#[async_trait]
impl db::LoadJobStorer for InMemory {
    async fn get_load_job(&self, id: String) -> Result<jobs::LoadJob, db::JobStoreError> {
        debug!(id = id, "getting load job");
        let found_job: jobs::LoadJob;
        if let Some(job) = self.db.read().expect("lock not poisoned").get(&id) {
            found_job = job.clone();
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
        return Ok(found_job);
    }

    async fn get_load_jobs(&self) -> Result<Vec<jobs::LoadJob>, db::JobStoreError> {
        debug!("getting load jobs");
        let jobs = self
            .db
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect();
        return Ok(jobs);
    }

    async fn create_load_job(&self, job: jobs::LoadJob) -> Result<(), db::JobStoreError> {
        debug!(
            id = job.id,
            stream = job.write_stream,
            subject = job.write_subject,
            bucket = job.bucket,
            "creating load job"
        );
        self.db
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
        status: jobs::LoadJobStatus,
    ) -> Result<jobs::LoadJob, db::JobStoreError> {
        debug!(id = id, status = status.to_string(), "updating load job");

        // lookup existing job
        let mut job = self.get_load_job(id).await?;
        job.status = status.clone();

        // insert updated job
        self.db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());

        // decrement gauge if job status is terminal
        let label_job = job.clone();
        match status {
            jobs::LoadJobStatus::Success | jobs::LoadJobStatus::Failure => {
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
        if let Some(_) = self.db.write().expect("lock not poisoned").remove(&id) {
            return Ok(());
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
    }
}
