use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::db;
use crate::jobs;

#[derive(Debug)]
pub struct InMemory {
    db: RwLock<HashMap<String, jobs::LoadJob>>,
}

impl InMemory {
    pub fn new() -> Self {
        return InMemory {
            db: RwLock::new(HashMap::new()),
        };
    }
}

#[async_trait]
impl db::JobStorer for InMemory {
    async fn get_load_job(&self, id: String) -> Result<jobs::LoadJob, db::JobStoreError> {
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
        let jobs = self
            .db
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect();
        return Ok(jobs);
    }

    async fn update_load_job(
        &self,
        id: String,
        status: jobs::LoadJobStatus,
    ) -> Result<jobs::LoadJob, db::JobStoreError> {
        let mut job = self.get_load_job(id).await?;
        job.status = status;
        self.create_load_job(job.clone()).await?;
        return Ok(job);
    }

    async fn create_load_job(&self, job: jobs::LoadJob) -> Result<(), db::JobStoreError> {
        self.db
            .write()
            .expect("lock not poisoned")
            .insert(job.id.clone(), job.clone());
        return Ok(());
    }

    async fn delete_load_job(&self, id: String) -> Result<(), db::JobStoreError> {
        if let Some(_) = self.db.write().expect("lock not poisoned").remove(&id) {
            return Ok(());
        } else {
            let err = db::JobStoreError::NotFound { id };
            return Err(err);
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[tokio::test]
//     async fn paste_too_large() {
//         // paste new text is 4 bytes while max_size is 3 bytes
//         let db = InMemory::new(3).await.unwrap();
//         let paste = models::Paste::new(String::from("key"), String::from("text"), None, true);
//         assert!(db.create(paste.clone()).await.is_err())
//     }
//
//     #[tokio::test]
//     async fn create_and_get() {
//         let db = InMemory::new(1024).await.unwrap();
//         let paste = models::Paste::new(String::from("key"), String::from("text"), None, true);
//         db.create(paste.clone())
//             .await
//             .expect("should create new paste");
//         let resp = db
//             .get(paste.clone().key)
//             .await
//             .expect("should return paste");
//
//         assert_eq!(paste, resp);
//     }
//
//     #[tokio::test]
//     async fn delete() {
//         let db = InMemory::new(1024).await.unwrap();
//         let paste = models::Paste::new(String::from("key"), String::from("text"), None, false);
//         db.create(paste.clone())
//             .await
//             .expect("should create new paste");
//
//         db.delete(&paste.clone().key)
//             .await
//             .expect("should delete paste");
//
//         let resp = db.get(paste.clone().key).await;
//         assert!(resp.is_err());
//         if let Err(msg) = resp {
//             assert_eq!(msg, "paste not found")
//         }
//     }
//
//     #[tokio::test]
//     async fn burn_on_read() {
//         let db = InMemory::new(1024).await.unwrap();
//         let paste = models::Paste::new(String::from("key"), String::from("text"), None, true);
//         db.create(paste.clone())
//             .await
//             .expect("should create new paste");
//         let resp = db
//             .get(paste.clone().key)
//             .await
//             .expect("should return paste");
//         assert_eq!(paste, resp);
//
//         let resp = db.get(paste.clone().key).await;
//         assert!(resp.is_err());
//         if let Err(msg) = resp {
//             assert_eq!(msg, "paste not found")
//         }
//     }
// }
