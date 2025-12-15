use crate::db::{postgres::PostgresStore, LoadJobStorer, StoreJobStorer};
use nats3_types::{
    Batch, Codec, Encoding, ListLoadJobsQuery, ListStoreJobsQuery, LoadJob, LoadJobStatus,
    StoreJob, StoreJobStatus,
};
use std::time;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

struct TestContext {
    _container: testcontainers::ContainerAsync<Postgres>,
    store: PostgresStore,
}

async fn setup_postgres() -> TestContext {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("fail start postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("fail get port");

    let database_url = format!("postgresql://postgres:postgres@localhost:{}/postgres", port);

    let store = PostgresStore::new(&database_url)
        .await
        .expect("fail create store");

    store.migrate().await.expect("migration failed");

    TestContext {
        _container: container,
        store,
    }
}

fn load_job_builder() -> LoadJobBuilder {
    LoadJobBuilder::default()
}

struct LoadJobBuilder {
    id: String,
    status: LoadJobStatus,
    bucket: String,
    prefix: Option<String>,
    read_stream: String,
    read_consumer: Option<String>,
    read_subject: String,
    write_subject: String,
    poll_interval: Option<time::Duration>,
    delete_chunks: bool,
    start_pos: Option<usize>,
    end_pos: Option<usize>,
}

impl Default for LoadJobBuilder {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            status: LoadJobStatus::Created,
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            read_stream: "read-stream".to_string(),
            read_consumer: None,
            read_subject: "read.subject".to_string(),
            write_subject: "write.subject".to_string(),
            poll_interval: None,
            delete_chunks: false,
            start_pos: Some(0),
            end_pos: Some(1000),
        }
    }
}

impl LoadJobBuilder {
    fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    fn status(mut self, status: LoadJobStatus) -> Self {
        self.status = status;
        self
    }

    fn build(self) -> LoadJob {
        LoadJob {
            id: self.id,
            status: self.status,
            bucket: self.bucket,
            prefix: self.prefix,
            read_stream: self.read_stream,
            read_consumer: self.read_consumer,
            read_subject: self.read_subject,
            write_subject: self.write_subject,
            poll_interval: self.poll_interval,
            delete_chunks: self.delete_chunks,
            start: self.start_pos,
            end: self.end_pos,
        }
    }
}

fn store_job_builder() -> StoreJobBuilder {
    StoreJobBuilder::default()
}

struct StoreJobBuilder {
    id: String,
    name: String,
    status: StoreJobStatus,
    stream: String,
    consumer: Option<String>,
    subject: String,
    bucket: String,
    prefix: Option<String>,
    batch_max_bytes: Option<i64>,
    batch_max_count: Option<i64>,
    encoding_codec: Option<Encoding>,
}

impl Default for StoreJobBuilder {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "test-store-job".to_string(),
            status: StoreJobStatus::Created,
            stream: "test-stream".to_string(),
            consumer: None,
            subject: "test.subject".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            batch_max_bytes: Some(1024000),
            batch_max_count: Some(100),
            encoding_codec: Some(Encoding { codec: Codec::Json }),
        }
    }
}

impl StoreJobBuilder {
    fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    fn status(mut self, status: StoreJobStatus) -> Self {
        self.status = status;
        self
    }

    fn build(self) -> StoreJob {
        StoreJob {
            id: self.id,
            name: self.name,
            status: self.status,
            stream: self.stream,
            consumer: self.consumer,
            subject: self.subject,
            bucket: self.bucket,
            prefix: self.prefix,
            batch: Batch {
                max_bytes: self.batch_max_bytes.expect("has default max bytes"),
                max_count: self.batch_max_count.expect("has default max count"),
            },
            encoding: self.encoding_codec.expect("has default codec"),
        }
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_create_and_get_load_job() {
    let ctx = setup_postgres().await;

    let job = load_job_builder()
        .id("test-job-1")
        .bucket("my-bucket")
        .build();

    ctx.store.create_load_job(job.clone()).await.unwrap();

    let retrieved = ctx
        .store
        .get_load_job("test-job-1".to_string())
        .await
        .unwrap();

    assert_eq!(retrieved.id, "test-job-1");
    assert_eq!(retrieved.bucket, "my-bucket");
    assert_eq!(retrieved.status, LoadJobStatus::Created);
    assert!(!retrieved.delete_chunks);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_update_load_job_status() {
    let ctx = setup_postgres().await;

    let job = load_job_builder().id("test-job-2").build();
    ctx.store.create_load_job(job).await.unwrap();

    let updated = ctx
        .store
        .update_load_job("test-job-2".to_string(), LoadJobStatus::Running)
        .await
        .unwrap();

    assert_eq!(updated.status, LoadJobStatus::Running);
    assert_eq!(updated.id, "test-job-2");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_load_jobs() {
    let ctx = setup_postgres().await;

    let job1 = load_job_builder().id("load-1").bucket("bucket-1").build();
    let job2 = load_job_builder().id("load-2").bucket("bucket-2").build();
    let job3 = load_job_builder().id("load-3").bucket("bucket-3").build();

    ctx.store.create_load_job(job1).await.unwrap();
    ctx.store.create_load_job(job2).await.unwrap();
    ctx.store.create_load_job(job3).await.unwrap();

    let jobs = ctx.store.get_load_jobs(None).await.unwrap();

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].id, "load-3");
    assert_eq!(jobs[1].id, "load-2");
    assert_eq!(jobs[2].id, "load-1");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_load_jobs_with_filters() {
    let ctx = setup_postgres().await;

    let job1 = load_job_builder()
        .id("load-filter-1")
        .bucket("bucket-x")
        .status(LoadJobStatus::Running)
        .build();
    let job2 = load_job_builder()
        .id("load-filter-2")
        .bucket("bucket-y")
        .status(LoadJobStatus::Running)
        .build();
    let job3 = load_job_builder()
        .id("load-filter-3")
        .bucket("bucket-x")
        .status(LoadJobStatus::Success)
        .build();

    ctx.store.create_load_job(job1).await.unwrap();
    ctx.store.create_load_job(job2).await.unwrap();
    ctx.store.create_load_job(job3).await.unwrap();

    let query = ListLoadJobsQuery {
        bucket: Some("bucket-x".to_string()),
        ..Default::default()
    };

    let jobs = ctx.store.get_load_jobs(Some(query)).await.unwrap();

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|j| j.bucket == "bucket-x"));

    let query2 = ListLoadJobsQuery::new().with_statuses(vec![LoadJobStatus::Running]);

    let jobs2 = ctx.store.get_load_jobs(Some(query2)).await.unwrap();

    assert_eq!(jobs2.len(), 2);
    assert!(jobs2.iter().all(|j| j.status == LoadJobStatus::Running));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_load_jobs_with_limit() {
    let ctx = setup_postgres().await;

    for i in 1..=5 {
        let job = load_job_builder().id(format!("load-limit-{}", i)).build();
        ctx.store.create_load_job(job).await.unwrap();
    }

    let query = ListLoadJobsQuery {
        limit: Some(2),
        ..Default::default()
    };

    let jobs = ctx.store.get_load_jobs(Some(query)).await.unwrap();

    assert_eq!(jobs.len(), 2);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_load_jobs_no_filter() {
    let ctx = setup_postgres().await;

    let job1 = load_job_builder().id("load-no-filter-1").build();
    let job2 = load_job_builder().id("load-no-filter-2").build();

    ctx.store.create_load_job(job1).await.unwrap();
    ctx.store.create_load_job(job2).await.unwrap();

    let jobs = ctx.store.get_load_jobs(None).await.unwrap();

    assert!(jobs.len() >= 2);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_delete_load_job() {
    let ctx = setup_postgres().await;

    let job = load_job_builder().id("load-delete").build();
    ctx.store.create_load_job(job).await.unwrap();

    ctx.store
        .delete_load_job("load-delete".to_string())
        .await
        .unwrap();

    let result = ctx.store.get_load_job("load-delete".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_delete_load_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.delete_load_job("nonexistent".to_string()).await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_create_and_get_store_job() {
    let ctx = setup_postgres().await;

    let job = store_job_builder()
        .id("store-1")
        .name("my-store-job")
        .bucket("store-bucket")
        .build();

    ctx.store.create_store_job(job.clone()).await.unwrap();

    let retrieved = ctx
        .store
        .get_store_job("store-1".to_string())
        .await
        .unwrap();

    assert_eq!(retrieved.id, "store-1");
    assert_eq!(retrieved.name, "my-store-job");
    assert_eq!(retrieved.bucket, "store-bucket");
    assert_eq!(retrieved.status, StoreJobStatus::Created);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_store_jobs() {
    let ctx = setup_postgres().await;

    let job1 = store_job_builder().id("store-1").name("job-1").build();
    let job2 = store_job_builder().id("store-2").name("job-2").build();
    let job3 = store_job_builder().id("store-3").name("job-3").build();

    ctx.store.create_store_job(job1).await.unwrap();
    ctx.store.create_store_job(job2).await.unwrap();
    ctx.store.create_store_job(job3).await.unwrap();

    let jobs = ctx.store.get_store_jobs(None).await.unwrap();

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].id, "store-3");
    assert_eq!(jobs[1].id, "store-2");
    assert_eq!(jobs[2].id, "store-1");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_store_jobs_with_filters() {
    let ctx = setup_postgres().await;

    let job1 = store_job_builder()
        .id("store-filter-1")
        .bucket("bucket-a")
        .status(StoreJobStatus::Running)
        .build();
    let job2 = store_job_builder()
        .id("store-filter-2")
        .bucket("bucket-b")
        .status(StoreJobStatus::Failure)
        .build();
    let job3 = store_job_builder()
        .id("store-filter-3")
        .bucket("bucket-a")
        .status(StoreJobStatus::Running)
        .build();

    ctx.store.create_store_job(job1).await.unwrap();
    ctx.store.create_store_job(job2).await.unwrap();
    ctx.store.create_store_job(job3).await.unwrap();

    let query = ListStoreJobsQuery {
        bucket: Some("bucket-a".to_string()),
        ..Default::default()
    };

    let jobs = ctx.store.get_store_jobs(Some(query)).await.unwrap();

    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|j| j.bucket == "bucket-a"));

    let query2 = ListStoreJobsQuery::new().with_statuses(vec![StoreJobStatus::Running]);

    let jobs2 = ctx.store.get_store_jobs(Some(query2)).await.unwrap();

    assert_eq!(jobs2.len(), 2);
    assert!(jobs2.iter().all(|j| j.status == StoreJobStatus::Running));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_store_jobs_with_limit() {
    let ctx = setup_postgres().await;

    for i in 1..=5 {
        let job = store_job_builder().id(format!("store-limit-{}", i)).build();
        ctx.store.create_store_job(job).await.unwrap();
    }

    let query = ListStoreJobsQuery {
        limit: Some(3),
        ..Default::default()
    };

    let jobs = ctx.store.get_store_jobs(Some(query)).await.unwrap();

    assert_eq!(jobs.len(), 3);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_store_jobs_no_filter() {
    let ctx = setup_postgres().await;

    let job1 = store_job_builder().id("store-no-filter-1").build();
    let job2 = store_job_builder().id("store-no-filter-2").build();

    ctx.store.create_store_job(job1).await.unwrap();
    ctx.store.create_store_job(job2).await.unwrap();

    let jobs = ctx.store.get_store_jobs(None).await.unwrap();

    assert!(jobs.len() >= 2);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_update_store_job_status() {
    let ctx = setup_postgres().await;

    let job = store_job_builder().id("store-update").build();
    ctx.store.create_store_job(job).await.unwrap();

    let updated = ctx
        .store
        .update_store_job("store-update".to_string(), StoreJobStatus::Running)
        .await
        .unwrap();

    assert_eq!(updated.status, StoreJobStatus::Running);
    assert_eq!(updated.id, "store-update");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_delete_store_job() {
    let ctx = setup_postgres().await;

    let job = store_job_builder().id("store-delete").build();
    ctx.store.create_store_job(job).await.unwrap();

    ctx.store
        .delete_store_job("store-delete".to_string())
        .await
        .unwrap();

    let result = ctx.store.get_store_job("store-delete".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_delete_store_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.delete_store_job("nonexistent".to_string()).await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_update_load_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx
        .store
        .update_load_job("nonexistent".to_string(), LoadJobStatus::Running)
        .await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_update_store_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx
        .store
        .update_store_job("nonexistent".to_string(), StoreJobStatus::Running)
        .await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_load_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.get_load_job("nonexistent".to_string()).await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_store_job_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.get_store_job("nonexistent".to_string()).await;
    assert!(
        matches!(result, Err(crate::db::JobStoreError::NotFound { id }) if id == "nonexistent")
    );
}
