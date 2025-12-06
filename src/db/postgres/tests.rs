use crate::db::postgres::PostgresStore;
use crate::db::{LoadJobStorer, StoreJobStorer};
use crate::jobs::{LoadJob, LoadJobStatus};
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
    println!("Migration completed successfully");

    let store_other = store.clone();
    let client = store_other.get_client().await.unwrap();
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'load_jobs')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    println!("Table exists: {}", exists);

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
    read_subject: String,
    write_stream: String,
    write_subject: String,
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
            read_subject: "read.subject".to_string(),
            write_stream: "write-stream".to_string(),
            write_subject: "write.subject".to_string(),
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

    fn status(mut self, status: LoadJobStatus) -> Self {
        self.status = status;
        self
    }

    fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    fn delete_chunks(mut self, delete: bool) -> Self {
        self.delete_chunks = delete;
        self
    }

    fn build(self) -> LoadJob {
        LoadJob {
            id: self.id,
            status: self.status,
            bucket: self.bucket,
            prefix: self.prefix,
            read_stream: self.read_stream,
            read_subject: self.read_subject,
            write_stream: self.write_stream,
            write_subject: self.write_subject,
            delete_chunks: self.delete_chunks,
            start: self.start_pos,
            end: self.end_pos,
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
    assert_eq!(retrieved.delete_chunks, false);
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
