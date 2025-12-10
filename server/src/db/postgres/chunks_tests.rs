use bytes::Bytes;
use chrono::{Duration, Utc};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

use crate::db::postgres::PostgresStore;
use crate::db::{ChunkMetadataError, ChunkMetadataStorer, CreateChunkMetadata, ListChunksQuery};
use nats3_types::Codec;

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

fn chunk_builder() -> ChunkMetadataBuilder {
    ChunkMetadataBuilder::default()
}

struct ChunkMetadataBuilder {
    bucket: String,
    prefix: Option<String>,
    key: String,
    stream: String,
    consumer: Option<String>,
    subject: String,
    timestamp_start: chrono::DateTime<chrono::Utc>,
    timestamp_end: chrono::DateTime<chrono::Utc>,
    message_count: i64,
    size_bytes: i64,
    codec: Codec,
    hash: Bytes,
    version: String,
}

impl Default for ChunkMetadataBuilder {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            key: format!("chunk-{}.dat", uuid::Uuid::new_v4()),
            stream: "test-stream".to_string(),
            consumer: None,
            subject: "test.subject".to_string(),
            timestamp_start: now,
            timestamp_end: now + Duration::minutes(5),
            message_count: 100,
            size_bytes: 1024,
            codec: Codec::Json,
            hash: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
            version: "1.0.0".to_string(),
        }
    }
}

impl ChunkMetadataBuilder {
    fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    fn key(mut self, key: impl Into<String>) -> Self {
        self.key = key.into();
        self
    }

    fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = stream.into();
        self
    }

    fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = Some(consumer.into());
        self
    }

    fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = subject.into();
        self
    }

    fn timestamp_start(mut self, ts: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp_start = ts;
        self
    }

    fn timestamp_end(mut self, ts: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp_end = ts;
        self
    }

    fn message_count(mut self, count: i64) -> Self {
        self.message_count = count;
        self
    }

    fn size_bytes(mut self, size: i64) -> Self {
        self.size_bytes = size;
        self
    }

    fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    fn build(self) -> CreateChunkMetadata {
        CreateChunkMetadata {
            bucket: self.bucket,
            prefix: self.prefix,
            key: self.key,
            stream: self.stream,
            consumer: self.consumer,
            subject: self.subject,
            timestamp_start: self.timestamp_start,
            timestamp_end: self.timestamp_end,
            message_count: self.message_count,
            size_bytes: self.size_bytes,
            codec: self.codec,
            hash: self.hash,
            version: self.version,
        }
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_create_and_get_chunk() {
    let ctx = setup_postgres().await;

    let chunk = chunk_builder()
        .bucket("my-bucket")
        .key("my-chunk.dat")
        .build();

    let created = ctx.store.create_chunk(chunk).await.unwrap();

    assert!(created.sequence_number > 0);
    assert_eq!(created.bucket, "my-bucket");
    assert_eq!(created.key, "my-chunk.dat");
    assert_eq!(created.message_count, 100);
    assert!(created.deleted_at.is_none());

    let retrieved = ctx.store.get_chunk(created.sequence_number).await.unwrap();

    assert_eq!(retrieved.sequence_number, created.sequence_number);
    assert_eq!(retrieved.bucket, created.bucket);
    assert_eq!(retrieved.key, created.key);
    assert_eq!(retrieved.stream, created.stream);
    assert_eq!(retrieved.subject, created.subject);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_get_chunk_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.get_chunk(99999).await;
    assert!(matches!(
        result,
        Err(ChunkMetadataError::NotFound { sequence_number }) if sequence_number == 99999
    ));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_duplicate_chunk_location() {
    let ctx = setup_postgres().await;

    let chunk1 = chunk_builder()
        .bucket("bucket-1")
        .prefix("prefix-1")
        .key("duplicate.dat")
        .build();

    ctx.store.create_chunk(chunk1).await.unwrap();

    let chunk2 = chunk_builder()
        .bucket("bucket-1")
        .prefix("prefix-1")
        .key("duplicate.dat")
        .build();

    let result = ctx.store.create_chunk(chunk2).await;
    assert!(matches!(
        result,
        Err(ChunkMetadataError::Duplicate { bucket, key })
        if bucket == "bucket-1" && key == "duplicate.dat"
    ));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_basic() {
    let ctx = setup_postgres().await;

    let chunk1 = chunk_builder()
        .stream("stream-1")
        .subject("subject.a")
        .bucket("bucket-1")
        .prefix("prefix-1")
        .key("chunk-1.dat")
        .build();

    let chunk2 = chunk_builder()
        .stream("stream-1")
        .subject("subject.a")
        .bucket("bucket-1")
        .prefix("prefix-1")
        .key("chunk-2.dat")
        .build();

    let chunk3 = chunk_builder()
        .stream("stream-2")
        .subject("subject.b")
        .bucket("bucket-1")
        .prefix("prefix-1")
        .key("chunk-3.dat")
        .build();

    ctx.store.create_chunk(chunk1).await.unwrap();
    ctx.store.create_chunk(chunk2).await.unwrap();
    ctx.store.create_chunk(chunk3).await.unwrap();

    let query = ListChunksQuery {
        stream: "stream-1".to_string(),
        consumer: None,
        subject: "subject.a".to_string(),
        bucket: "bucket-1".to_string(),
        prefix: Some("prefix-1".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].stream, "stream-1");
    assert_eq!(chunks[1].stream, "stream-1");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_with_time_range() {
    let ctx = setup_postgres().await;

    let base_time = Utc::now();

    let chunk1 = chunk_builder()
        .stream("stream-1")
        .subject("subject.a")
        .timestamp_start(base_time)
        .timestamp_end(base_time + Duration::minutes(5))
        .key("chunk-1.dat")
        .build();

    let chunk2 = chunk_builder()
        .stream("stream-1")
        .subject("subject.a")
        .timestamp_start(base_time + Duration::hours(1))
        .timestamp_end(base_time + Duration::hours(1) + Duration::minutes(5))
        .key("chunk-2.dat")
        .build();

    let chunk3 = chunk_builder()
        .stream("stream-1")
        .subject("subject.a")
        .timestamp_start(base_time + Duration::hours(2))
        .timestamp_end(base_time + Duration::hours(2) + Duration::minutes(5))
        .key("chunk-3.dat")
        .build();

    ctx.store.create_chunk(chunk1).await.unwrap();
    ctx.store.create_chunk(chunk2).await.unwrap();
    ctx.store.create_chunk(chunk3).await.unwrap();

    let query = ListChunksQuery {
        stream: "stream-1".to_string(),
        consumer: None,
        subject: "subject.a".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix".to_string()),
        timestamp_start: Some(base_time + Duration::minutes(30)),
        timestamp_end: Some(base_time + Duration::hours(1) + Duration::minutes(30)),
        limit: None,
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].key, "chunk-2.dat");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_ordering() {
    let ctx = setup_postgres().await;

    let base_time = Utc::now();

    let chunk1 = chunk_builder()
        .timestamp_start(base_time + Duration::hours(2))
        .timestamp_end(base_time + Duration::hours(2) + Duration::minutes(10))
        .key("chunk-late.dat")
        .build();

    let chunk2 = chunk_builder()
        .timestamp_start(base_time)
        .timestamp_end(base_time + Duration::minutes(5))
        .key("chunk-early.dat")
        .build();

    let chunk3 = chunk_builder()
        .timestamp_start(base_time + Duration::hours(1))
        .timestamp_end(base_time + Duration::hours(1) + Duration::minutes(5))
        .key("chunk-middle.dat")
        .build();

    ctx.store.create_chunk(chunk1).await.unwrap();
    ctx.store.create_chunk(chunk2).await.unwrap();
    ctx.store.create_chunk(chunk3).await.unwrap();

    let query = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].key, "chunk-early.dat");
    assert_eq!(chunks[1].key, "chunk-middle.dat");
    assert_eq!(chunks[2].key, "chunk-late.dat");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_with_limit() {
    let ctx = setup_postgres().await;

    for i in 0..5 {
        let chunk = chunk_builder().key(format!("chunk-{}.dat", i)).build();
        ctx.store.create_chunk(chunk).await.unwrap();
    }

    let query = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: Some(3),
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 3);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_empty() {
    let ctx = setup_postgres().await;

    let query = ListChunksQuery {
        stream: "nonexistent-stream".to_string(),
        consumer: None,
        subject: "nonexistent.subject".to_string(),
        bucket: "nonexistent-bucket".to_string(),
        prefix: Some("nonexistent-prefix".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 0);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_soft_delete_chunk() {
    let ctx = setup_postgres().await;

    let chunk = chunk_builder().key("to-delete.dat").build();

    let created = ctx.store.create_chunk(chunk).await.unwrap();
    assert!(created.deleted_at.is_none());

    let deleted = ctx
        .store
        .soft_delete_chunk(created.sequence_number)
        .await
        .unwrap();

    assert_eq!(deleted.sequence_number, created.sequence_number);
    assert!(deleted.deleted_at.is_some());

    let retrieved = ctx.store.get_chunk(created.sequence_number).await.unwrap();
    assert!(retrieved.deleted_at.is_some());
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_soft_delete_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.soft_delete_chunk(99999).await;
    assert!(matches!(
        result,
        Err(ChunkMetadataError::NotFound { sequence_number }) if sequence_number == 99999
    ));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_exclude_deleted() {
    let ctx = setup_postgres().await;

    let chunk1 = chunk_builder().key("active.dat").build();
    let chunk2 = chunk_builder().key("deleted.dat").build();

    let created1 = ctx.store.create_chunk(chunk1).await.unwrap();
    let created2 = ctx.store.create_chunk(chunk2).await.unwrap();

    ctx.store
        .soft_delete_chunk(created2.sequence_number)
        .await
        .unwrap();

    let query = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].sequence_number, created1.sequence_number);
    assert_eq!(chunks[0].key, "active.dat");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_list_chunks_include_deleted() {
    let ctx = setup_postgres().await;

    let chunk1 = chunk_builder().key("active.dat").build();
    let chunk2 = chunk_builder().key("deleted.dat").build();

    let created1 = ctx.store.create_chunk(chunk1).await.unwrap();
    let created2 = ctx.store.create_chunk(chunk2).await.unwrap();

    ctx.store
        .soft_delete_chunk(created2.sequence_number)
        .await
        .unwrap();

    let query = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: true,
    };

    let chunks = ctx.store.list_chunks(query).await.unwrap();

    assert_eq!(chunks.len(), 2);
    assert!(chunks
        .iter()
        .any(|c| c.sequence_number == created1.sequence_number));
    assert!(chunks
        .iter()
        .any(|c| c.sequence_number == created2.sequence_number));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_hard_delete_chunk() {
    let ctx = setup_postgres().await;

    let chunk = chunk_builder().key("to-hard-delete.dat").build();

    let created = ctx.store.create_chunk(chunk).await.unwrap();

    ctx.store
        .hard_delete_chunk(created.sequence_number)
        .await
        .unwrap();

    let result = ctx.store.get_chunk(created.sequence_number).await;
    assert!(matches!(
        result,
        Err(ChunkMetadataError::NotFound { sequence_number }) if sequence_number == created.sequence_number
    ));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_hard_delete_not_found() {
    let ctx = setup_postgres().await;

    let result = ctx.store.hard_delete_chunk(99999).await;
    assert!(matches!(
        result,
        Err(ChunkMetadataError::NotFound { sequence_number }) if sequence_number == 99999
    ));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_chunk_codec_binary() {
    let ctx = setup_postgres().await;

    let chunk = chunk_builder()
        .codec(Codec::Binary)
        .key("binary-chunk.dat")
        .build();

    let created = ctx.store.create_chunk(chunk).await.unwrap();

    assert_eq!(created.codec, Codec::Binary);

    let retrieved = ctx.store.get_chunk(created.sequence_number).await.unwrap();
    assert_eq!(retrieved.codec, Codec::Binary);
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_chunk_different_prefixes() {
    let ctx = setup_postgres().await;

    let chunk1 = chunk_builder()
        .prefix("prefix-a")
        .key("same-key.dat")
        .build();

    let chunk2 = chunk_builder()
        .prefix("prefix-b")
        .key("same-key.dat")
        .build();

    ctx.store.create_chunk(chunk1).await.unwrap();
    ctx.store.create_chunk(chunk2).await.unwrap();

    let query1 = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("prefix-a".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks1 = ctx.store.list_chunks(query1).await.unwrap();
    assert_eq!(chunks1.len(), 1);
    assert_eq!(chunks1[0].prefix, Some("prefix-a".to_string()));

    let query2 = ListChunksQuery {
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("prefix-b".to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    };

    let chunks2 = ctx.store.list_chunks(query2).await.unwrap();
    assert_eq!(chunks2.len(), 1);
    assert_eq!(chunks2[0].prefix, Some("prefix-b".to_string()));
}
