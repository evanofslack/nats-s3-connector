use async_nats::{jetstream, HeaderMap};
use std::{collections::HashSet, sync::Arc, time::Duration};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use testcontainers_modules::{
    nats::{Nats, NatsServerCmd},
    postgres::Postgres,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use nats3_types::Codec;

use crate::{
    db::{self, ChunkMetadataStorer, ListChunksQuery, PostgresStore},
    io, metrics, nats, s3,
};

const BUCKET: &str = "test-bucket";
const PREFIX: &str = "pfx";
const STREAM: &str = "EVENTS";
const SUBJECT: &str = "events.test";
const REPLAY_STREAM: &str = "REPLAY";
const REPLAY_SUBJECT: &str = "replay.test";
const MESSAGE_COUNT: u64 = 100;
const BATCH_SIZE: i64 = 10;

struct TestEnv {
    _postgres: ContainerAsync<Postgres>,
    _nats: ContainerAsync<Nats>,
    _minio: ContainerAsync<GenericImage>,
    io: io::IO,
    store: PostgresStore,
    jetstream: jetstream::Context,
}

async fn setup() -> TestEnv {
    let postgres = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("fail start postgres container");
    let nats = Nats::default()
        .with_cmd(&NatsServerCmd::default().with_jetstream())
        .start()
        .await
        .expect("fail start nats container");
    // minio no longer publishes images to docker hub or quay
    let minio = GenericImage::new("cgr.dev/chainguard/minio", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_either_std("API: http"))
        .with_cmd(["server", "/tmp/data"])
        .start()
        .await
        .expect("fail start minio container");

    let pg_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let nats_port = nats.get_host_port_ipv4(4222).await.unwrap();
    let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();

    let store = PostgresStore::new(&format!(
        "postgresql://postgres:postgres@localhost:{pg_port}/postgres"
    ))
    .await
    .expect("fail create store");
    store.migrate().await.expect("migration failed");

    let nats_url = format!("nats://localhost:{nats_port}");
    let metrics = metrics::Metrics::new();
    let s3_client = s3::Client::new(
        "us-east-1".to_string(),
        format!("http://localhost:{minio_port}"),
        "minioadmin".to_string(),
        "minioadmin".to_string(),
        metrics.clone(),
    );
    let nats_client = nats::Client::new(nats_url.clone(), metrics.clone())
        .await
        .expect("fail create nats client");
    let chunk_db: db::DynChunkStorer = Arc::new(store.clone());
    let io = io::IO::new(metrics, s3_client, nats_client, chunk_db);

    let jetstream = jetstream::new(async_nats::connect(nats_url).await.unwrap());

    TestEnv {
        _postgres: postgres,
        _nats: nats,
        _minio: minio,
        io,
        store,
        jetstream,
    }
}

fn chunks_query() -> ListChunksQuery {
    ListChunksQuery {
        stream: STREAM.to_string(),
        consumer: None,
        subject: SUBJECT.to_string(),
        bucket: BUCKET.to_string(),
        prefix: Some(PREFIX.to_string()),
        timestamp_start: None,
        timestamp_end: None,
        limit: None,
        include_deleted: false,
    }
}

async fn create_stream(
    js: &jetstream::Context,
    name: &str,
    subject: &str,
) -> jetstream::stream::Stream {
    js.create_stream(jetstream::stream::Config {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    })
    .await
    .expect("fail create stream")
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_store_and_load_roundtrip() {
    let env = setup().await;
    let js = &env.jetstream;

    create_stream(js, STREAM, SUBJECT).await;
    let mut replay = create_stream(js, REPLAY_STREAM, REPLAY_SUBJECT).await;

    let publish_start = chrono::Utc::now();
    let mut acks = Vec::new();
    for i in 1..=MESSAGE_COUNT {
        let mut headers = HeaderMap::new();
        headers.insert("X-Test-Index", i.to_string().as_str());
        let ack = js
            .publish_with_headers(SUBJECT, headers, format!("msg-{i}").into())
            .await
            .unwrap();
        acks.push(ack);
    }
    for ack in acks {
        ack.await.unwrap();
    }
    let publish_end = chrono::Utc::now();

    // consume well after publish so chunk timestamps must come from the server
    tokio::time::sleep(Duration::from_secs(2)).await;

    let consume_config = io::ConsumeConfig {
        stream: STREAM.to_string(),
        consumer: None,
        subject: SUBJECT.to_string(),
        bucket: BUCKET.to_string(),
        prefix: Some(PREFIX.to_string()),
        bytes_max: 100_000_000,
        messages_max: BATCH_SIZE,
        codec: Codec::Binary,
    };
    let cancel = CancellationToken::new();
    let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
    let consume = {
        let io = env.io.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            io.consume_stream(
                "store".to_string(),
                consume_config,
                cancel,
                CancellationToken::new(),
                exit_tx,
            )
            .await
        })
    };

    let expected_chunks = (MESSAGE_COUNT / BATCH_SIZE as u64) as usize;
    let chunks = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let chunks = env.store.list_chunks(chunks_query()).await.unwrap();
            if chunks.len() >= expected_chunks {
                return chunks;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timeout wait for chunks");
    cancel.cancel();
    consume.await.unwrap().unwrap();

    assert_eq!(chunks.len(), expected_chunks);
    let keys: HashSet<_> = chunks.iter().map(|c| c.key.clone()).collect();
    assert_eq!(keys.len(), expected_chunks);

    let slack = chrono::Duration::milliseconds(500);
    let mut sequences = Vec::new();
    for chunk_md in &chunks {
        assert!(!chunk_md.key.starts_with(PREFIX), "key {}", chunk_md.key);
        assert!(chunk_md.timestamp_start >= publish_start - slack);
        assert!(chunk_md.timestamp_end <= publish_end + slack);

        let chunk = env
            .io
            .s3_client
            .download_chunk(BUCKET, &chunk_md.object_path(), Codec::Binary)
            .await
            .expect("fail download chunk");
        sequences.extend(chunk.block.messages.iter().map(|m| m.sequence));
    }
    sequences.sort();
    assert_eq!(sequences, (1..=MESSAGE_COUNT).collect::<Vec<_>>());

    let publish_config = io::PublishConfig {
        read_stream: STREAM.to_string(),
        read_consumer: None,
        read_subject: SUBJECT.to_string(),
        write_subject: REPLAY_SUBJECT.to_string(),
        bucket: BUCKET.to_string(),
        prefix: Some(PREFIX.to_string()),
        poll_interval: None,
        delete_chunks: false,
        from_time: None,
        to_time: None,
    };
    let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
    env.io
        .publish_stream(
            "load".to_string(),
            publish_config,
            CancellationToken::new(),
            CancellationToken::new(),
            exit_tx,
        )
        .await
        .expect("fail publish stream");

    let info = replay.info().await.unwrap();
    assert_eq!(info.state.messages, MESSAGE_COUNT);

    for i in 1..=MESSAGE_COUNT {
        let message = replay.get_raw_message(i).await.unwrap();
        assert_eq!(message.payload, format!("msg-{i}").as_bytes());
        assert_eq!(
            message.headers.get("X-Test-Index").map(|v| v.as_str()),
            Some(i.to_string().as_str())
        );
    }
}
