use anyhow::Result;
use async_nats::jetstream;
use futures::StreamExt;
use nats3_types::Codec;
use std::sync::Arc;
use tokio::{sync::RwLock, time};
use tracing::{debug, trace, warn};

use crate::db;
use crate::encoding;
use crate::metrics;
use crate::nats;
use crate::s3;

const KEEP_ALIVE_INTERVAL: time::Duration = time::Duration::from_secs(10);
const DEFAULT_BATCH_WAIT: time::Duration = time::Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ConsumeConfig {
    pub stream: String,
    pub consumer: Option<String>,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub bytes_max: i64,
    pub messages_max: i64,
    pub codec: Codec,
}

#[derive(Debug, Clone)]
pub struct PublishConfig {
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub write_subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub delete_chunks: bool,
    pub start: Option<usize>,
    pub end: Option<usize>,
}

// IO handles interfacing with NATs and S3
#[derive(Debug, Clone)]
pub struct IO {
    pub metrics: metrics::Metrics,
    pub s3_client: s3::Client,
    pub nats_client: nats::Client,
    pub chunk_db: db::DynChunkStorer,
}

impl IO {
    pub fn new(
        metrics: metrics::Metrics,
        s3_client: s3::Client,
        nats_client: nats::Client,
        chunk_db: db::DynChunkStorer,
    ) -> IO {
        debug!("creating new IO instance");

        IO {
            metrics,
            s3_client,
            nats_client,
            chunk_db,
        }
    }
    pub async fn consume_stream(&self, config: ConsumeConfig) -> Result<()> {
        debug!(
            stream = config.stream,
            subject = config.subject,
            bucket = config.bucket,
            prefix = config.prefix,
            codec = config.codec.to_string(),
            "start consume stream and upload to bucket"
        );

        let buffer = MessageBuffer::new();
        buffer.keep_alive(KEEP_ALIVE_INTERVAL);

        let mut bytes_total = 0;
        let mut buffer_start = std::time::Instant::now();
        let mut messages = self
            .nats_client
            .consume(
                config.stream.clone(),
                config.subject.clone(),
                config.messages_max,
            )
            .await?;
        let prefix = &config.prefix;

        let mut interval = tokio::time::interval(DEFAULT_BATCH_WAIT);
        interval.tick().await;

        loop {
            tokio::select! {
                maybe_message = messages.next() => {
                    match maybe_message {
                        Some(Ok(message)) => {
                            trace!(
                                subject = message.subject.to_string(),
                                "consumer got message"
                            );
                            bytes_total += &message.payload.len();
                            buffer.push(message).await;

                            let messages_total = buffer.len().await;

                            if messages_total >= config.messages_max as usize
                                || bytes_total >= config.bytes_max as usize
                            {
                                self.upload_buffer(&buffer, &mut bytes_total, &mut buffer_start, &config, prefix).await?;
                            }
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    let messages_total = buffer.len().await;
                    if messages_total > 0 {
                        debug!(messages = messages_total, "timer triggered upload");
                        self.upload_buffer(&buffer, &mut bytes_total, &mut buffer_start, &config, prefix).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn upload_buffer(
        &self,
        buffer: &MessageBuffer,
        bytes_total: &mut usize,
        buffer_start: &mut std::time::Instant,
        config: &ConsumeConfig,
        prefix: &Option<String>,
    ) -> Result<()> {
        let messages_total = buffer.len().await;
        let elapsed = buffer_start.elapsed();

        debug!(
            messages = messages_total,
            bytes = *bytes_total,
            elapsed_secs = elapsed.as_secs(),
            "buffer threshold reached"
        );

        let block = encoding::MessageBlock::from(buffer.to_vec().await);
        let chunk = encoding::Chunk::from(block);
        let key = chunk.key(config.codec.clone()).to_string();

        let stream = config.stream.clone();
        let subject = config.subject.clone();
        let key = format!("{stream}/{subject}/{key}");

        let path = if let Some(prefix) = prefix {
            format!("{}/{}", prefix, key)
        } else {
            key
        };

        let serialized = chunk.serialize(config.codec.clone())?;
        let serialized_size = serialized.len();
        self.s3_client
            .upload_chunk(serialized, &config.bucket, &path, config.codec.clone())
            .await?;

        let chunk_md = chunk.to_chunk_metadata(config, &path, serialized_size);
        self.chunk_db.create_chunk(chunk_md).await?;

        let nats_metrics = self.metrics.nats.write().await;
        let labels = metrics::NatsLabels {
            subject: subject.clone(),
            stream: stream.clone(),
        };
        nats_metrics
            .store_messages
            .get_or_create(&labels)
            .inc_by(messages_total as u64);
        nats_metrics
            .store_bytes
            .get_or_create(&labels)
            .inc_by(*bytes_total as u64);
        drop(nats_metrics);

        buffer.ack_all().await;
        buffer.clear().await;
        *bytes_total = 0;
        *buffer_start = std::time::Instant::now();

        Ok(())
    }

    pub async fn publish_stream(&self, config: PublishConfig) -> Result<()> {
        trace!(
            read_stream = config.read_stream,
            read_consumer = config.read_consumer,
            read_subject = config.read_subject,
            write_subject = config.write_subject,
            bucket = config.bucket,
            prefix = config.prefix,
            delete_chunks = config.delete_chunks,
            start = config.start,
            end = config.end,
            "start download from bucket and publish to stream"
        );

        let write_subject = config.write_subject.clone();
        let read_stream = config.read_stream.clone();
        let read_consumer = config.read_consumer.clone();
        let read_subject = config.read_subject.clone();

        let query = db::ListChunksQuery {
            stream: read_stream.clone(),
            consumer: read_consumer.clone(),
            subject: read_subject.clone(),
            bucket: config.bucket.clone(),
            prefix: config.prefix,
            timestamp_start: config.start.map(|ts| {
                chrono::DateTime::from_timestamp(ts.try_into().expect("usize to int64"), 0)
                    .expect("invalid start timestamp")
            }),
            timestamp_end: config.end.map(|ts| {
                chrono::DateTime::from_timestamp(ts.try_into().expect("usize to int64"), 0)
                    .expect("invalid end timestamp")
            }),
            limit: None,
            include_deleted: false,
        };

        let chunks = self.chunk_db.list_chunks(query).await?;

        for chunk_md in chunks {
            let path = if chunk_md.prefix.clone().is_some_and(|p| !p.is_empty()) {
                format!(
                    "{}/{}",
                    chunk_md.prefix.expect("prefix already checked as not none"),
                    chunk_md.key
                )
            } else {
                chunk_md.key
            };

            let chunk = match self
                .s3_client
                .download_chunk(&chunk_md.bucket, &path, chunk_md.codec)
                .await
            {
                Ok(chunk) => chunk,
                Err(e) => {
                    warn!(
                        bucket = chunk_md.bucket,
                        key = path,
                        error = ?e,
                        "metadata exists but s3 object is missing, skipping"
                    );
                    continue;
                }
            };
            // Recalculate block hash and compare it to the stored hash
            if chunk.block.hash() != chunk_md.hash {
                warn!(
                    key = path,
                    bucket = config.bucket,
                    "download chunk hash mismatch, skip publish"
                );
                continue;
            }

            let mut bytes_total = 0;
            let messages_total = chunk.block.messages.len();

            for message in chunk.block.messages {
                bytes_total += message.payload.len();
                self.nats_client
                    .publish(write_subject.clone(), message.payload)
                    .await?;
            }

            let nats_metrics = self.metrics.nats.write().await;
            let labels = metrics::NatsLabels {
                subject: write_subject.clone(),
                // TODO: change metrics labels
                stream: "".to_string(),
            };
            nats_metrics
                .load_messages
                .get_or_create(&labels)
                .inc_by(messages_total as u64);
            nats_metrics
                .load_bytes
                .get_or_create(&labels)
                .inc_by(bytes_total as u64);
            drop(nats_metrics);

            if config.delete_chunks {
                if let Err(e) = self.s3_client.delete_chunk(&chunk_md.bucket, &path).await {
                    warn!(
                        bucket = chunk_md.bucket,
                        path = path,
                        error = ?e,
                        "fail delete chunk from s3, skip soft delete"
                    );
                    continue;
                }

                if let Err(e) = self
                    .chunk_db
                    .soft_delete_chunk(chunk_md.sequence_number)
                    .await
                {
                    warn!(
                        sequence_number = chunk_md.sequence_number,
                        error = ?e,
                        "fail soft delete chunk metadata after s3 delete"
                    );
                }
            }
        }

        debug!(
            read_subject = read_subject,
            write_subject = write_subject,
            bucket = config.bucket,
            "finish download from s3 and publish to nats"
        );
        Ok(())
    }
}

// MessageBuffer is a thread safe Vec<jetstream::Message>
struct MessageBuffer {
    messages: Arc<RwLock<Vec<jetstream::Message>>>,
}

impl MessageBuffer {
    fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // starts a thread periodically keeping nats messages alive
    fn keep_alive(&self, interval: time::Duration) {
        let messages = self.messages.clone();
        // keepalive thread currently runs forever.
        // TODO: cancel thread on job cancellation.
        tokio::spawn(async move {
            let mut interval = time::interval(interval);
            loop {
                // send ack::progress for all messages
                let messages = messages.read().await;
                for i in 0..messages.len() {
                    let message = &messages[i];
                    match message
                        .ack_with(jetstream::message::AckKind::Progress)
                        .await
                    {
                        Ok(()) => {}
                        Err(err) => warn!(err = err, "message ack::progress"),
                    }
                }
                // release read lock
                drop(messages);
                // sleep for interval
                interval.tick().await;
            }
        });
    }

    // push message onto vec
    async fn push(&self, message: jetstream::Message) -> () {
        self.messages.clone().write_owned().await.push(message);
    }

    // clear vec
    async fn clear(&self) -> () {
        self.messages.clone().write_owned().await.clear();
    }

    async fn len(&self) -> usize {
        self.messages.clone().write_owned().await.len()
    }
    async fn to_vec(&self) -> Vec<jetstream::Message> {
        self.messages.clone().read().await.to_vec()
    }

    // ack all messages with nats
    async fn ack_all(&self) -> () {
        let messages = self.messages.read().await;
        for i in 0..messages.len() {
            let message = &messages[i];
            match message.ack().await {
                Ok(()) => {}
                Err(err) => warn!(err = err, "message ack"),
            }
        }
    }
}
