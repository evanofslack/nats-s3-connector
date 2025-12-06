use async_nats::jetstream;
use futures::StreamExt;
use std::str::from_utf8;
use std::sync::Arc;
use tokio::{sync::RwLock, time};

use crate::encoding;
use crate::metrics;
use crate::nats;
use crate::s3;
use anyhow::Result;
use tracing::{debug, trace, warn};

const KEEP_ALIVE_INTERVAL: time::Duration = time::Duration::from_secs(10);

pub struct ConsumeConfig {
    pub stream: String,
    pub subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub bytes_max: i64,
    pub messages_max: i64,
    pub codec: encoding::Codec,
}

pub struct PublishConfig {
    pub read_stream: String,
    pub read_subject: String,
    pub write_stream: String,
    pub write_subject: String,
    pub bucket: String,
    pub key_prefix: Option<String>,
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
}

impl IO {
    pub fn new(metrics: metrics::Metrics, s3_client: s3::Client, nats_client: nats::Client) -> IO {
        debug!("creating new IO instance");

        IO {
            metrics,
            s3_client,
            nats_client,
        }
    }
    pub async fn consume_stream(&self, config: ConsumeConfig) -> Result<()> {
        debug!(
            stream = config.stream,
            subject = config.subject,
            bucket = config.bucket,
            prefix = config.prefix,
            codec = config.codec.to_string(),
            "starting to consume from stream and upload to bucket"
        );

        // create new buffer and start thread to
        // let nats know all messages are in progress.
        let buffer = MessageBuffer::new();
        buffer.keep_alive(KEEP_ALIVE_INTERVAL);

        let mut bytes_total = 0;
        let mut messages = self
            .nats_client
            .consume(
                config.stream.clone(),
                config.subject.clone(),
                config.messages_max,
            )
            .await?;
        let prefix = &config.prefix;

        while let Some(message) = messages.next().await {
            let message = message?;
            trace!(
                subject = message.subject.to_string(),
                "got message with payload {:?}",
                from_utf8(&message.payload)
            );
            bytes_total += &message.payload.len();
            buffer.push(message).await;

            // upload to S3 if thresholds reached
            let messages_total = buffer.len().await;
            if messages_total >= config.messages_max as usize
                || bytes_total >= config.bytes_max as usize
            {
                debug!(
                    messages = messages_total,
                    bytes = bytes_total,
                    "reached buffer threshold"
                );
                let block = encoding::MessageBlock::from(buffer.to_vec().await);
                let chunk = encoding::Chunk::from_block(block);
                let key = chunk.key(config.codec.clone()).to_string();

                // stream and consumer/subject are part of path
                let stream = config.stream.clone();
                let subject = config.subject.clone();
                let key = format!("{stream}/{subject}/{key}");

                // append the prefix if provided
                let path = if let Some(prefix) = prefix {
                    format!("{}/{}", prefix, key)
                } else {
                    key
                };

                // do upload
                self.s3_client
                    .upload_chunk(chunk, &config.bucket, &path, config.codec.clone())
                    .await?;

                // increment metrics
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
                    .inc_by(bytes_total as u64);
                drop(nats_metrics);

                // ack all messages, clear buffer and counter
                buffer.ack_all().await;
                buffer.clear().await;
                bytes_total = 0;
            }
        }
        Ok(())
    }

    pub async fn publish_stream(&self, config: PublishConfig) -> Result<()> {
        trace!(
            read_stream = config.read_stream,
            read_subject = config.read_subject,
            write_stream = config.write_stream,
            write_subject = config.write_subject,
            bucket = config.bucket,
            prefix = config.key_prefix,
            delete_chunks = config.delete_chunks,
            start = config.start,
            end = config.end,
            "starting to download from bucket and publish to stream "
        );

        let write_stream = config.write_stream.clone();
        let write_subject = config.write_subject.clone();

        // build up the prefix
        let read_stream = config.read_stream.clone();
        let read_subject = config.read_subject.clone();
        let mut prefix = format!("{read_stream}/{read_subject}");
        // append optional provided bucket prefix
        if let Some(pre) = config.key_prefix {
            prefix = format!("{pre}/{prefix}")
        }

        let paths = self.s3_client.list_paths(&config.bucket, &prefix).await?;
        for path in paths {
            // check if block falls within allowed timespan.
            // since each block's key is the unix timestamp at
            // upload time, we can parse and compare.
            let prefix = format!("{prefix}/");
            if let Some(key) = path.strip_prefix(&prefix) {
                let chunk_key = encoding::ChunkKey::from_string(key.to_string())?;
                if let Some(start) = config.start {
                    if chunk_key.timestamp < start as u128 {
                        trace!(
                            start = start,
                            key = chunk_key.timestamp,
                            "message block falls before time window, skipping"
                        );
                        continue;
                    }
                }
                if let Some(end) = config.end {
                    if chunk_key.timestamp > end as u128 {
                        trace!(
                            start = config.start,
                            key = chunk_key.timestamp,
                            "message block falls after time window, skipping"
                        );
                        // TODO: potentially skip rest of keys if ensured
                        // they are sorted such that all remaining are too old.
                        continue;
                    }
                }

                // download from s3
                let chunk = self
                    .s3_client
                    .download_chunk(&config.bucket, &path, chunk_key.codec)
                    .await?;

                let mut bytes_total = 0;
                let messages_total = chunk.block.messages.len();
                let subject = format!("{write_stream}.{write_subject}");

                // publish each message to nats
                for message in chunk.block.messages {
                    bytes_total += message.payload.len();
                    self.nats_client
                        .publish(subject.clone(), message.payload)
                        .await?;
                }

                // increment metrics
                let nats_metrics = self.metrics.nats.write().await;
                let labels = metrics::NatsLabels {
                    subject: write_subject.clone(),
                    stream: write_stream.clone(),
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

                // if enabled, delete published chunk from s3
                if config.delete_chunks {
                    self.s3_client.delete_chunk(&config.bucket, &path).await?;
                }
            }
        }
        debug!(
            read_subject = read_subject,
            write_subject = write_subject,
            bucket = config.bucket,
            "finished download from s3 and publish to nats"
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
