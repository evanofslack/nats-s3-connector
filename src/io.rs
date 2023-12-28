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

        let io = IO {
            metrics,
            s3_client,
            nats_client,
        };

        return io;
    }
    pub async fn consume_stream(
        &self,
        stream: String,
        subject: String,
        bucket: String,
        prefix: Option<String>,
        bytes_max: i64,
        messages_max: i64,
        codec: encoding::Codec,
    ) -> Result<()> {
        debug!(
            stream = stream,
            subject = subject,
            bucket = bucket,
            prefix = prefix,
            codec = codec.to_string(),
            "starting to consume from stream and upload to bucket"
        );

        // create new buffer and start thread to
        // let nats know all messages are in progress.
        let buffer = MessageBuffer::new();
        buffer.keep_alive(KEEP_ALIVE_INTERVAL);

        let mut bytes_total = 0;
        let mut messages = self
            .nats_client
            .consume(stream.clone(), subject.clone(), messages_max)
            .await?;
        let prefix = &prefix;

        while let Some(message) = messages.next().await {
            let message = message?;
            trace!(
                subject = message.subject,
                "got message with payload {:?}",
                from_utf8(&message.payload)
            );
            bytes_total += &message.payload.len();
            buffer.push(message).await;

            // upload to S3 if thresholds reached
            let messages_total = buffer.len().await;
            if messages_total >= messages_max as usize || bytes_total >= bytes_max as usize {
                debug!(
                    messages = messages_total,
                    bytes = bytes_total,
                    "reached buffer threshold"
                );
                let block = encoding::MessageBlock::from(buffer.to_vec().await);
                let chunk = encoding::Chunk::from_block(block);
                let key = chunk.key(codec.clone()).to_string();

                // stream and consumer/subject are part of path
                let key = format!("{stream}/{subject}/{key}");

                // append the prefix if provided
                let path = if let Some(prefix) = prefix {
                    format!("{}/{}", prefix, key)
                } else {
                    key
                };

                // do upload
                self.s3_client
                    .upload_chunk(chunk, &bucket, &path, codec.clone())
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

    pub async fn publish_stream(
        &self,
        read_stream: String,
        read_subject: String,
        write_stream: String,
        write_subject: String,
        bucket: String,
        key_prefix: Option<String>,
        delete_chunks: bool,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<()> {
        trace!(
            read_stream = read_stream,
            read_subject = read_subject,
            write_stream = write_stream,
            write_subject = write_subject,
            bucket = bucket,
            prefix = key_prefix,
            delete_chunks = delete_chunks,
            start = start,
            end = end,
            "starting to download from bucket and publish to stream "
        );

        // build up the prefix
        let mut prefix = format!("{read_stream}/{read_subject}");
        // append optional provided bucket prefix
        if let Some(pre) = key_prefix {
            prefix = format!("{pre}/{prefix}")
        }

        let paths = self.s3_client.list_paths(&bucket, &prefix).await?;
        for path in paths {
            // check if block falls within allowed timespan.
            // since each block's key is the unix timestamp at
            // upload time, we can parse and compare.
            let prefix = format!("{prefix}/");
            if let Some(key) = path.strip_prefix(&prefix) {
                let chunk_key = encoding::ChunkKey::from_string(key.to_string())?;
                if let Some(start) = start {
                    if chunk_key.timestamp < start as u128 {
                        trace!(
                            start = start,
                            key = chunk_key.timestamp,
                            "message block falls before time window, skipping"
                        );
                        continue;
                    }
                }
                if let Some(end) = end {
                    if chunk_key.timestamp > end as u128 {
                        trace!(
                            start = start,
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
                    .download_chunk(&bucket, &path, chunk_key.codec)
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
                if delete_chunks {
                    self.s3_client.delete_chunk(&bucket, &path).await?;
                }
            }
        }
        debug!(
            read_subject = read_subject,
            write_subject = write_subject,
            bucket = bucket,
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
    fn keep_alive(&self, interval: time::Duration) -> () {
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
