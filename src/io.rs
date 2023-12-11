use async_nats::jetstream;
use futures::StreamExt;
use std::sync::Arc;
use std::{str::from_utf8, time::SystemTime};
use tokio::{sync::RwLock, time};

use crate::encoding;
use crate::nats;
use crate::s3;
use anyhow::Result;
use tracing::{debug, trace, warn};

const KEEP_ALIVE_INTERVAL: time::Duration = time::Duration::from_secs(10);

// IO handles interfacing with NATs and S3
#[derive(Debug, Clone)]
pub struct IO {
    pub s3_client: s3::Client,
    pub nats_client: nats::Client,
}

impl IO {
    pub fn new(s3_client: s3::Client, nats_client: nats::Client) -> IO {
        debug!("creating new IO instance");

        let io = IO {
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
        max_bytes: i64,
        max_count: i64,
    ) -> Result<()> {
        debug!(
            stream = stream,
            subject = subject,
            bucket = bucket,
            "starting to consume from stream and upload to bucket"
        );

        // create new buffer and start thread to
        // let nats know all messages are in progress.
        let buffer = MessageBuffer::new();
        buffer.keep_alive(KEEP_ALIVE_INTERVAL);

        let mut block_size = 0;
        let mut messages = self
            .nats_client
            .consume(stream, subject.clone(), max_count)
            .await?;

        while let Some(message) = messages.next().await {
            let message = message?;
            trace!(
                subject = message.subject,
                "got message with payload {:?}",
                from_utf8(&message.payload)
            );
            block_size += &message.length;
            buffer.push(message).await;

            // Upload to S3 if threshold's reached
            let buffer_count = buffer.len().await;
            if buffer_count >= max_count as usize || block_size >= max_bytes as usize {
                debug!(
                    buffer_size = buffer_count,
                    block_size = block_size,
                    "reached buffer threshold"
                );
                let block = encoding::MessageBlock::from(buffer.to_vec().await);
                let chunk = encoding::Chunk::from_block(block);
                let path = format!("{}/{}", subject, time());
                self.s3_client.upload_chunk(chunk, &bucket, &path).await?;

                // Ack all messages, clear buffer and counter
                buffer.ack_all().await;
                buffer.clear().await;
                block_size = 0;
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
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<()> {
        trace!(
            read_stream = read_stream,
            read_subject = read_subject,
            write_stream = write_stream,
            write_subject = write_subject,
            bucket = bucket,
            start = start,
            end = end,
            "starting to download from bucket and publish to stream "
        );
        let paths = self.s3_client.list_paths(&bucket, &read_subject).await?;

        for path in paths {
            let prefix = format!("{}/", read_subject);

            // check if block falls within allowed timespan.
            // since each block's key is the unix timestamp at
            // upload time, we can parse and compare.
            if let Some(key) = path.strip_prefix(&prefix) {
                let key_int = key.parse::<usize>()?;
                if let Some(start) = start {
                    if key_int < start {
                        trace!(
                            start = start,
                            key = key_int,
                            "message block falls before time window, skipping"
                        );
                        continue;
                    }
                }
                if let Some(end) = end {
                    if key_int > end {
                        trace!(
                            start = start,
                            key = key_int,
                            "message block falls after time window, skipping"
                        );
                        // TODO: potentially skip rest of keys if ensured
                        // they are sorted such that all remaining are too old.
                        continue;
                    }
                }

                // download from s3 and publish to nats
                let chunk = self.s3_client.download_chunk(&bucket, &path).await?;
                for message in chunk.block.messages {
                    let subject = format!("{}.{}", write_stream, write_subject);
                    trace!(
                        bytes = message.payload.len(),
                        subject = subject,
                        "writing message to nats"
                    );
                    self.nats_client
                        .publish(write_subject.clone(), message.payload)
                        .await?;
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

fn time() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
