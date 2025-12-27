use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use nats3_types::Codec;
use std::sync::Arc;
use tokio::{
    sync::{mpsc, RwLock},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use nats3_types::{LoadJob, StoreJob};

use crate::{db, encoding, metrics, nats, registry, s3};

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

impl From<StoreJob> for ConsumeConfig {
    fn from(job: StoreJob) -> Self {
        Self {
            stream: job.stream,
            consumer: job.consumer,
            subject: job.subject,
            bucket: job.bucket,
            prefix: job.prefix,
            bytes_max: job.batch.max_bytes,
            messages_max: job.batch.max_count,
            codec: job.encoding.codec,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublishConfig {
    pub read_stream: String,
    pub read_consumer: Option<String>,
    pub read_subject: String,
    pub write_subject: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub poll_interval: Option<time::Duration>,
    pub delete_chunks: bool,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
}

impl From<LoadJob> for PublishConfig {
    fn from(job: LoadJob) -> Self {
        Self {
            read_stream: job.read_stream,
            read_consumer: job.read_consumer,
            read_subject: job.read_subject,
            write_subject: job.write_subject,
            bucket: job.bucket,
            prefix: job.prefix,
            poll_interval: job.poll_interval,
            delete_chunks: job.delete_chunks,
            from_time: job.from_time,
            to_time: job.to_time,
        }
    }
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
        debug!("create new IO instance");

        IO {
            metrics,
            s3_client,
            nats_client,
            chunk_db,
        }
    }
    pub async fn consume_stream(
        &self,
        job_id: String,
        config: ConsumeConfig,
        cancel_token: CancellationToken,
        pause_token: CancellationToken,
        exit_tx: mpsc::UnboundedSender<registry::TaskExitInfo>,
    ) -> Result<()> {
        debug!(
            job_id = job_id,
            stream = config.stream,
            subject = config.subject,
            bucket = config.bucket,
            prefix = config.prefix,
            codec = config.codec.to_string(),
            "consume stream and upload to bucket"
        );

        let buffer = MessageBuffer::new(cancel_token.clone(), pause_token.clone());
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
                                self.upload_buffer(&buffer, &mut bytes_total, &config, prefix).await?;
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
                        self.upload_buffer(&buffer, &mut bytes_total, &config, prefix).await?;
                    }
                }
                _ = pause_token.cancelled() => {
                    debug!("consume stream paused, flushing buffer");
                    let messages_total = buffer.len().await;
                    if messages_total > 0 {
                        self.upload_buffer(&buffer, &mut bytes_total, &config, prefix).await?;
                    }
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Paused,
                        job_id: job_id.clone(),
                    });
                    debug!("buffer flushed, exiting gracefully for pause");
                    return Ok(());
            }
                _ = cancel_token.cancelled() => {
                    debug!("consume stream cancelled, flushing buffer");
                    let messages_total = buffer.len().await;
                    if messages_total > 0 {
                        self.upload_buffer(&buffer, &mut bytes_total, &config, prefix).await?;
                    }
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Cancelled,
                        job_id: job_id.clone(),
                    });
                    return Ok(());
                    }
            }
        }
        let _ = exit_tx.send(registry::TaskExitInfo {
            reason: registry::TaskExitReason::Completed(Ok(())),
            job_id,
        });
        Ok(())
    }

    async fn upload_buffer(
        &self,
        buffer: &MessageBuffer,
        bytes_total: &mut usize,
        config: &ConsumeConfig,
        prefix: &Option<String>,
    ) -> Result<()> {
        let messages_total = buffer.len().await;

        debug!(
            messages = messages_total,
            bytes = *bytes_total,
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
        let byte_count = serialized.len();
        self.s3_client
            .upload_chunk(serialized, &config.bucket, &path, config.codec.clone())
            .await?;

        let chunk_md = chunk.to_chunk_metadata(config, &path, byte_count);
        self.chunk_db.create_chunk(chunk_md).await?;

        self.metrics
            .io
            .nats_messages_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_OUT.to_string(),
            })
            .inc_by(messages_total as u64);
        self.metrics
            .io
            .nats_bytes_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_OUT.to_string(),
            })
            .inc_by(byte_count as u64);

        buffer.ack_all().await;
        buffer.clear().await;
        *bytes_total = 0;

        Ok(())
    }

    pub async fn publish_stream(
        &self,
        job_id: String,
        config: PublishConfig,
        cancel_token: CancellationToken,
        pause_token: CancellationToken,
        exit_tx: mpsc::UnboundedSender<registry::TaskExitInfo>,
    ) -> Result<()> {
        debug!(
            job_id = job_id,
            read_stream = config.read_stream,
            read_consumer = config.read_consumer,
            read_subject = config.read_subject,
            write_subject = config.write_subject,
            bucket = config.bucket,
            prefix = config.prefix,
            delete_chunks = config.delete_chunks,
            "download from bucket and publish to stream"
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
            timestamp_start: config.from_time,
            timestamp_end: config.to_time,
            limit: None,
            include_deleted: false,
        };

        loop {
            let chunks = self.chunk_db.list_chunks(query.clone()).await?;
            for chunk_md in chunks {
                if cancel_token.is_cancelled() {
                    debug!("publish stream cancelled during chunk list");
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Cancelled,
                        job_id: job_id.clone(),
                    });
                    return Ok(());
                }
                if pause_token.is_cancelled() {
                    debug!("publish stream paused during chunk list");
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Paused,
                        job_id: job_id.clone(),
                    });

                    return Ok(());
                }
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
                        sequence_number = chunk_md.sequence_number,
                        "download chunk hash mismatch, skip publish"
                    );
                    continue;
                }

                for message in chunk.block.messages {
                    self.nats_client
                        .publish(write_subject.clone(), message.payload, message.headers)
                        .await?;
                }

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
            // Poll interval handling
            match config.poll_interval {
                None => break,
                Some(duration) => {
                    trace!(
                        duration_secs = duration.as_secs(),
                        "sleep poll interval duration for load job"
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                            continue;
                        }
                        _ = cancel_token.cancelled() => {
                            debug!("publish stream cancelled during sleep");
                            let _ = exit_tx.send(registry::TaskExitInfo {
                                reason: registry::TaskExitReason::Cancelled,
                                job_id: job_id.clone(),
                            });
                            return Ok(());
                        }
                        _ = pause_token.cancelled() => {
                            debug!("publish stream paused during sleep");
                            let _ = exit_tx.send(registry::TaskExitInfo {
                                reason: registry::TaskExitReason::Paused,
                                job_id: job_id.clone(),
                            });
                            return Ok(());
                        }
                    }
                }
            }
        }

        debug!(
            read_subject = read_subject,
            write_subject = write_subject,
            bucket = config.bucket,
            "finish download from s3 and publish to nats"
        );
        let _ = exit_tx.send(registry::TaskExitInfo {
            reason: registry::TaskExitReason::Completed(Ok(())),
            job_id,
        });

        Ok(())
    }
}

// MessageBuffer is a thread safe Vec<jetstream::Message>
struct MessageBuffer {
    messages: Arc<RwLock<Vec<jetstream::Message>>>,
    cancel_token: CancellationToken,
    pause_token: CancellationToken,
}

impl MessageBuffer {
    fn new(cancel_token: CancellationToken, pause_token: CancellationToken) -> Self {
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
            cancel_token,
            pause_token,
        }
    }

    // starts a thread periodically keeping nats messages alive
    fn keep_alive(&self, interval: time::Duration) {
        let messages = self.messages.clone();
        let cancel_token = self.cancel_token.clone();
        let pause_token = self.pause_token.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let messages = messages.read().await;
                        for i in 0..messages.len() {
                            let message = &messages[i];
                            match message
                                .ack_with(jetstream::message::AckKind::Progress)
                                .await
                            {
                                Ok(()) => {}
                                Err(err) => warn!(err = ?err, "message ack::progress"),
                            }
                        }
                        drop(messages);
                    }
                    _ = cancel_token.cancelled() => {
                        debug!("keepalive thread cancelled (cancel)");
                        return;
                    }
                    _ = pause_token.cancelled() => {
                        debug!("keepalive thread cancelled (pause)");
                        return;
                    }
                }
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
