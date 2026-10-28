use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use nats3_types::Codec;
use tokio::{sync::mpsc, time};
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

        let mut buffer: Vec<jetstream::Message> = Vec::new();
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

        let mut interval = time::interval(DEFAULT_BATCH_WAIT);
        interval.tick().await;
        let mut keep_alive = time::interval(KEEP_ALIVE_INTERVAL);
        keep_alive.tick().await;

        loop {
            tokio::select! {
                maybe_message = messages.next() => {
                    match maybe_message {
                        Some(Ok(message)) => {
                            trace!(
                                subject = message.subject.to_string(),
                                "consumer got message"
                            );
                            bytes_total += message.payload.len();
                            buffer.push(message);

                            if buffer.len() >= config.messages_max as usize
                                || bytes_total >= config.bytes_max as usize
                            {
                                self.upload_buffer(&mut buffer, &mut bytes_total, &config, prefix).await?;
                            }
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        debug!(messages = buffer.len(), "timer triggered upload");
                        self.upload_buffer(&mut buffer, &mut bytes_total, &config, prefix).await?;
                    }
                }
                _ = keep_alive.tick() => {
                    keep_alive_messages(&buffer).await;
                }
                _ = pause_token.cancelled() => {
                    debug!("consume stream paused, flushing buffer");
                    if !buffer.is_empty() {
                        self.upload_buffer(&mut buffer, &mut bytes_total, &config, prefix).await?;
                    }
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Paused,
                        job_id: job_id.clone(),
                        kind: registry::JobKind::Store,
                    });
                    debug!("buffer flushed, exiting gracefully for pause");
                    return Ok(());
                }
                _ = cancel_token.cancelled() => {
                    debug!("consume stream cancelled, flushing buffer");
                    if !buffer.is_empty() {
                        self.upload_buffer(&mut buffer, &mut bytes_total, &config, prefix).await?;
                    }
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Cancelled,
                        job_id: job_id.clone(),
                        kind: registry::JobKind::Store,
                    });
                    return Ok(());
                }
            }
        }
        let _ = exit_tx.send(registry::TaskExitInfo {
            reason: registry::TaskExitReason::Completed(Ok(())),
            job_id,
            kind: registry::JobKind::Store,
        });
        Ok(())
    }

    async fn upload_buffer(
        &self,
        buffer: &mut Vec<jetstream::Message>,
        bytes_total: &mut usize,
        config: &ConsumeConfig,
        prefix: &Option<String>,
    ) -> Result<()> {
        let messages_total = buffer.len();

        debug!(
            messages = messages_total,
            bytes = *bytes_total,
            "buffer threshold reached"
        );

        let block = encoding::MessageBlock::from(buffer.clone());
        let chunk = encoding::Chunk::from(block);
        let key = chunk.key(config.codec.clone()).to_string();

        let key = format!("{}/{}/{}", config.stream, config.subject, key);
        let path = db::object_path(prefix.as_deref(), &key);

        let serialized = chunk.serialize(config.codec.clone())?;
        let byte_count = serialized.len();
        self.s3_client
            .upload_chunk(serialized, &config.bucket, &path, config.codec.clone())
            .await?;

        let chunk_md = chunk.to_chunk_metadata(config, &key, byte_count);
        match self.chunk_db.create_chunk(chunk_md).await {
            Ok(_) => {}
            Err(db::ChunkMetadataError::Duplicate { bucket, key }) => {
                debug!(bucket = bucket, key = key, "chunk already stored");
            }
            Err(e) => return Err(e.into()),
        }

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

        for message in buffer.iter() {
            if let Err(err) = message.ack().await {
                warn!(err = err, "message ack");
            }
        }
        buffer.clear();
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
                        kind: registry::JobKind::Load,
                    });
                    return Ok(());
                }
                if pause_token.is_cancelled() {
                    debug!("publish stream paused during chunk list");
                    let _ = exit_tx.send(registry::TaskExitInfo {
                        reason: registry::TaskExitReason::Paused,
                        job_id: job_id.clone(),
                        kind: registry::JobKind::Load,
                    });

                    return Ok(());
                }
                let path = chunk_md.object_path();

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
                                kind: registry::JobKind::Load,
                            });
                            return Ok(());
                        }
                        _ = pause_token.cancelled() => {
                            debug!("publish stream paused during sleep");
                            let _ = exit_tx.send(registry::TaskExitInfo {
                                reason: registry::TaskExitReason::Paused,
                                job_id: job_id.clone(),
                                kind: registry::JobKind::Load,
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
            kind: registry::JobKind::Load,
        });

        Ok(())
    }
}

async fn keep_alive_messages(buffer: &[jetstream::Message]) {
    for message in buffer {
        if let Err(err) = message
            .ack_with(jetstream::message::AckKind::Progress)
            .await
        {
            warn!(err = ?err, "message ack::progress");
        }
    }
}
