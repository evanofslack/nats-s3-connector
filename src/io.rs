use async_nats::jetstream;
use futures::StreamExt;
use std::{str::from_utf8, time::SystemTime};

use crate::encoding;
use crate::nats;
use crate::s3;
use anyhow::Result;
use tracing::{debug, trace, warn};

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
        max_bytes: usize,
        max_count: usize,
    ) -> Result<()> {
        debug!(
            stream = stream,
            subject = subject,
            bucket = bucket,
            "starting to consume from stream and upload to bucket"
        );

        let mut buffer: Vec<jetstream::Message> = Vec::new();
        let mut block_size = 0;
        let mut messages = self.nats_client.consume(stream, subject.clone()).await?;

        while let Some(message) = messages.next().await {
            let message = message?;
            trace!(
                subject = message.subject,
                "got message with payload {:?}",
                from_utf8(&message.payload)
            );
            block_size += &message.length;
            buffer.push(message);

            // Upload to S3 if threshold's reached
            if buffer.len() > max_count || block_size > max_bytes {
                trace!(
                    buffer_size = buffer.len(),
                    block_size = block_size,
                    "reached buffer threshold"
                );
                let block = encoding::MessageBlock::from(buffer.clone());
                let chunk = encoding::Chunk::from_block(block);
                let path = format!("{}/{}", subject, time());
                self.s3_client.upload_chunk(chunk, &bucket, &path).await?;
                for message in &buffer {
                    match message.ack().await {
                        Ok(()) => {}
                        Err(err) => warn!(err = err, "message ack"),
                    }
                }
                // Clear buffer and counter
                buffer.clear();
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
                        continue;
                    }
                }
                if let Some(end) = end {
                    if key_int > end {
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

fn time() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
