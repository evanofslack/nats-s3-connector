use async_nats::jetstream;
use futures::StreamExt;
use std::{str::from_utf8, sync::Arc, time::SystemTime};

use crate::config::Config;
use crate::encoding;
use crate::nats;
use crate::s3;
use anyhow::{Context, Result};
use tracing::{debug, trace, warn};

/// Load balancer application
#[derive(Debug, Clone)]
pub struct App {
    pub config: Arc<Config>,
    pub s3_client: s3::Client,
    pub nats_client: nats::Client,
}

/// Construct a new instance of NATS-S3
pub async fn new(config: Config) -> Result<App> {
    debug!("creating new application from config");

    let s3_client = s3::Client::new(
        config.s3.region.clone(),
        config.s3.endpoint.clone(),
        config.s3.access_key.clone(),
        config.s3.secret_key.clone(),
    );

    let nats_client = nats::Client::new(config.nats.url.clone())
        .await
        .context("failed to connect to nats server")?;

    let app = App {
        config: Arc::new(config),
        s3_client,
        nats_client,
    };

    return Ok(app);
}

impl App {
    pub async fn start_store_jobs(&self) {
        // start all store jobs defined in config
        if let Some(stores) = self.config.clone().store.clone() {
            debug!("starting up store jobs");
            for store in stores.iter() {
                // must clone the instances we pass to the async thread
                let app = self.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(err) = app
                        .consume_stream(
                            store.stream.clone(),
                            store.subject.clone(),
                            store.bucket.clone(),
                        )
                        .await
                    {
                        println!("{}", err);
                    }
                });
            }
        }
    }
    async fn consume_stream(&self, stream: String, subject: String, bucket: String) -> Result<()> {
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
            if buffer.len() > encoding::BUFFER_MAX || block_size > encoding::BLOCK_MAX {
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

    async fn _publish_stream(
        &self,
        read_subject: String,
        write_subject: String,
        bucket: String,
    ) -> Result<()> {
        let paths = self.s3_client.list_paths(&bucket, &read_subject).await?;

        for path in paths {
            let prefix = format!("{}/", read_subject);
            if let Some(key) = path.strip_prefix(&prefix) {
                let _key_int = key.parse::<i128>()?;
                // if key_int > 1701150969777385 {
                //     break;
                // }
                println!("trying s3 path {}", path);
                let chunk = self.s3_client.download_chunk(&bucket, &path).await?;
                for message in chunk.block.messages {
                    println!("load message {}", from_utf8(&message.payload)?);
                    println!("write message to {}", write_subject);
                    self.nats_client
                        .publish(write_subject.clone(), message.payload)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

fn time() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
