use anyhow::{Context, Error, Result};
use async_nats::jetstream;
use futures::StreamExt;
use std::{str::from_utf8, time::SystemTime};

use clap::{Parser, Subcommand};

mod encoding;
mod nats;
mod s3;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // Consume messages from NATS and store in S3
    Store {
        /// NATS URL
        #[arg(long, default_value = "nats://localhost:4222")]
        nats_url: String,

        /// Name of the stream
        #[arg(long)]
        stream: String,

        /// Name of the subject
        #[arg(long)]
        subject: String,

        /// S3 bucket name
        #[arg(long)]
        bucket: String,

        /// S3 region
        #[arg(long, default_value = "us-east-1")]
        region: String,

        /// S3 endpoint
        #[arg(long)]
        endpoint: String,

        /// S3 access key
        #[arg(long)]
        access_key: String,

        /// S3 secret key
        #[arg(long)]
        secret_key: String,
    },
    /// Load messages from S3 and publish to NATS
    Load {
        /// NATS URL
        #[arg(long, default_value = "nats://localhost:4222")]
        nats_url: String,

        /// NATS subject to read from
        #[arg(long)]
        read_subject: String,

        /// NATS subject to write to
        #[arg(long)]
        write_subject: String,

        /// S3 bucket name
        #[arg(long)]
        bucket: String,

        /// S3 region
        #[arg(long, default_value = "us-east-1")]
        region: String,

        /// S3 endpoint
        #[arg(long)]
        endpoint: String,

        /// S3 access key
        #[arg(long)]
        access_key: String,

        /// S3 secret key
        #[arg(long)]
        secret_key: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    match args.command {
        Command::Store {
            nats_url,
            stream,
            subject,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
        } => {
            store(
                nats_url, stream, subject, bucket, region, endpoint, access_key, secret_key,
            )
            .await?
        }
        Command::Load {
            nats_url,
            read_subject,
            write_subject,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
        } => {
            load(
                nats_url,
                read_subject,
                write_subject,
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
            )
            .await?
        }
    }
    Ok(())
}

async fn store(
    nats_url: String,
    stream: String,
    subject: String,
    bucket: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
) -> Result<(), Error> {
    let s3_client = s3::Client::new(&region, &endpoint, &access_key, &secret_key);
    let nats_client = nats::Client::new(nats_url)
        .await
        .context("failed to connect to nats server")?;

    let mut buffer: Vec<jetstream::Message> = Vec::new();
    let mut block_size = 0;
    let mut messages = nats_client.consume(stream, subject.clone()).await?;

    while let Some(message) = messages.next().await {
        let message = message?;
        println!(
            "got message on subject {} with payload {:?}",
            message.subject,
            from_utf8(&message.payload)?
        );
        block_size += &message.length;
        buffer.push(message);

        // Upload to S3 if threshold's reached
        if buffer.len() > encoding::BUFFER_MAX || block_size > encoding::BLOCK_MAX {
            let block = encoding::MessageBlock::from(buffer.clone());
            let chunk = encoding::Chunk::from_block(block);
            let path = format!("{}/{}", subject, time());
            s3_client.upload_chunk(chunk, &bucket, &path).await?;
            println!("wrote chunk to s3 at path {}", path);
            for message in &buffer {
                message.ack().await.expect("ack");
                // match message.ack().await {
                //     Ok(()) => {}
                //     Err(err) => dbg!("{}", err),
                // }
            }
            // Clear buffer and counter
            buffer.clear();
            block_size = 0;
        }
    }
    Ok(())
}

async fn load(
    nats_url: String,
    read_subject: String,
    write_subject: String,
    bucket: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
) -> Result<(), Error> {
    let nats_client = nats::Client::new(nats_url)
        .await
        .context("failed to connect to nats server")?;
    let s3_client = s3::Client::new(&region, &endpoint, &access_key, &secret_key);
    let paths = s3_client.list_paths(&bucket, &read_subject).await?;

    for path in paths {
        let prefix = format!("{}/", read_subject);
        if let Some(key) = path.strip_prefix(&prefix) {
            let _key_int = key.parse::<i128>()?;
            // if key_int > 1701150969777385 {
            //     break;
            // }
            println!("trying s3 path {}", path);
            let chunk = s3_client.download_chunk(&bucket, &path).await?;
            for message in chunk.block.messages {
                println!("load message {}", from_utf8(&message.payload)?);
                println!("write message to {}", write_subject);
                nats_client
                    .publish(write_subject.clone(), message.payload)
                    .await?;
            }
        }
    }
    Ok(())
}

fn time() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
