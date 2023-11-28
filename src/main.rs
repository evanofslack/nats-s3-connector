use anyhow::{Context, Error, Result};
use async_nats::jetstream::{self, consumer::pull::Stream, consumer::PullConsumer};
use bincode;
use futures::StreamExt;
use s3::{creds::Credentials, serde_types::Object, Bucket, BucketConfiguration, Region};
use sha2::{Digest, Sha256};
use std::str::from_utf8;
use std::time::SystemTime;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const MAGIC_NUMBER: &'static str = "S3NATSCONNECT";
const VERSION: &'static str = "1";
const BUFFER_MAX: usize = 1000;
const BLOCK_MAX: usize = 10000000;

// Our repr of a NATS message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// Subject to which message is published to.
    pub subject: String,
    /// Payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    /// Optional headers.
    pub headers: Option<HashMap<String, String>>,
    pub length: usize,
}

impl From<jetstream::Message> for Message {
    fn from(source: jetstream::Message) -> Message {
        Message {
            subject: source.subject.clone(),
            payload: source.payload.clone(),
            headers: None,
            length: source.length,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MessageBlock {
    messages: Vec<Message>,
}

impl From<Vec<jetstream::Message>> for MessageBlock {
    fn from(js_messages: Vec<jetstream::Message>) -> MessageBlock {
        let mut messages = Vec::new();
        for m in js_messages {
            messages.push(Message::from(m))
        }
        MessageBlock { messages }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Chunk {
    magic_number: String,
    version: String,
    block: MessageBlock,
    hash: Vec<u8>,
}

impl Chunk {
    fn from_block(block: MessageBlock) -> Self {
        let payload: Vec<u8> = bincode::serialize(&block).unwrap();
        let _hash = Sha256::digest(&payload);

        Chunk {
            magic_number: MAGIC_NUMBER.to_string(),
            version: VERSION.to_string(),
            block,
            hash: _hash.to_vec(),
        }
    }
}

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
    let s3_client = S3Client::new(&region, &endpoint, &access_key, &secret_key);
    let nats_client = NatsClient::new(nats_url)
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
        if buffer.len() > BUFFER_MAX || block_size > BLOCK_MAX {
            let block = MessageBlock::from(buffer.clone());
            let chunk = Chunk::from_block(block);
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
    let nats_client = NatsClient::new(nats_url)
        .await
        .context("failed to connect to nats server")?;
    let s3_client = S3Client::new(&region, &endpoint, &access_key, &secret_key);
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

struct NatsClient {
    client: async_nats::Client,
}

impl NatsClient {
    async fn new(url: String) -> Result<Self, Error> {
        let client = async_nats::connect(url.clone())
            .await
            .context("failed to connect to nats server")?;
        let client = NatsClient { client };
        return Ok(client);
    }

    async fn consume(&self, stream_name: String, subject: String) -> Result<Stream, Error> {
        let jetstream = jetstream::new(self.client.clone());

        let stream = jetstream.get_stream(stream_name.clone()).await?;
        // let consumer: PullConsumer = stream.get_consumer(&consumer_name).await?;

        let consumer: PullConsumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(subject.into()),
                ..Default::default()
            })
            .await?;
        let messages = consumer.messages().await?;
        return Ok(messages);
    }

    async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
        let jetstream = jetstream::new(self.client.clone());
        jetstream.publish(subject, payload).await?;
        return Ok(());
    }
}

struct S3Client<'a> {
    region: &'a str,
    endpoint: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

impl<'a> S3Client<'a> {
    fn new(region: &'a str, endpoint: &'a str, access_key: &'a str, secret_key: &'a str) -> Self {
        return S3Client {
            region,
            endpoint,
            access_key,
            secret_key,
        };
    }

    async fn upload_chunk(&self, chunk: Chunk, bucket_name: &str, path: &str) -> Result<(), Error> {
        let bucket = self.bucket(bucket_name, true).await?;
        let data = bincode::serialize(&chunk).context("chunk serialization")?;
        let response_data = bucket.put_object(path, &data).await.context("put object")?;
        assert_eq!(response_data.status_code(), 200);
        println!("uploaded block to s3");
        Ok(())
    }

    async fn download_chunk(&self, bucket_name: &str, path: &str) -> Result<Chunk, Error> {
        let bucket = self.bucket(bucket_name, false).await?;
        let response_data = bucket.get_object(path).await?;
        assert_eq!(response_data.status_code(), 200);
        let chunk: Chunk = bincode::deserialize(response_data.as_slice()).unwrap();

        println!("downloaded block to s3");
        Ok(chunk)
    }

    async fn list_paths(&self, bucket_name: &str, path: &str) -> Result<Vec<String>, Error> {
        let bucket = self.bucket(bucket_name, false).await?;
        let prefix = path.to_string();
        // println!("[prefix: {}", prefix);
        let results = bucket.list(prefix, None).await?;
        // println!("list_paths results: {:?}", results);

        let mut objects: Vec<Object> = Vec::new();
        for mut result in results {
            objects.append(&mut result.contents)
        }

        let paths: Vec<String> = objects.into_iter().map(|obj| obj.key).collect();
        // println!("list_paths paths: {:?}", paths);
        return Ok(paths);
        // return Ok(Vec::new());
    }

    async fn bucket(&self, bucket_name: &str, try_create: bool) -> Result<s3::Bucket, Error> {
        let region = Region::Custom {
            region: self.region.to_string(),
            endpoint: self.endpoint.to_string(),
        };
        let credentials = Credentials::new(
            Some(self.access_key),
            Some(self.secret_key),
            None,
            None,
            None,
        )?;

        let mut bucket =
            Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();

        if try_create {
            if !bucket.exists().await? {
                bucket = Bucket::create_with_path_style(
                    bucket_name,
                    region,
                    credentials,
                    BucketConfiguration::default(),
                )
                .await
                .context("create bucket")?
                .bucket;
            }
        }
        Ok(bucket)
    }
}

fn time() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
