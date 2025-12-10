use crate::config::Config;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
use nats3_client::Client;
use nats3_types::{Batch, CreateLoadJob, CreateStoreJob};
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration, Region};
use tracing::{debug, info};

pub async fn run(config: &Config) -> anyhow::Result<()> {
    info!("Starting setup phase");

    create_bucket(config).await?;
    check_health(config).await?;
    create_streams(config).await?;

    if !config.skip_store {
        create_store_job(config).await?;
    }

    if !config.skip_load {
        create_load_job(config).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    info!("Setup complete");

    Ok(())
}

async fn check_health(config: &Config) -> anyhow::Result<()> {
    debug!("Checking NATS connection");
    let client = async_nats::connect(&config.nats_url).await?;
    client.flush().await?;
    drop(client);

    debug!("Checking S3 connection");
    let (bucket_name, region, creds, _) = get_bucket_details(config)?;
    let mut bucket = Bucket::new(&bucket_name, region, creds)?;
    bucket.set_path_style();
    if !bucket.exists().await? {
        return Err(anyhow::anyhow!("bucket {} does not exist", bucket_name));
    }

    debug!("Checking NATS3 connection");
    let nats3_client = Client::new(config.nats3_url.clone());
    nats3_client.get_store_jobs().await?;

    Ok(())
}

fn get_bucket_details(
    config: &Config,
) -> anyhow::Result<(String, Region, Credentials, BucketConfiguration)> {
    let credentials = Credentials::new(
        Some(&config.s3_access_key),
        Some(&config.s3_secret_key),
        None,
        None,
        None,
    )?;
    let region = Region::Custom {
        region: if config.s3_region.is_empty() {
            "us-east-1".to_string()
        } else {
            config.s3_region.clone()
        },
        endpoint: config.s3_endpoint.clone(),
    };
    let bucket_config = BucketConfiguration::default();
    Ok((config.bucket_name(), region, credentials, bucket_config))
}

async fn create_bucket(config: &Config) -> anyhow::Result<()> {
    let (bucket_name, region, creds, bucket_config) = get_bucket_details(config)?;
    info!("Creating bucket: {}", bucket_name);
    Bucket::create_with_path_style(&bucket_name, region, creds, bucket_config).await?;
    Ok(())
}

async fn create_streams(config: &Config) -> anyhow::Result<()> {
    let client = async_nats::connect(&config.nats_url).await?;
    let jetstream = async_nats::jetstream::new(client);

    info!("Creating input stream: {}", config.input_stream);
    create_stream(&jetstream, &config.input_stream, &config.input_subject).await?;

    info!("Creating output stream: {}", config.output_stream);
    create_stream(&jetstream, &config.output_stream, &config.output_subject).await?;

    Ok(())
}

async fn create_stream(
    jetstream: &async_nats::jetstream::Context,
    name: &str,
    subject: &str,
) -> anyhow::Result<()> {
    let stream_config = StreamConfig {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        ..Default::default()
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(_) => {
            debug!("Stream created or already exists: {}", name);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

async fn create_store_job(config: &Config) -> anyhow::Result<()> {
    let client = Client::new(config.nats3_url.clone());
    let job_name = config.store_job_name();

    info!("Creating store job: {}", job_name);

    let create_job = CreateStoreJob {
        name: job_name.clone(),
        stream: config.input_stream.clone(),
        consumer: None,
        subject: config.input_subject.clone(),
        bucket: config.bucket_name(),
        prefix: None,
        batch: Some(Batch {
            max_bytes: 100000,
            max_count: 100,
        }),
        encoding: None,
    };

    match client.create_store_job(create_job).await {
        Ok(_) => info!("Store job created: {}", job_name),
        Err(e) => {
            if e.to_string().contains("already exists") {
                info!("Store job already exists: {}", job_name);
            } else {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

async fn create_load_job(config: &Config) -> anyhow::Result<()> {
    let client = Client::new(config.nats3_url.clone());
    let job_name = config.load_job_name();

    info!("Creating load job: {}", job_name);

    let create_job = CreateLoadJob {
        bucket: config.bucket_name(),
        prefix: None,
        read_stream: config.input_stream.clone(),
        read_consumer: None,
        read_subject: config.input_subject.clone(),
        write_subject: config.output_subject.clone(),
        delete_chunks: false,
        start: None,
        end: None,
    };

    match client.create_load_job(create_job).await {
        Ok(_) => info!("Load job created: {}", job_name),
        Err(e) => {
            if e.to_string().contains("already exists") {
                info!("Load job already exists: {}", job_name);
            } else {
                return Err(e.into());
            }
        }
    }
    Ok(())
}
