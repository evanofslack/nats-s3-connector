use anyhow::{bail, Context, Result};
use nats3_types::Codec;
use s3::{creds::Credentials, error::S3Error, Bucket, BucketConfiguration, Region};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::{encoding, metrics};

#[derive(Clone, Debug)]
pub struct Client {
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    metrics: metrics::Metrics,
    buckets: Arc<Mutex<BucketCache>>,
}

#[derive(Debug, Default)]
struct BucketCache {
    buckets: HashMap<String, Box<Bucket>>,
    ensured: HashSet<String>,
}

impl Client {
    pub fn new(
        region: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
        metrics: metrics::Metrics,
    ) -> Self {
        debug!(endpoint = endpoint, region = region, "create new s3 client");
        Client {
            region,
            endpoint,
            access_key,
            secret_key,
            metrics,
            buckets: Arc::new(Mutex::new(BucketCache::default())),
        }
    }

    pub async fn upload_chunk(
        &self,
        chunk: Vec<u8>,
        bucket_name: &str,
        path: &str,
        codec: Codec,
    ) -> Result<()> {
        let bucket = self.bucket(bucket_name, true).await?;
        let resp = bucket
            .put_object(&path, &chunk)
            .await
            .context("put object")?;
        let code = resp.status_code();
        if !(200..300).contains(&code) {
            bail!("upload chunk {bucket_name}/{path}, unexpected status code {code}");
        }
        debug!(
            bucket = bucket_name,
            path = path,
            codec = codec.to_string(),
            "finish upload block to s3"
        );
        self.metrics
            .io
            .s3_objects_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_IN.to_string(),
            })
            .inc();
        self.metrics
            .io
            .s3_bytes_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_IN.to_string(),
            })
            .inc_by(chunk.len() as u64);
        Ok(())
    }

    pub async fn download_chunk(
        &self,
        bucket_name: &str,
        path: &str,
        codec: Codec,
    ) -> Result<encoding::Chunk> {
        let bucket = self.bucket(bucket_name, false).await?;
        let resp = bucket.get_object(path).await.context("get object")?;
        let code = resp.status_code();
        if !(200..300).contains(&code) {
            bail!("download chunk {bucket_name}/{path}, unexpected status code {code}");
        }
        let b = resp.bytes();
        let bytes_download = b.len();
        let chunk = encoding::Chunk::deserialize(b, codec.clone())?;

        debug!(
            bucket = bucket_name,
            path = path,
            codec = codec.to_string(),
            "finish download block from s3"
        );

        self.metrics
            .io
            .s3_objects_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_OUT.to_string(),
            })
            .inc();
        self.metrics
            .io
            .s3_bytes_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_OUT.to_string(),
            })
            .inc_by(bytes_download as u64);

        Ok(chunk)
    }

    pub async fn delete_chunk(&self, bucket_name: &str, path: &str) -> Result<()> {
        let bucket = self.bucket(bucket_name, false).await?;
        let code = match bucket.delete_object(path).await {
            Ok(resp) => resp.status_code(),
            Err(S3Error::HttpFailWithBody(code, _)) => code,
            Err(e) => return Err(e).context("delete object"),
        };
        if !(200..300).contains(&code) && code != 404 {
            bail!("delete chunk {bucket_name}/{path}, unexpected status code {code}");
        }
        debug!(
            bucket = bucket_name,
            path = path,
            "finish delete block from s3"
        );
        Ok(())
    }

    async fn bucket(&self, bucket_name: &str, try_create: bool) -> Result<Box<Bucket>> {
        let mut cache = self.buckets.lock().await;
        let bucket = match cache.buckets.get(bucket_name) {
            Some(bucket) => bucket.clone(),
            None => {
                let bucket =
                    Bucket::new(bucket_name, self.region(), self.credentials()?)?.with_path_style();
                cache
                    .buckets
                    .insert(bucket_name.to_string(), bucket.clone());
                bucket
            }
        };

        if try_create && !cache.ensured.contains(bucket_name) {
            if !bucket.exists().await? {
                Bucket::create_with_path_style(
                    bucket_name,
                    self.region(),
                    self.credentials()?,
                    BucketConfiguration::default(),
                )
                .await
                .context("create bucket")?;
                info!(
                    bucket = bucket_name,
                    endpoint = self.endpoint,
                    "create bucket in s3"
                );
            }
            cache.ensured.insert(bucket_name.to_string());
        }
        Ok(bucket)
    }

    fn region(&self) -> Region {
        Region::Custom {
            region: self.region.to_string(),
            endpoint: self.endpoint.to_string(),
        }
    }

    fn credentials(&self) -> Result<Credentials> {
        Ok(Credentials::new(
            Some(self.access_key.as_str()),
            Some(self.secret_key.as_str()),
            None,
            None,
            None,
        )?)
    }
}
