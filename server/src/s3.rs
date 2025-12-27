use anyhow::{Context, Result};
use nats3_types::Codec;
use s3::{creds::Credentials, Bucket, BucketConfiguration, Region};
use tracing::{debug, info, warn};

use crate::{encoding, metrics};

#[derive(Clone, Debug)]
pub struct Client {
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    metrics: metrics::Metrics,
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
        if code != 200 {
            warn!(
                code = code,
                bucket = bucket_name,
                path = path,
                "upload chunk, unexpected status code"
            )
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
        let resp = bucket.get_object(path).await?;
        let code = resp.status_code();
        if code != 200 {
            warn!(
                code = code,
                bucket = bucket_name,
                path = path,
                "download chunk, unexpected status code"
            )
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
        let response_data = bucket.delete_object(path).await?;
        let code = response_data.status_code();
        if code >= 300 {
            warn!(
                code = code,
                bucket = bucket_name,
                path = path,
                "delete chunk, unexpected status code"
            )
        }
        debug!(
            bucket = bucket_name,
            path = path,
            "finish delete block from s3"
        );
        Ok(())
    }

    async fn bucket(&self, bucket_name: &str, try_create: bool) -> Result<s3::Bucket> {
        let region = Region::Custom {
            region: self.region.to_string(),
            endpoint: self.endpoint.to_string(),
        };
        let credentials = Credentials::new(
            Some(self.access_key.as_str()),
            Some(self.secret_key.as_str()),
            None,
            None,
            None,
        )?;

        let mut bucket =
            Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();

        if try_create && !bucket.exists().await? {
            bucket = Bucket::create_with_path_style(
                bucket_name,
                region.clone(),
                credentials,
                BucketConfiguration::default(),
            )
            .await
            .context("create bucket")?
            .bucket;
            info!(
                bucket = bucket_name,
                endpoint = region.endpoint(),
                "create bucket in s3"
            );
        }
        Ok(*bucket)
    }
}
