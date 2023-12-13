use anyhow::{Context, Result};
use bincode;
use s3::{creds::Credentials, serde_types::Object, Bucket, BucketConfiguration, Region};
use serde_json;
use tracing::{debug, info, trace, warn};

use crate::config;
use crate::encoding;

#[derive(Clone, Debug)]
pub struct Client {
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
}

impl Client {
    pub fn new(region: String, endpoint: String, access_key: String, secret_key: String) -> Self {
        debug!(
            endpoint = endpoint,
            region = region,
            "creating new s3 client"
        );
        return Client {
            region,
            endpoint,
            access_key,
            secret_key,
        };
    }

    pub async fn upload_chunk(
        &self,
        chunk: encoding::Chunk,
        bucket_name: &str,
        path: &str,
        codec: &config::Codec,
    ) -> Result<()> {
        let bucket = self.bucket(bucket_name, true).await?;
        let data = match codec {
            config::Codec::Json => serde_json::to_vec(&chunk).context("serialization")?,
            config::Codec::Binary => bincode::serialize(&chunk).context("serialization")?,
        };
        let mime_type = match codec {
            config::Codec::Binary => "bin",
            config::Codec::Json => "json",
        };
        let path = format!("{}.{}", path, mime_type);
        let response_data = bucket
            .put_object(path.clone(), &data)
            .await
            .context("put object")?;
        let code = response_data.status_code();
        if code != 200 {
            warn!(code = code, "unexpected status code")
        }
        info!(
            bucket = bucket_name,
            path = path,
            codec = codec.to_string(),
            "uploaded block to s3"
        );
        Ok(())
    }

    pub async fn download_chunk(&self, bucket_name: &str, path: &str) -> Result<encoding::Chunk> {
        let bucket = self.bucket(bucket_name, false).await?;
        let response_data = bucket.get_object(path).await?;
        let code = response_data.status_code();
        if code != 200 {
            warn!(code = code, "unexpected status code")
        }
        let chunk: encoding::Chunk = bincode::deserialize(response_data.as_slice()).unwrap();
        debug!(
            bucket = bucket_name,
            path = path,
            "downloaded block from s3"
        );
        Ok(chunk)
    }

    pub async fn delete_chunk(&self, bucket_name: &str, path: &str) -> Result<()> {
        let bucket = self.bucket(bucket_name, false).await?;
        let response_data = bucket.delete_object(path).await?;
        let code = response_data.status_code();
        if code != 200 {
            warn!(code = code, "unexpected status code")
        }
        debug!(bucket = bucket_name, path = path, "deleted block from s3");
        Ok(())
    }

    pub async fn list_paths(&self, bucket_name: &str, path: &str) -> Result<Vec<String>> {
        let bucket = self.bucket(bucket_name, false).await?;
        let prefix = path.to_string();
        let results = bucket.list(prefix, None).await?;

        let mut objects: Vec<Object> = Vec::new();
        for mut result in results {
            objects.append(&mut result.contents)
        }

        let paths: Vec<String> = objects.into_iter().map(|obj| obj.key).collect();
        trace!(bucket = bucket_name, path = path, "listed objects from s3");
        return Ok(paths);
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

        if try_create {
            if !bucket.exists().await? {
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
                    "created bucket in s3"
                );
            }
        }
        Ok(bucket)
    }
}
