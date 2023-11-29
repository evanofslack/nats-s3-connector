use anyhow::{Context, Error, Result};
use bincode;
use s3::{creds::Credentials, serde_types::Object, Bucket, BucketConfiguration, Region};

use crate::encoding;

pub struct Client<'a> {
    region: &'a str,
    endpoint: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

impl<'a> Client<'a> {
    pub fn new(
        region: &'a str,
        endpoint: &'a str,
        access_key: &'a str,
        secret_key: &'a str,
    ) -> Self {
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
    ) -> Result<(), Error> {
        let bucket = self.bucket(bucket_name, true).await?;
        let data = bincode::serialize(&chunk).context("chunk serialization")?;
        let response_data = bucket.put_object(path, &data).await.context("put object")?;
        assert_eq!(response_data.status_code(), 200);
        println!("uploaded block to s3");
        Ok(())
    }

    pub async fn download_chunk(
        &self,
        bucket_name: &str,
        path: &str,
    ) -> Result<encoding::Chunk, Error> {
        let bucket = self.bucket(bucket_name, false).await?;
        let response_data = bucket.get_object(path).await?;
        assert_eq!(response_data.status_code(), 200);
        let chunk: encoding::Chunk = bincode::deserialize(response_data.as_slice()).unwrap();

        println!("downloaded block to s3");
        Ok(chunk)
    }

    pub async fn list_paths(&self, bucket_name: &str, path: &str) -> Result<Vec<String>, Error> {
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
