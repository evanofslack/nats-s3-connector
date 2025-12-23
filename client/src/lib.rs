mod error;

pub use error::{ClientError, Result};
use nats3_types::{LoadJob, LoadJobCreate, StoreJob, StoreJobCreate};

pub struct Client {
    http: reqwest::Client,
    base_url: String,
}

impl Client {
    pub fn new(base_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    pub async fn get_load_job(&self, id: String) -> Result<LoadJob> {
        let url = format!("{}/load/job", self.base_url);
        let response = self.http.get(&url).query(&[("job_id", id)]).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn pause_load_job(&self, id: String) -> Result<LoadJob> {
        let url = format!("{}/load/job/pause", self.base_url);
        let response = self.http.post(&url).query(&[("job_id", id)]).send().await?;
        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn resume_load_job(&self, id: String) -> Result<LoadJob> {
        let url = format!("{}/load/job/resume", self.base_url);
        let response = self.http.post(&url).query(&[("job_id", id)]).send().await?;
        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn get_load_jobs(&self) -> Result<Vec<LoadJob>> {
        let url = format!("{}/load/jobs", self.base_url);
        let response = self.http.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn delete_load_job(&self, id: String) -> Result<()> {
        let url = format!("{}/load/job", self.base_url);
        let response = self
            .http
            .delete(&url)
            .query(&[("job_id", id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    pub async fn create_load_job(&self, job: LoadJobCreate) -> Result<LoadJob> {
        let url = format!("{}/load/job", self.base_url);
        let response = self.http.post(&url).json(&job).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn get_store_jobs(&self) -> Result<Vec<StoreJob>> {
        let url = format!("{}/store/jobs", self.base_url);
        let response = self.http.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn get_store_job(&self, id: String) -> Result<StoreJob> {
        let url = format!("{}/store/job", self.base_url);
        let response = self.http.get(&url).query(&[("job_id", id)]).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn pause_store_job(&self, id: String) -> Result<StoreJob> {
        let url = format!("{}/store/job/pause", self.base_url);
        let response = self.http.post(&url).query(&[("job_id", id)]).send().await?;
        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn resume_store_job(&self, id: String) -> Result<StoreJob> {
        let url = format!("{}/store/job/resume", self.base_url);
        let response = self.http.post(&url).query(&[("job_id", id)]).send().await?;
        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }

    pub async fn delete_store_job(&self, id: String) -> Result<()> {
        let url = format!("{}/store/job", self.base_url);
        let response = self
            .http
            .delete(&url)
            .query(&[("job_id", id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    pub async fn create_store_job(&self, job: StoreJobCreate) -> Result<StoreJob> {
        let url = format!("{}/store/job", self.base_url);
        let response = self.http.post(&url).json(&job).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::Http {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
