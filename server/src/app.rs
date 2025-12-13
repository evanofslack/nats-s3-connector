use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db;
use crate::db::JobStoreError;
use crate::io;
use crate::jobs;
use crate::metrics;
use crate::nats;
use crate::s3;
use crate::server;

#[derive(Debug, Clone)]
pub struct App {
    pub db: db::DynJobStorer,
    pub io: io::IO,
    pub server: server::Server,
    pub registry: Arc<jobs::JobRegistry>,
}

// construct a new instance of nats3 application
pub async fn new(config: Config) -> Result<App> {
    debug!("create new application from config");

    let metrics = metrics::Metrics::new().await;

    let pg_store = db::PostgresStore::new(&config.postgres.url)
        .await
        .context("fail create postgres store")?;
    if config.postgres.migrate {
        pg_store.migrate().await.context("fail run migrations")?;
    }
    let job_db: db::DynJobStorer = Arc::new(pg_store.clone());
    let chunk_db: db::DynChunkStorer = Arc::new(pg_store);

    let s3_client = s3::Client::new(
        config.s3.region.clone(),
        config.s3.endpoint.clone(),
        config.s3.access_key.clone(),
        config.s3.secret_key.clone(),
    );

    let nats_client = nats::Client::new(config.nats.url.clone())
        .await
        .context("fail connect to nats server")?;

    let io = io::IO::new(metrics.clone(), s3_client, nats_client, chunk_db);
    let registry = Arc::new(jobs::JobRegistry::new());

    let server = server::Server::new(
        config.clone().server.addr,
        metrics.clone(),
        io.clone(),
        job_db.clone(),
        registry.clone(),
    );

    let app = App {
        io,
        server,
        db: job_db,
        registry,
    };

    Ok(app)
}

impl App {
    // start all store jobs already saved in database
    pub async fn start_store_jobs(&self) -> Result<(), JobStoreError> {
        let store_jobs = self.db.clone().get_store_jobs().await?;
        if store_jobs.is_empty() {
            return Ok(());
        }
        info!(
            job_count = store_jobs.len(),
            "start existing store jobs from database"
        );
        for job in store_jobs {
            if self.registry.is_store_job_running(&job.id).await {
                warn!(
                    job_id = job.id,
                    "skip start existing store job, already registered"
                );
                continue;
            }
            // must clone the instances we pass to the async thread
            let app = self.clone();
            let job = job.clone();
            let config = io::ConsumeConfig {
                stream: job.stream.clone(),
                consumer: job.consumer.clone(),
                subject: job.subject.clone(),
                bucket: job.bucket.clone(),
                prefix: job.prefix,
                bytes_max: job.batch.max_bytes,
                messages_max: job.batch.max_count,
                codec: job.encoding.codec,
            };
            let registry_config = config.clone();
            let handle: tokio::task::JoinHandle<Result<()>> =
                tokio::spawn(async move { app.io.consume_stream(config).await });
            self.registry
                .try_register_store_job(job.id, handle, registry_config)
                .await;
        }
        Ok(())
    }

    // start all load jobs already saved in database
    pub async fn start_load_jobs(&self) -> Result<(), JobStoreError> {
        let load_jobs = self.db.clone().get_load_jobs().await?;
        if load_jobs.is_empty() {
            return Ok(());
        }
        info!(
            job_count = load_jobs.len(),
            "start existing load jobs from database"
        );
        for job in load_jobs {
            if self.registry.is_load_job_running(&job.id).await {
                warn!(
                    job_id = job.id,
                    "skip start existing load job, already registered"
                );
                continue;
            }

            // must clone the instances we pass to the async thread
            let app = self.clone();
            let job = job.clone();
            let config = io::PublishConfig {
                read_stream: job.read_stream,
                read_consumer: job.read_consumer,
                read_subject: job.read_subject,
                write_subject: job.write_subject,
                bucket: job.bucket.clone(),
                prefix: job.prefix,
                delete_chunks: job.delete_chunks,
                start: job.start,
                end: job.end,
            };
            let registry_config = config.clone();
            let handle: tokio::task::JoinHandle<Result<()>> =
                tokio::spawn(async move { app.io.publish_stream(config).await });
            self.registry
                .try_register_load_job(job.id, handle, registry_config)
                .await;
        }
        Ok(())
    }

    pub fn cleanup_completed_job_tasks(&self) {
        let registry = self.registry.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = registry.cleanup_completed_jobs().await {
                    warn!(error = e.to_string(), "fail cleanup completed jobs");
                }
            }
        });
    }
}
