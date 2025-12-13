use std::sync::Arc;

use crate::config::Config;
use crate::db;
use crate::db::JobStoreError;
use crate::io;
use crate::metrics;
use crate::nats;
use crate::s3;
use crate::server;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct App {
    pub db: db::DynJobStorer,
    pub io: io::IO,
    pub server: server::Server,
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

    let server = server::Server::new(
        config.clone().server.addr,
        metrics.clone(),
        io.clone(),
        job_db.clone(),
    );

    let app = App {
        io,
        server,
        db: job_db,
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
            // must clone the instances we pass to the async thread
            let app = self.clone();
            let job = job.clone();
            tokio::spawn(async move {
                if let Err(err) = app
                    .io
                    .consume_stream(io::ConsumeConfig {
                        stream: job.stream.clone(),
                        consumer: job.consumer.clone(),
                        subject: job.subject.clone(),
                        bucket: job.bucket.clone(),
                        prefix: job.prefix,
                        bytes_max: job.batch.max_bytes,
                        messages_max: job.batch.max_count,
                        codec: job.encoding.codec,
                    })
                    .await
                {
                    warn!(id = job.id, error = err.to_string(), "store job terminated");
                }
            });
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
            // must clone the instances we pass to the async thread
            let app = self.clone();
            let job = job.clone();
            tokio::spawn(async move {
                if let Err(err) = app
                    .io
                    .publish_stream(io::PublishConfig {
                        read_stream: job.read_stream,
                        read_consumer: job.read_consumer,
                        read_subject: job.read_subject,
                        write_subject: job.write_subject,
                        bucket: job.bucket.clone(),
                        prefix: job.prefix,
                        delete_chunks: job.delete_chunks,
                        start: job.start,
                        end: job.end,
                    })
                    .await
                {
                    warn!(id = job.id, error = err.to_string(), "store job terminated");
                }
            });
        }
        Ok(())
    }
}
