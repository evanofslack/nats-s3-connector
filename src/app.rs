use std::sync::Arc;

use crate::config::Config;
use crate::db;
use crate::io;
use crate::metrics;
use crate::nats;
use crate::s3;
use crate::server;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

// nats3 application
#[derive(Debug, Clone)]
pub struct App {
    pub config: Arc<Config>,
    pub db: db::DynStorer,
    pub io: io::IO,
    pub server: server::Server,
}

// construct a new instance of nats3 application
pub async fn new(config: Config) -> Result<App> {
    debug!("creating new application from config");

    let metrics = metrics::Metrics::new().await;

    // TODO: switch store based on config
    // let db: db::DynStorer = Arc::new(db::inmem::InMemory::new(metrics.clone()));
    let db: db::DynStorer = match &config.database_url {
        Some(url) => {
            info!(url = url, "initializing postgres store");
            let pg_store = db::PostgresStore::new(url)
                .await
                .context("fail create postgres store")?;
            pg_store.migrate().await.context("fail run migrations")?;
            Arc::new(pg_store)
        }
        None => {
            info!("using in-memory store");
            Arc::new(db::inmem::InMemory::new(metrics.clone()))
        }
    };

    let s3_client = s3::Client::new(
        config.s3.region.clone(),
        config.s3.endpoint.clone(),
        config.s3.access_key.clone(),
        config.s3.secret_key.clone(),
    );

    let nats_client = nats::Client::new(config.nats.url.clone())
        .await
        .context("failed to connect to nats server")?;

    let io = io::IO::new(metrics.clone(), s3_client, nats_client);

    let server = server::Server::new(
        config.clone().server.addr,
        metrics.clone(),
        io.clone(),
        db.clone(),
    );

    let app = App {
        config: Arc::new(config),
        io,
        server,
        db,
    };

    Ok(app)
}

impl App {
    // start all store jobs as defined in config
    pub async fn start_store_jobs(&self) {
        if let Some(store_jobs) = self.config.clone().store_jobs.clone() {
            info!("starting up {} store jobs", store_jobs.len());
            for job in store_jobs.iter() {
                // must clone the instances we pass to the async thread
                let app = self.clone();
                let job = job.clone();
                tokio::spawn(async move {
                    if let Err(err) = app.db.create_store_job(job.clone()).await {
                        warn!("{err}");
                    }

                    if let Err(err) = app
                        .io
                        .consume_stream(io::ConsumeConfig {
                            stream: job.stream.clone(),
                            subject: job.subject.clone(),
                            bucket: job.bucket.clone(),
                            prefix: job.prefix,
                            bytes_max: job.batch.max_bytes,
                            messages_max: job.batch.max_count,
                            codec: job.encoding.codec,
                        })
                        .await
                    {
                        warn!("{}", err);
                        warn!(id = job.id, "store job terminated with error: {err}");
                        app.io
                            .metrics
                            .jobs
                            .write()
                            .await
                            .store_jobs
                            .get_or_create(&metrics::JobLabels {
                                stream: job.stream.clone(),
                                subject: job.subject.clone(),
                                bucket: job.bucket.clone(),
                            })
                            .dec();
                    }
                });
            }
        }
    }
}
