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

    // TODO: switch store based on config
    let db: db::DynStorer = Arc::new(db::inmem::InMemory::new());

    let s3_client = s3::Client::new(
        config.s3.region.clone(),
        config.s3.endpoint.clone(),
        config.s3.access_key.clone(),
        config.s3.secret_key.clone(),
    );

    let nats_client = nats::Client::new(config.nats.url.clone())
        .await
        .context("failed to connect to nats server")?;

    let metrics = metrics::Metrics::new().await;

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

    return Ok(app);
}

impl App {
    // start all store jobs as defined in config
    pub async fn start_store_jobs(&self) {
        if let Some(stores) = self.config.clone().store.clone() {
            info!("starting up {} store jobs", stores.len());
            for store in stores.iter() {
                // must clone the instances we pass to the async thread
                let app = self.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(err) = app
                        .io
                        .consume_stream(
                            store.stream.clone(),
                            store.subject.clone(),
                            store.bucket.clone(),
                            store.prefix,
                            store.batch.max_bytes,
                            store.batch.max_count,
                            store.encoding.codec,
                        )
                        .await
                    {
                        warn!("{}", err);
                    }
                });
            }
        }
    }
}
