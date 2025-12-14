use anyhow::{Context, Result};
use nats3_types::{ListLoadJobsQuery, ListStoreJobsQuery, LoadJobStatus, StoreJobStatus};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::{config::Config, coordinator, db, error, io, metrics, nats, registry, s3, server};

#[derive(Debug, Clone)]
pub struct App {
    pub db: db::DynJobStorer,
    pub coordinator: coordinator::Coordinator,
    pub server: server::Server,
    pub registry: Arc<registry::Registry>,
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
    let registry = Arc::new(registry::Registry::new()); // need to pass the callback here

    let coordinator = coordinator::Coordinator::new(registry.clone(), io.clone(), job_db.clone());

    let server = server::Server::new(
        config.clone().server.addr,
        metrics.clone(),
        job_db.clone(),
        coordinator.clone(),
    );

    let app = App {
        coordinator,
        server,
        db: job_db,
        registry,
    };

    Ok(app)
}

impl App {
    // start all store jobs (not in terminal state) already saved in database
    pub async fn start_store_jobs(&self) -> Result<(), error::AppError> {
        let query = ListStoreJobsQuery::new()
            .with_statuses(vec![StoreJobStatus::Running, StoreJobStatus::Created]);
        let store_jobs = self.db.clone().get_store_jobs(Some(query)).await?;
        if store_jobs.is_empty() {
            return Ok(());
        }
        info!(
            job_count = store_jobs.len(),
            "start existing store jobs from database"
        );

        for job in store_jobs {
            let config: io::ConsumeConfig = job.clone().into();
            self.coordinator.restart_store_job(job, config).await?;
        }
        Ok(())
    }

    // start all load jobs (that are not in terminal state) already saved in database
    pub async fn start_load_jobs(&self) -> Result<(), error::AppError> {
        let query = ListLoadJobsQuery::new()
            .with_statuses(vec![LoadJobStatus::Running, LoadJobStatus::Created]);
        let load_jobs = self.db.clone().get_load_jobs(Some(query)).await?;
        if load_jobs.is_empty() {
            return Ok(());
        }
        info!(
            job_count = load_jobs.len(),
            "start existing load jobs from database"
        );
        for job in load_jobs {
            let config: io::PublishConfig = job.clone().into();
            self.coordinator.restart_load_job(job, config).await?;
        }
        Ok(())
    }

    pub fn cleanup_completed_job_tasks(&self) {
        let registry = self.registry.clone();
        let job_handler = Arc::new(self.coordinator.clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = registry
                    .cleanup_completed_jobs(job_handler.clone(), job_handler.clone())
                    .await
                {
                    warn!(error = e.to_string(), "fail cleanup completed jobs");
                }
            }
        });
    }
}
