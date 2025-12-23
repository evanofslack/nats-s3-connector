use anyhow::{Context, Result};
use nats3_types::{ListLoadJobsQuery, ListStoreJobsQuery, LoadJobStatus, StoreJobStatus};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    completer::TaskCompleter, config::Config, coordinator, db, error, io, metrics, nats, registry,
    s3, server, shutdown::ShutdownCoordinator,
};

#[derive(Debug, Clone)]
pub struct App {
    pub db: db::DynJobStorer,
    pub coordinator: coordinator::Coordinator,
    pub server: server::Server,
    pub registry: Arc<registry::Registry>,
    pub completer: TaskCompleter,
}

// construct a new instance of nats3 application
pub async fn new(config: Config, shutdown: ShutdownCoordinator) -> Result<App> {
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

    let registry_token = shutdown.subscribe();
    let registry = Arc::new(registry::Registry::new(registry_token));

    let coordinator = coordinator::Coordinator::new(registry.clone(), io.clone(), job_db.clone());

    let server = server::Server::new(
        config.clone().server.addr,
        metrics.clone(),
        job_db.clone(),
        coordinator.clone(),
    );
    let completer = TaskCompleter::new(job_db.clone(), registry.clone());

    let app = App {
        coordinator,
        server,
        db: job_db,
        registry,
        completer,
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
            // TODO: a little wasteful, multiple DB lookups
            self.coordinator.resume_store_job(job.id).await?;
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
            // TODO: a little wasteful, multple DB reads
            self.coordinator.resume_load_job(job.id).await?;
        }
        Ok(())
    }

    pub fn start_task_completer(&self, shutdown_token: CancellationToken) {
        self.completer.clone().start(shutdown_token);
    }
}
