use anyhow::{Error, Result};
use clap::Parser;
use std::path::PathBuf;
use tokio::signal;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod app;
mod config;
mod coordinator;
mod db;
mod encoding;
mod error;
mod io;
mod metrics;
mod nats;
mod registry;
mod s3;
mod server;
mod shutdown;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// path to the config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    // load config from path
    let config = config::Config::load(args.config)?;

    // init tracing
    let _ = FmtSubscriber::builder()
        .with_max_level(config.log_level())
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let shutdown = shutdown::ShutdownCoordinator::new();

    // create app
    let app = app::new(config.clone(), shutdown.clone()).await?;

    // Restart existing jobs
    app.start_store_jobs().await?;
    app.start_load_jobs().await?;

    // Thread periodically cleaning up async threads
    let cleanup_token = shutdown.subscribe();
    app.cleanup_completed_job_tasks(cleanup_token);

    // start server
    let server_token = shutdown.subscribe();
    let server_handle = {
        let server = app.server.clone();
        tokio::spawn(async move {
            server.serve(server_token).await;
        })
    };

    wait_for_shutdown_signal().await;

    info!("start graceful shutdown");
    shutdown.shutdown();

    debug!(timeout = 30, "wait for jobs to complete");
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        app.registry.wait_for_all_jobs(),
    )
    .await
    {
        Ok(Ok(())) => {
            debug!("all jobs completed successfully");
        }
        Ok(Err(e)) => {
            warn!(error = ?e, "error wait for jobs");
        }
        Err(_) => {
            warn!("timeout wait for jobs to complete");
        }
    }

    let _ = server_handle.await;
    info!("shutdown complete");
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            debug!("received Ctrl+C signal");
        },
        _ = terminate => {
            debug!("received SIGTERM signal");
        },
    }
}
