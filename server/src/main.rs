use anyhow::{Error, Result};
use std::path::PathBuf;

use clap::Parser;
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

    // create app
    let app = app::new(config.clone()).await?;

    // Restart existing jobs
    app.start_store_jobs().await?;
    app.start_load_jobs().await?;

    // Thread periodically cleaning up async threads
    app.cleanup_completed_job_tasks();

    // start server
    app.server.serve().await;

    Ok(())
}
