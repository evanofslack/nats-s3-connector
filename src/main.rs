use anyhow::{Error, Result};
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::FmtSubscriber;

mod app;
mod config;
mod encoding;
mod nats;
mod s3;
mod server;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to the config file
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
        .try_init();

    // create app
    let app = app::new(config.clone()).await?;

    // start all store jobs
    app.start_store_jobs().await;

    // start server
    let server = server::Server::new(
        config.server.addr.expect("always have addr"),
        app.s3_client,
        app.nats_client,
    )?;
    server.serve().await;

    Ok(())
}
