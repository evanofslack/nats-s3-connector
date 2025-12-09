mod config;
mod message;
mod setup;
mod test;
mod verify;

use config::Config;
use tracing::Level;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse_args();
    
    init_logging(&config.log_level);

    if !config.skip_setup {
        setup::run(&config).await?;
    }

    let results = test::run(&config).await?;

    results.print();

    if results.success {
        Ok(())
    } else {
        std::process::exit(1)
    }
}

fn init_logging(level: &str) {
    let level = match level {
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .without_time()
        .init();
}
