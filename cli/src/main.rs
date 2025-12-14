mod commands;
mod config;
mod interactive;
mod output;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use commands::{config::ConfigCommand, load::LoadCommand, store::StoreCommand};
use nats3_client::Client;

#[derive(Parser)]
#[command(name = "nats3")]
#[command(about = "cli for interacting with nats3 server", long_about = None)]
struct Cli {
    /// Server url (overides config)
    #[arg(long, global = true)]
    server: Option<String>,

    /// Output format (table or json)
    #[arg(long, global = true, value_parser = ["table", "json"])]
    output: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Clone)]
enum Command {
    Load {
        #[command(subcommand)]
        command: LoadCommand,
    },
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{}\n{:#}", "Error:".red().bold(), err);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Command::Config { command } => command.clone().execute(),
        Command::Load { .. } | Command::Store { .. } => {
            let (client, output_format) = setup_client_and_format(&cli)?;

            match cli.command {
                Command::Load { command } => command.execute(&client, &output_format).await,
                Command::Store { command } => command.execute(&client, &output_format).await,
                _ => unreachable!(),
            }
        }
    }
}

fn setup_client_and_format(cli: &Cli) -> Result<(Client, config::OutputFormat)> {
    let cfg = config::load_config().context("Fail to load config")?;

    let server_url = cli.server.as_ref().unwrap_or(&cfg.server_url);
    let client = Client::new(server_url.clone());

    let output_format = cli
        .output
        .as_deref()
        .map(|fmt| match fmt {
            "json" => config::OutputFormat::Json,
            _ => config::OutputFormat::Table,
        })
        .unwrap_or(cfg.output_format);

    Ok((client, output_format))
}
