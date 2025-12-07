use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;

use crate::config::{load_config, save_config, OutputFormat};

#[derive(Subcommand, Clone)]
pub enum ConfigCommand {
    Show,
    Set {
        #[arg(value_enum)]
        key: ConfigKey,
        value: String,
    },
    Get {
        #[arg(value_enum)]
        key: ConfigKey,
    },
}

#[derive(clap::ValueEnum, Clone)]
pub enum ConfigKey {
    ServerUrl,
    OutputFormat,
}

impl ConfigCommand {
    pub fn execute(self) -> Result<()> {
        match self {
            ConfigCommand::Show => {
                let cfg = load_config()?;
                println!("{}", "Current configuration:".blue().bold());
                println!("  server_url: {}", cfg.server_url);
                println!("  output_format: {:?}", cfg.output_format);
            }
            ConfigCommand::Set { key, value } => {
                let mut cfg = load_config()?;
                match key {
                    ConfigKey::ServerUrl => {
                        cfg.server_url = value.clone();
                        println!("{} server_url = {}", "Set".green(), value);
                    }
                    ConfigKey::OutputFormat => {
                        cfg.output_format = match value.to_lowercase().as_str() {
                            "table" => OutputFormat::Table,
                            "json" => OutputFormat::Json,
                            _ => anyhow::bail!("Invalid output format. Use 'table' or 'json'"),
                        };
                        println!("{} output_format = {}", "Set".green(), value);
                    }
                }
                save_config(&cfg)?;
            }
            ConfigCommand::Get { key } => {
                let cfg = load_config()?;
                match key {
                    ConfigKey::ServerUrl => println!("{}", cfg.server_url),
                    ConfigKey::OutputFormat => println!("{:?}", cfg.output_format),
                }
            }
        }
        Ok(())
    }
}
