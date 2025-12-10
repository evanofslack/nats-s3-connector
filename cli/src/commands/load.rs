use anyhow::{Context, Result};
use clap::Subcommand;
use nats3_client::Client;
use nats3_types::CreateLoadJob;
use std::path::PathBuf;

use crate::{config::OutputFormat, interactive, output};

#[derive(Subcommand, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum LoadCommand {
    List,
    Create {
        #[arg(short, long)]
        interactive: bool,

        /// Load job definition from JSON file (use '-' for stdin)
        #[arg(long, value_name = "FILE")]
        from_json: Option<PathBuf>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        bucket: Option<String>,

        #[arg(long)]
        prefix: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        read_stream: Option<String>,

        #[arg(long)]
        read_consumer: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        read_subject: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        write_subject: Option<String>,

        #[arg(long)]
        delete_chunks: bool,

        #[arg(long)]
        start: Option<usize>,

        #[arg(long)]
        end: Option<usize>,
    },
}

impl LoadCommand {
    pub async fn execute(self, client: &Client, output_format: &OutputFormat) -> Result<()> {
        match self {
            LoadCommand::List => {
                let jobs = client
                    .get_load_jobs()
                    .await
                    .context("Fail fetch load jobs")?;
                output::print_load_jobs(jobs, output_format)?;
            }
            LoadCommand::Create {
                interactive,
                from_json,
                bucket,
                prefix,
                read_stream,
                read_consumer,
                read_subject,
                write_subject,
                delete_chunks,
                start,
                end,
            } => {
                let job = if interactive {
                    interactive::prompt_create_load_job()?
                } else if let Some(path) = from_json {
                    load_from_json(&path)?
                } else {
                    CreateLoadJob {
                        bucket: bucket.unwrap(),
                        prefix,
                        read_stream: read_stream.unwrap(),
                        read_consumer,
                        read_subject: read_subject.unwrap(),
                        write_subject: write_subject.unwrap(),
                        delete_chunks,
                        start,
                        end,
                    }
                };

                let created = client
                    .create_load_job(job)
                    .await
                    .context("Fail create load job")?;
                output::print_load_job(created, output_format)?;
            }
        }
        Ok(())
    }
}

fn load_from_json(path: &PathBuf) -> Result<CreateLoadJob> {
    let content = if path.to_str() == Some("-") {
        std::io::read_to_string(std::io::stdin())?
    } else {
        std::fs::read_to_string(path)?
    };
    serde_json::from_str(&content).context("Fail parse json")
}
