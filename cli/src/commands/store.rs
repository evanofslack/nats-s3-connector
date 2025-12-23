use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;
use nats3_client::Client;
use nats3_types::{Batch, Codec, Encoding, StoreJobCreate};
use std::path::PathBuf;

use crate::{config::OutputFormat, interactive, output};

#[derive(Subcommand, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum StoreCommand {
    List,
    Create {
        #[arg(short, long)]
        interactive: bool,

        /// Load job definition from json file (use '-' for stdin)
        #[arg(long, value_name = "FILE")]
        from_json: Option<PathBuf>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        name: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        stream: Option<String>,

        #[arg(long)]
        consumer: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        subject: Option<String>,

        #[arg(long, required_unless_present_any = ["interactive", "from_json"])]
        bucket: Option<String>,

        #[arg(long)]
        prefix: Option<String>,

        #[arg(long)]
        batch_max_bytes: Option<i64>,

        #[arg(long)]
        batch_max_count: Option<i64>,

        #[arg(long, value_parser = clap::value_parser!(Codec))]
        codec: Option<Codec>,
    },
    Pause {
        #[arg(short, long)]
        interactive: bool,

        #[arg(long, required_unless_present_any = ["interactive"])]
        job_id: Option<String>,
    },
    Resume {
        #[arg(short, long)]
        interactive: bool,

        #[arg(long, required_unless_present_any = ["interactive"])]
        job_id: Option<String>,
    },
    Delete {
        #[arg(short, long)]
        interactive: bool,

        #[arg(long, required_unless_present_any = ["interactive"])]
        job_id: Option<String>,
    },
}

impl StoreCommand {
    pub async fn execute(self, client: &Client, output_format: &OutputFormat) -> Result<()> {
        match self {
            StoreCommand::List => {
                let jobs = client
                    .get_store_jobs()
                    .await
                    .context("Fail fetch store jobs")?;
                output::print_store_jobs(jobs, output_format)?;
            }
            StoreCommand::Create {
                interactive,
                from_json,
                name,
                stream,
                consumer,
                subject,
                bucket,
                prefix,
                batch_max_bytes,
                batch_max_count,
                codec,
            } => {
                let job = if interactive {
                    interactive::prompt_create_store_job()?
                } else if let Some(path) = from_json {
                    load_from_json(&path)?
                } else {
                    let batch = match (batch_max_bytes, batch_max_count) {
                        (None, None) => Batch::default(),
                        _ => Batch {
                            max_bytes: batch_max_bytes.unwrap_or(1_000_000),
                            max_count: batch_max_count.unwrap_or(1000),
                        },
                    };
                    let encoding = codec.map_or(Encoding::default(), |c| Encoding { codec: c });

                    StoreJobCreate {
                        name: name.unwrap(),
                        stream: stream.unwrap(),
                        consumer,
                        subject: subject.unwrap(),
                        bucket: bucket.unwrap(),
                        prefix,
                        batch,
                        encoding,
                    }
                };

                let created = client
                    .create_store_job(job)
                    .await
                    .context("Fail create store job")?;
                output::print_store_job(created, output_format)?;
            }
            StoreCommand::Pause {
                interactive,
                mut job_id,
            } => {
                if interactive {
                    job_id = Some(interactive::prompt_job_id()?);
                };
                if job_id.is_none() {
                    println!("{}", "Must provide job id to pause store job".red());
                    return Ok(());
                }

                client
                    .pause_store_job(job_id.expect("job id is set"))
                    .await
                    .context("Fail pause store job")?;

                println!("{}", "Store job paused successfully!".green());
            }
            StoreCommand::Resume {
                interactive,
                mut job_id,
            } => {
                if interactive {
                    job_id = Some(interactive::prompt_job_id()?);
                };
                if job_id.is_none() {
                    println!("{}", "Must provide job id to resume store job".red());
                    return Ok(());
                }

                client
                    .resume_store_job(job_id.expect("job id is set"))
                    .await
                    .context("Fail resume store job")?;

                println!("{}", "Store job resumed successfully!".green());
            }
            StoreCommand::Delete {
                interactive,
                mut job_id,
            } => {
                if interactive {
                    job_id = Some(interactive::prompt_job_id()?);
                };
                if job_id.is_none() {
                    println!("{}", "Must provide job id to delete store job".red());
                    return Ok(());
                }

                client
                    .delete_store_job(job_id.expect("job id is set"))
                    .await
                    .context("Fail delete store job")?;

                println!("{}", "Store job deleted successfully!".green());
            }
        }
        Ok(())
    }
}

fn load_from_json(path: &PathBuf) -> Result<StoreJobCreate> {
    let content = if path.to_str() == Some("-") {
        std::io::read_to_string(std::io::stdin())?
    } else {
        std::fs::read_to_string(path)?
    };
    serde_json::from_str(&content).context("Fail parse json")
}
