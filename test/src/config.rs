use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "nats3-test")]
#[command(about = "E2E test for nats3")]
pub struct Config {
    #[arg(long, default_value = "100")]
    pub num_messages: usize,

    #[arg(long, default_value = "1024")]
    pub message_size: usize,

    #[arg(long, default_value = "10")]
    pub idle_timeout: u64,

    #[arg(long)]
    pub bucket: Option<String>,

    #[arg(long, default_value = "test-input")]
    pub input_stream: String,

    #[arg(long, default_value = "test-output")]
    pub output_stream: String,

    #[arg(long, default_value = "test.input")]
    pub input_subject: String,

    #[arg(long, default_value = "test.output")]
    pub output_subject: String,

    #[arg(long, default_value = None)]
    pub poll_interval_sec: Option<u64>,

    #[arg(long, default_value = "nats://localhost:4222")]
    pub nats_url: String,

    #[arg(long, default_value = "http://localhost:9000")]
    pub s3_endpoint: String,

    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, default_value = "test-user")]
    pub s3_access_key: String,

    #[arg(long, default_value = "test-password")]
    pub s3_secret_key: String,

    #[arg(long, default_value = "http://localhost:8080")]
    pub nats3_url: String,

    #[arg(long)]
    pub store_job_name: Option<String>,

    #[arg(long)]
    pub load_job_name: Option<String>,

    #[arg(long, default_value = "false")]
    pub skip_setup: bool,

    #[arg(long, default_value = "false")]
    pub skip_store: bool,

    #[arg(long, default_value = "false")]
    pub skip_load: bool,

    #[arg(long, default_value = "info")]
    pub log_level: String,

    #[arg(long, default_value = "false")]
    pub header_random: bool,

    #[arg(long, default_value = "false")]
    pub header_none: bool,

    #[arg(long, default_value = "256")]
    pub header_length: usize,
}

impl Config {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub fn bucket_name(&self) -> String {
        self.bucket
            .clone()
            .unwrap_or_else(|| format!("test-bucket-{}", chrono::Utc::now().timestamp()))
    }

    pub fn store_job_name(&self) -> String {
        self.store_job_name
            .clone()
            .unwrap_or_else(|| format!("test-store-{}", chrono::Utc::now().timestamp()))
    }

    pub fn load_job_name(&self) -> String {
        self.load_job_name
            .clone()
            .unwrap_or_else(|| format!("test-load-{}", chrono::Utc::now().timestamp()))
    }
}
