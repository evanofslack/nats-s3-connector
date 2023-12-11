use anyhow::{anyhow, Error, Result};
use figment::{
    providers::{Env, Format, Toml, Yaml},
    Figment,
};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::PathBuf;
use tracing_subscriber::filter::LevelFilter;

const DEFAULT_CONFIG_PATH: &'static str = "/etc/nats3/config.toml";
const DEFAULT_SERVER_ADDR: &'static str = "0.0.0.0:8080";
const DEFAULT_MAX_BYTES: i64 = 1_000_000;
const DEFAULT_MAX_COUNT: i64 = 1000;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub log: Option<String>,
    pub server: Server,
    pub nats: Nats,
    pub s3: S3,
    pub store: Option<Vec<Store>>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Server {
    #[serde(default = "addr_default")]
    pub addr: String,
}

fn addr_default() -> String {
    DEFAULT_SERVER_ADDR.to_string()
}

#[derive(Deserialize, Clone, Debug)]
pub struct Nats {
    pub url: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct S3 {
    pub endpoint: String,
    pub region: String,
    #[serde(rename = "secret")]
    pub secret_key: String,
    #[serde(rename = "access")]
    pub access_key: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Store {
    pub name: String,
    pub stream: String,
    pub subject: String,
    pub bucket: String,
    pub batch: Batch,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Batch {
    #[serde(default = "max_bytes_default")]
    pub max_bytes: i64,
    #[serde(default = "max_count_default")]
    pub max_count: i64,
}

fn max_bytes_default() -> i64 {
    DEFAULT_MAX_BYTES
}

fn max_count_default() -> i64 {
    DEFAULT_MAX_COUNT
}

impl Config {
    pub fn load(path: Option<PathBuf>) -> Result<Self, Error> {
        let path = path.unwrap_or(PathBuf::from(DEFAULT_CONFIG_PATH));
        let figment = Figment::new();
        let figment = match path.extension().and_then(OsStr::to_str) {
            Some("toml") => figment.merge(Toml::file(path)),
            Some("yaml") => figment.merge(Yaml::file(path)),
            Some(ext) => return Err(anyhow!("unexpected file extension '{}'", ext)),
            None => return Err(anyhow!("failed to parse path")),
        };

        let config: Config = figment.join(Env::prefixed("NATS3_").split("_")).extract()?;
        return Ok(config);
    }

    pub fn log_level(&self) -> LevelFilter {
        match self
            .log
            .to_owned()
            .unwrap_or_else(|| "INFO".to_string())
            .to_uppercase()
            .as_str()
        {
            "TRACE" => LevelFilter::TRACE,
            "DEBUG" => LevelFilter::DEBUG,
            "ERROR" => LevelFilter::ERROR,
            "INFO" => LevelFilter::INFO,
            _ => LevelFilter::INFO,
        }
    }
}
