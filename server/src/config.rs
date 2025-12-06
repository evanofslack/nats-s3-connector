use anyhow::{anyhow, Error, Result};
use figment::{
    providers::{Env, Format, Toml, Yaml},
    Figment,
};
use nats3_types::StoreJob;
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::string::ToString;
use tracing_subscriber::filter::LevelFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/nats3/config.toml";
const DEFAULT_SERVER_ADDR: &str = "0.0.0.0:8080";

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub log: Option<String>,
    pub server: Server,
    pub postgres: Option<Postgres>,
    pub nats: Nats,
    pub s3: S3,
    pub store_jobs: Option<Vec<StoreJob>>,
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
pub struct Postgres {
    pub url: String,
    pub migrate: bool,
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
        Ok(config)
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
