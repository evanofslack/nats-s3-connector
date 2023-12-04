use anyhow::{anyhow, Error, Result};
use figment::{
    providers::{Env, Format, Toml, Yaml},
    Figment,
};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::PathBuf;
use tracing_subscriber::filter::LevelFilter;

const DEFAULT_CONFIG_PATH: &'static str = "/etc/nats-s3-connector/config.toml";

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub log: Option<String>,
    pub server: Server,
    pub nats: Nats,
    pub s3: S3,
    pub store: Option<Vec<Store>>,
    pub load: Option<Vec<Load>>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Server {
    #[serde(default = "addr_default")]
    pub addr: Option<String>,
}

fn addr_default() -> Option<String> {
    Some("0.0.0.0:8080".to_string())
}

#[derive(Deserialize, Clone, Debug)]
pub struct Nats {
    pub url: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct S3 {
    pub endpoint: String,
    pub region: String,
    pub secret_key: String,
    pub access_key: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Store {
    pub name: String,
    pub stream: String,
    pub subject: String,
    pub bucket: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Load {
    pub name: String,
    pub stream: String,
    pub subject: String,
    pub bucket: String,
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

        let config: Config = figment
            .join(Env::prefixed("NATS_S3_CONNECTOR_").split("_"))
            .extract()?;
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
