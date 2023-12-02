use anyhow::{Error, Result};
use figment::{
    providers::{Env, Format, Toml, Yaml},
    Figment,
};
use serde::Deserialize;
use tracing_subscriber::filter::LevelFilter;

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
    /// Utility function for parsing the log level from the configuration.
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

pub fn load() -> Result<Config, Error> {
    let config: Config = Figment::new()
        .merge(Toml::file("config.toml"))
        .merge(Env::prefixed("NATSS3_"))
        .join(Yaml::file("config.yaml"))
        .extract()?;
    return Ok(config);
}
