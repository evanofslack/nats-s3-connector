use anyhow::Result;
use serde::{Deserialize, Serialize};

const APP_NAME: &str = "nats3";
const CONFIG_NAME: &str = "config";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub server_url: String,
    #[serde(default)]
    pub output_format: OutputFormat,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:8080".to_string(),
            output_format: OutputFormat::Table,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
}

pub fn load_config() -> Result<Config> {
    confy::load(APP_NAME, CONFIG_NAME).map_err(|e| {
        match confy::get_configuration_file_path(APP_NAME, CONFIG_NAME) {
            Ok(path) => anyhow::anyhow!("Fail load config from {}: {}", path.display(), e),
            Err(path_err) => anyhow::anyhow!(
                "Fail load config and couldn't determine config path: {} (path error: {})",
                e,
                path_err
            ),
        }
    })
}

pub fn save_config(config: &Config) -> Result<()> {
    confy::store(APP_NAME, CONFIG_NAME, config).map_err(
        |e| match confy::get_configuration_file_path(APP_NAME, CONFIG_NAME) {
            Ok(path) => anyhow::anyhow!("Fail save config to {}: {}", path.display(), e),
            Err(path_err) => anyhow::anyhow!(
                "Fail save config and couldn't determine config path: {} (path error: {})",
                e,
                path_err
            ),
        },
    )
}
