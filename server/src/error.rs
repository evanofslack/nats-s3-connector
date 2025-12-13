use thiserror::Error;

use crate::db;
use crate::registry;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("job store error: {0}")]
    JobStore(#[from] db::JobStoreError),
    #[error("job registry error: {0}")]
    JobRegistry(#[from] registry::RegistryError),
}
