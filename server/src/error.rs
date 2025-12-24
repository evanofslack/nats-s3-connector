use nats3_types::ValidationError;
use thiserror::Error;

use crate::{db, registry};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("job store error: {0}")]
    JobStore(#[from] db::JobStoreError),
    #[error("job registry error: {0}")]
    JobRegistry(#[from] registry::RegistryError),
    #[error("config validation error: {0}")]
    Validation(#[from] ValidationError),
}
