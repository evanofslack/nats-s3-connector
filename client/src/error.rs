use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    
    #[error("HTTP error {status}: {message}")]
    Http { status: u16, message: String },
    
    #[error("Failed to deserialize response: {0}")]
    Deserialization(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
