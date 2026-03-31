use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("DSN parse error: {0}")]
    DsnParse(String),

    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("ClickHouse query error: {0}")]
    ClickHouse(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("CDC error: {0}")]
    #[allow(dead_code)]
    Cdc(String),

    #[error("Cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, ReplicatorError>;
