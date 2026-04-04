use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("DSN parse error: {0}")]
    DsnParse(String),

    #[error("HTTP request error: {0}")]
    Http(String),

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

impl From<reqwest::Error> for ReplicatorError {
    fn from(e: reqwest::Error) -> Self {
        ReplicatorError::Http(mask_password(&e.to_string()))
    }
}

/// Replace `password=<value>` with `password=***` in a string (e.g. URLs in reqwest errors).
fn mask_password(s: &str) -> String {
    let marker = "password=";
    let Some(start) = s.find(marker) else {
        return s.to_string();
    };
    let value_start = start + marker.len();
    let value_end = s[value_start..]
        .find(['&', ')', ' '])
        .map(|i| value_start + i)
        .unwrap_or(s.len());
    format!("{}{}***{}", &s[..value_start], "", &s[value_end..])
}
