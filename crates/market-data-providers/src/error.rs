use chrono::NaiveDate;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("No data available for {symbol} on {date}")]
    NoData { symbol: String, date: NaiveDate },

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Rate limited, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    #[error("Provider configuration error: {0}")]
    Config(String),

    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },
}
