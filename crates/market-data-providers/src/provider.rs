use async_trait::async_trait;
use chrono::NaiveDate;
use market_data_core::candle::Candle;

use crate::error::ProviderError;

/// Trait for fetching 5-minute candle data from an external source.
#[async_trait]
pub trait CandleProvider: Send + Sync {
    /// Provider name (for logging/display).
    fn name(&self) -> &str;

    /// Fetch 5-minute candles for a symbol on a specific date.
    /// Returns candles sorted by timestamp.
    /// Returns an empty vec if the date is not a trading day.
    async fn fetch_candles(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Candle>, ProviderError>;
}
