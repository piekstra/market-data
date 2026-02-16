use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
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

    /// Fetch 5-minute candles for a symbol across a date range (inclusive).
    /// Returns candles grouped by date, sorted by timestamp within each group.
    /// Providers should override this for efficient batch fetching.
    async fn fetch_candles_range(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<(NaiveDate, Vec<Candle>)>, ProviderError> {
        // Default: fetch day by day
        let mut results = Vec::new();
        let mut current = start;
        while current <= end {
            if current.weekday().number_from_monday() <= 5 {
                let candles = self.fetch_candles(symbol, current).await?;
                if !candles.is_empty() {
                    results.push((current, candles));
                }
            }
            current = current.succ_opt().unwrap();
        }
        Ok(results)
    }
}
