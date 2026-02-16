use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single 5-minute OHLCV candle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candle {
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
}
