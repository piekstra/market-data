use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use market_data_core::candle::Candle;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::error::ProviderError;
use crate::provider::CandleProvider;

const ALPACA_DATA_BASE_URL: &str = "https://data.alpaca.markets/v2";

/// Alpaca market data provider.
/// Authenticates via APCA-API-KEY-ID and APCA-API-SECRET-KEY headers.
pub struct AlpacaProvider {
    client: Client,
    api_key_id: String,
    api_secret_key: String,
    base_url: String,
}

impl AlpacaProvider {
    /// Create from environment variables `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY`.
    pub fn from_env() -> Result<Self, ProviderError> {
        let api_key_id = std::env::var("ALPACA_API_KEY_ID")
            .map_err(|_| ProviderError::Config("ALPACA_API_KEY_ID not set".into()))?;
        let api_secret_key = std::env::var("ALPACA_API_SECRET_KEY")
            .map_err(|_| ProviderError::Config("ALPACA_API_SECRET_KEY not set".into()))?;

        Ok(Self {
            client: Client::new(),
            api_key_id,
            api_secret_key,
            base_url: ALPACA_DATA_BASE_URL.to_string(),
        })
    }

    /// Create with explicit credentials and optional base URL override.
    pub fn new(api_key_id: String, api_secret_key: String, base_url: Option<String>) -> Self {
        Self {
            client: Client::new(),
            api_key_id,
            api_secret_key,
            base_url: base_url.unwrap_or_else(|| ALPACA_DATA_BASE_URL.to_string()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct AlpacaBarsResponse {
    bars: Option<Vec<AlpacaBar>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlpacaBar {
    /// Timestamp in RFC3339 format
    t: String,
    /// Open price
    o: Decimal,
    /// High price
    h: Decimal,
    /// Low price
    l: Decimal,
    /// Close price
    c: Decimal,
    /// Volume
    v: i64,
}

impl AlpacaBar {
    fn to_candle(&self) -> Result<Candle, ProviderError> {
        let timestamp = chrono::DateTime::parse_from_rfc3339(&self.t)
            .map_err(|e| ProviderError::Parse(format!("invalid timestamp '{}': {e}", self.t)))?
            .with_timezone(&Utc);

        Ok(Candle {
            timestamp,
            open: self.o,
            high: self.h,
            low: self.l,
            close: self.c,
            volume: self.v,
        })
    }
}

#[async_trait]
impl CandleProvider for AlpacaProvider {
    fn name(&self) -> &str {
        "alpaca"
    }

    async fn fetch_candles(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Candle>, ProviderError> {
        let start = Utc
            .from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
            .to_rfc3339();
        let end = Utc
            .from_utc_datetime(&date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap())
            .to_rfc3339();

        let mut all_candles = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .get(format!("{}/stocks/{}/bars", self.base_url, symbol))
                .header("APCA-API-KEY-ID", &self.api_key_id)
                .header("APCA-API-SECRET-KEY", &self.api_secret_key)
                .query(&[
                    ("timeframe", "5Min"),
                    ("start", &start),
                    ("end", &end),
                    ("adjustment", "split"),
                    ("feed", "iex"),
                    ("limit", "10000"),
                ]);

            if let Some(token) = &page_token {
                request = request.query(&[("page_token", token.as_str())]);
            }

            let response = request.send().await?;

            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(60);
                return Err(ProviderError::RateLimited {
                    retry_after_secs: retry_after,
                });
            }

            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                return Err(ProviderError::Api {
                    status,
                    message: body,
                });
            }

            let body: AlpacaBarsResponse = response
                .json()
                .await
                .map_err(|e| ProviderError::Parse(format!("failed to parse response: {e}")))?;

            if let Some(bars) = body.bars {
                for bar in &bars {
                    all_candles.push(bar.to_candle()?);
                }
            }

            match body.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => break,
            }
        }

        all_candles.sort_by_key(|c| c.timestamp);
        Ok(all_candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parse_alpaca_bar() {
        let bar = AlpacaBar {
            t: "2025-01-15T14:30:00Z".to_string(),
            o: dec!(150.12),
            h: dec!(151.50),
            l: dec!(149.00),
            c: dec!(150.99),
            v: 1000,
        };

        let candle = bar.to_candle().unwrap();
        assert_eq!(candle.open, dec!(150.12));
        assert_eq!(candle.high, dec!(151.50));
        assert_eq!(candle.low, dec!(149.00));
        assert_eq!(candle.close, dec!(150.99));
        assert_eq!(candle.volume, 1000);
        assert_eq!(
            candle.timestamp,
            Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap()
        );
    }

    #[test]
    fn parse_alpaca_response_json() {
        let json = r#"{
            "bars": [
                {"t": "2025-01-15T14:30:00Z", "o": 150.12, "h": 151.50, "l": 149.00, "c": 150.99, "v": 1000},
                {"t": "2025-01-15T14:35:00Z", "o": 150.99, "h": 152.00, "l": 150.50, "c": 151.75, "v": 2000}
            ],
            "next_page_token": null
        }"#;

        let response: AlpacaBarsResponse = serde_json::from_str(json).unwrap();
        let bars = response.bars.unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].v, 1000);
        assert_eq!(bars[1].v, 2000);
        assert!(response.next_page_token.is_none());
    }

    #[test]
    fn parse_alpaca_response_empty() {
        let json = r#"{"bars": null, "next_page_token": null}"#;
        let response: AlpacaBarsResponse = serde_json::from_str(json).unwrap();
        assert!(response.bars.is_none());
    }

    #[test]
    fn parse_alpaca_response_with_pagination() {
        let json = r#"{
            "bars": [
                {"t": "2025-01-15T14:30:00Z", "o": 150.12, "h": 151.50, "l": 149.00, "c": 150.99, "v": 1000}
            ],
            "next_page_token": "abc123"
        }"#;

        let response: AlpacaBarsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.next_page_token.as_deref(), Some("abc123"));
    }
}
