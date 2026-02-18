use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use market_data_core::candle::Candle;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::error::ProviderError;
use crate::provider::CandleProvider;

const YAHOO_CHART_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";

/// Yahoo Finance market data provider.
/// No authentication required. Limited to ~60 days of intraday history.
pub struct YahooProvider {
    client: Client,
    base_url: String,
}

impl YahooProvider {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("failed to build reqwest client"),
            base_url: YAHOO_CHART_URL.to_string(),
        }
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(base_url: String) -> Self {
        Self {
            client: Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("failed to build reqwest client"),
            base_url,
        }
    }
}

impl Default for YahooProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl YahooProvider {
    /// Fetch daily OHLCV bars for a symbol over a date range (inclusive).
    /// Yahoo provides 20+ years of split-adjusted daily data with no authentication.
    /// Returns candles sorted by timestamp.
    pub async fn fetch_daily_bars(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<Candle>, ProviderError> {
        let start_ts = start.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
        let end_ts = end
            .succ_opt()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();

        let response = self
            .client
            .get(format!("{}/{}", self.base_url, symbol))
            .query(&[
                ("period1", &start_ts.to_string()),
                ("period2", &end_ts.to_string()),
                ("interval", &"1d".to_string()),
            ])
            .send()
            .await?;

        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::RateLimited {
                retry_after_secs: 60,
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

        let body: YahooResponse = response
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("failed to parse response: {e}")))?;

        if let Some(error) = body.chart.error {
            return Err(ProviderError::Api {
                status: 0,
                message: format!("{}: {}", error.code, error.description),
            });
        }

        let results = body
            .chart
            .result
            .ok_or_else(|| ProviderError::Parse("no results in response".into()))?;

        if results.is_empty() {
            return Ok(Vec::new());
        }

        let mut candles = parse_yahoo_result(&results[0])?;
        candles.sort_by_key(|c| c.timestamp);
        Ok(candles)
    }
}

#[derive(Debug, Deserialize)]
struct YahooResponse {
    chart: YahooChart,
}

#[derive(Debug, Deserialize)]
struct YahooChart {
    result: Option<Vec<YahooResult>>,
    error: Option<YahooError>,
}

#[derive(Debug, Deserialize)]
struct YahooError {
    code: String,
    description: String,
}

#[derive(Debug, Deserialize)]
struct YahooResult {
    timestamp: Option<Vec<i64>>,
    indicators: YahooIndicators,
}

#[derive(Debug, Deserialize)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
}

#[derive(Debug, Deserialize)]
struct YahooQuote {
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<i64>>,
}

fn f64_to_decimal(val: f64) -> Result<Decimal, ProviderError> {
    Decimal::try_from(val).map_err(|e| ProviderError::Parse(format!("invalid decimal value: {e}")))
}

fn parse_yahoo_result(result: &YahooResult) -> Result<Vec<Candle>, ProviderError> {
    let timestamps = result
        .timestamp
        .as_ref()
        .ok_or_else(|| ProviderError::Parse("missing timestamps".into()))?;

    if result.indicators.quote.is_empty() {
        return Ok(Vec::new());
    }

    let quote = &result.indicators.quote[0];
    let mut candles = Vec::new();

    for (i, &ts) in timestamps.iter().enumerate() {
        let open = match quote.open.get(i).copied().flatten() {
            Some(v) => f64_to_decimal(v)?,
            None => continue, // skip candles with missing data
        };
        let high = match quote.high.get(i).copied().flatten() {
            Some(v) => f64_to_decimal(v)?,
            None => continue,
        };
        let low = match quote.low.get(i).copied().flatten() {
            Some(v) => f64_to_decimal(v)?,
            None => continue,
        };
        let close = match quote.close.get(i).copied().flatten() {
            Some(v) => f64_to_decimal(v)?,
            None => continue,
        };
        let volume = quote.volume.get(i).copied().flatten().unwrap_or(0);

        let timestamp = Utc
            .timestamp_opt(ts, 0)
            .single()
            .ok_or_else(|| ProviderError::Parse(format!("invalid unix timestamp: {ts}")))?;

        candles.push(Candle {
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        });
    }

    Ok(candles)
}

#[async_trait]
impl CandleProvider for YahooProvider {
    fn name(&self) -> &str {
        "yahoo"
    }

    async fn fetch_candles(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Candle>, ProviderError> {
        let start_ts = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
        let end_ts = date
            .succ_opt()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();

        let response = self
            .client
            .get(format!("{}/{}", self.base_url, symbol))
            .query(&[
                ("period1", &start_ts.to_string()),
                ("period2", &end_ts.to_string()),
                ("interval", &"5m".to_string()),
            ])
            .send()
            .await?;

        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::RateLimited {
                retry_after_secs: 60,
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

        let body: YahooResponse = response
            .json()
            .await
            .map_err(|e| ProviderError::Parse(format!("failed to parse response: {e}")))?;

        if let Some(error) = body.chart.error {
            return Err(ProviderError::Api {
                status: 0,
                message: format!("{}: {}", error.code, error.description),
            });
        }

        let results = body
            .chart
            .result
            .ok_or_else(|| ProviderError::Parse("no results in response".into()))?;

        if results.is_empty() {
            return Ok(Vec::new());
        }

        let mut candles = parse_yahoo_result(&results[0])?;
        candles.sort_by_key(|c| c.timestamp);
        Ok(candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parse_yahoo_response_json() {
        let json = r#"{
            "chart": {
                "result": [{
                    "timestamp": [1736942400, 1736942700],
                    "indicators": {
                        "quote": [{
                            "open": [150.12, 150.99],
                            "high": [151.50, 152.00],
                            "low": [149.00, 150.50],
                            "close": [150.99, 151.75],
                            "volume": [1000, 2000]
                        }]
                    }
                }],
                "error": null
            }
        }"#;

        let response: YahooResponse = serde_json::from_str(json).unwrap();
        let results = response.chart.result.unwrap();
        let candles = parse_yahoo_result(&results[0]).unwrap();

        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].volume, 1000);
        assert_eq!(candles[1].volume, 2000);
    }

    #[test]
    fn parse_yahoo_response_with_null_values() {
        let json = r#"{
            "chart": {
                "result": [{
                    "timestamp": [1736942400, 1736942700, 1736943000],
                    "indicators": {
                        "quote": [{
                            "open": [150.12, null, 151.00],
                            "high": [151.50, null, 152.00],
                            "low": [149.00, null, 150.50],
                            "close": [150.99, null, 151.75],
                            "volume": [1000, null, 2000]
                        }]
                    }
                }],
                "error": null
            }
        }"#;

        let response: YahooResponse = serde_json::from_str(json).unwrap();
        let results = response.chart.result.unwrap();
        let candles = parse_yahoo_result(&results[0]).unwrap();

        // The null candle should be skipped
        assert_eq!(candles.len(), 2);
    }

    #[test]
    fn parse_yahoo_error_response() {
        let json = r#"{
            "chart": {
                "result": null,
                "error": {
                    "code": "Not Found",
                    "description": "No data found for symbol INVALID"
                }
            }
        }"#;

        let response: YahooResponse = serde_json::from_str(json).unwrap();
        assert!(response.chart.error.is_some());
        assert_eq!(response.chart.error.as_ref().unwrap().code, "Not Found");
    }

    #[test]
    fn parse_yahoo_daily_response() {
        // Daily bars have the same JSON structure as intraday
        let json = r#"{
            "chart": {
                "result": [{
                    "timestamp": [1609459200, 1609545600, 1609632000],
                    "indicators": {
                        "quote": [{
                            "open": [375.31, 380.12, 382.50],
                            "high": [380.00, 385.00, 390.00],
                            "low": [373.00, 378.50, 381.00],
                            "close": [378.85, 383.20, 388.75],
                            "volume": [50000000, 45000000, 55000000]
                        }]
                    }
                }],
                "error": null
            }
        }"#;

        let response: YahooResponse = serde_json::from_str(json).unwrap();
        let results = response.chart.result.unwrap();
        let candles = parse_yahoo_result(&results[0]).unwrap();

        assert_eq!(candles.len(), 3);
        assert_eq!(candles[0].volume, 50000000);
        assert_eq!(candles[2].volume, 55000000);
        // Verify prices are parsed correctly
        assert!(candles[0].close > dec!(378.0) && candles[0].close < dec!(379.0));
    }

    #[test]
    fn f64_to_decimal_converts() {
        let result = f64_to_decimal(150.12).unwrap();
        // f64 -> Decimal may have precision nuances, but should be close
        assert!(result > dec!(150.0) && result < dec!(151.0));
    }
}
