use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use market_data_core::candle::Candle;
use reqwest::Client;
use rust_decimal::Decimal;
use tracing::{debug, info};

use crate::error::ProviderError;
use crate::provider::CandleProvider;

const CBOE_BASE_URL: &str = "https://cdn.cboe.com/api/global/us_indices/daily_prices";

/// Supported CBOE index symbols and their CSV file names.
fn csv_filename(symbol: &str) -> Option<&'static str> {
    match symbol.to_uppercase().as_str() {
        "VIX" | "^VIX" => Some("VIX_History.csv"),
        "VVIX" | "^VVIX" => Some("VVIX_History.csv"),
        "VIX9D" | "^VIX9D" => Some("VIX9D_History.csv"),
        "OVX" | "^OVX" => Some("OVX_History.csv"),
        "GVZ" | "^GVZ" => Some("GVZ_History.csv"),
        _ => None,
    }
}

/// CBOE historical data provider.
/// Downloads free daily OHLC CSV data for CBOE volatility indices (VIX, VVIX, etc.).
/// No authentication required. Data goes back to 1990 for VIX.
/// Note: CBOE data is daily only (no intraday) and has no volume.
pub struct CboeProvider {
    client: Client,
    base_url: String,
}

impl CboeProvider {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("failed to build reqwest client"),
            base_url: CBOE_BASE_URL.to_string(),
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

    /// Download and parse the full CSV for a CBOE index.
    async fn fetch_csv(&self, symbol: &str) -> Result<Vec<CboeRow>, ProviderError> {
        let filename = csv_filename(symbol).ok_or_else(|| {
            ProviderError::Config(format!(
                "unsupported CBOE symbol: '{symbol}'. Supported: VIX, VVIX, VIX9D, OVX, GVZ"
            ))
        })?;

        let url = format!("{}/{}", self.base_url, filename);
        debug!("Fetching CBOE CSV from {url}");

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ProviderError::Api {
                status,
                message: body,
            });
        }

        let text = response.text().await?;
        parse_cboe_csv(&text)
    }
}

impl Default for CboeProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// A parsed row from the CBOE CSV.
#[derive(Debug, Clone)]
struct CboeRow {
    date: NaiveDate,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

impl CboeRow {
    fn to_candle(&self) -> Candle {
        // Use market open (14:30 UTC = 9:30 ET) as the timestamp for daily bars
        let timestamp = Utc.from_utc_datetime(&self.date.and_hms_opt(14, 30, 0).unwrap());
        Candle {
            timestamp,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: 0, // CBOE daily data has no volume
        }
    }
}

/// Parse the CBOE CSV format.
/// Header: `DATE,OPEN,HIGH,LOW,CLOSE`
/// Date format: `MM/DD/YYYY`
/// Prices: decimal with up to 6 decimal places
fn parse_cboe_csv(text: &str) -> Result<Vec<CboeRow>, ProviderError> {
    let mut rows = Vec::new();
    let mut lines = text.lines();

    // Skip header
    let header = lines
        .next()
        .ok_or_else(|| ProviderError::Parse("empty CSV".into()))?;

    // Validate header
    let header_upper = header.to_uppercase().replace(' ', "");
    if !header_upper.contains("DATE") || !header_upper.contains("CLOSE") {
        return Err(ProviderError::Parse(format!(
            "unexpected CSV header: '{header}'"
        )));
    }

    for (line_num, line) in lines.enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 5 {
            return Err(ProviderError::Parse(format!(
                "line {}: expected 5 fields, got {}",
                line_num + 2,
                fields.len()
            )));
        }

        let date = NaiveDate::parse_from_str(fields[0].trim(), "%m/%d/%Y").map_err(|e| {
            ProviderError::Parse(format!(
                "line {}: invalid date '{}': {e}",
                line_num + 2,
                fields[0]
            ))
        })?;

        let open: Decimal = fields[1].trim().parse().map_err(|e| {
            ProviderError::Parse(format!(
                "line {}: invalid open '{}': {e}",
                line_num + 2,
                fields[1]
            ))
        })?;

        let high: Decimal = fields[2].trim().parse().map_err(|e| {
            ProviderError::Parse(format!(
                "line {}: invalid high '{}': {e}",
                line_num + 2,
                fields[2]
            ))
        })?;

        let low: Decimal = fields[3].trim().parse().map_err(|e| {
            ProviderError::Parse(format!(
                "line {}: invalid low '{}': {e}",
                line_num + 2,
                fields[3]
            ))
        })?;

        let close: Decimal = fields[4].trim().parse().map_err(|e| {
            ProviderError::Parse(format!(
                "line {}: invalid close '{}': {e}",
                line_num + 2,
                fields[4]
            ))
        })?;

        rows.push(CboeRow {
            date,
            open,
            high,
            low,
            close,
        });
    }

    Ok(rows)
}

#[async_trait]
impl CandleProvider for CboeProvider {
    fn name(&self) -> &str {
        "cboe"
    }

    async fn fetch_candles(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Candle>, ProviderError> {
        let rows = self.fetch_csv(symbol).await?;
        let candles: Vec<Candle> = rows
            .iter()
            .filter(|r| r.date == date)
            .map(|r| r.to_candle())
            .collect();
        Ok(candles)
    }

    /// Override for efficient bulk fetching — downloads the CSV once
    /// and groups all requested dates from it.
    async fn fetch_candles_range(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<(NaiveDate, Vec<Candle>)>, ProviderError> {
        info!("{symbol}: fetching CBOE daily data (full CSV download)");

        let rows = self.fetch_csv(symbol).await?;

        let mut by_date: BTreeMap<NaiveDate, Vec<Candle>> = BTreeMap::new();
        for row in &rows {
            if row.date >= start && row.date <= end {
                by_date.entry(row.date).or_default().push(row.to_candle());
            }
        }

        let total = by_date.len();
        debug!("{symbol}: {total} trading days in range {start} to {end}");

        Ok(by_date.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const SAMPLE_CSV: &str = "\
DATE,OPEN,HIGH,LOW,CLOSE
01/02/1990,17.240000,17.240000,17.240000,17.240000
01/03/1990,18.190000,18.190000,18.190000,18.190000
01/04/1990,19.220000,19.220000,19.220000,19.220000
02/21/2025,15.500000,16.750000,14.250000,15.820000";

    #[test]
    fn parse_csv_basic() {
        let rows = parse_cboe_csv(SAMPLE_CSV).unwrap();
        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].date, NaiveDate::from_ymd_opt(1990, 1, 2).unwrap());
        assert_eq!(rows[0].open, dec!(17.240000));
        assert_eq!(rows[0].close, dec!(17.240000));

        assert_eq!(rows[3].date, NaiveDate::from_ymd_opt(2025, 2, 21).unwrap());
        assert_eq!(rows[3].open, dec!(15.500000));
        assert_eq!(rows[3].high, dec!(16.750000));
        assert_eq!(rows[3].low, dec!(14.250000));
        assert_eq!(rows[3].close, dec!(15.820000));
    }

    #[test]
    fn parse_csv_empty_lines() {
        let csv = "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2025,20.00,21.00,19.00,20.50\n\n";
        let rows = parse_cboe_csv(csv).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn parse_csv_bad_header() {
        let csv = "bad header\n01/02/2025,20.00,21.00,19.00,20.50";
        assert!(parse_cboe_csv(csv).is_err());
    }

    #[test]
    fn parse_csv_empty() {
        assert!(parse_cboe_csv("").is_err());
    }

    #[test]
    fn parse_csv_bad_date() {
        let csv = "DATE,OPEN,HIGH,LOW,CLOSE\nNOT_A_DATE,20.00,21.00,19.00,20.50";
        assert!(parse_cboe_csv(csv).is_err());
    }

    #[test]
    fn parse_csv_bad_price() {
        let csv = "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2025,abc,21.00,19.00,20.50";
        assert!(parse_cboe_csv(csv).is_err());
    }

    #[test]
    fn to_candle_sets_timestamp_and_zero_volume() {
        let row = CboeRow {
            date: NaiveDate::from_ymd_opt(2025, 2, 21).unwrap(),
            open: dec!(15.50),
            high: dec!(16.75),
            low: dec!(14.25),
            close: dec!(15.82),
        };

        let candle = row.to_candle();
        assert_eq!(candle.open, dec!(15.50));
        assert_eq!(candle.high, dec!(16.75));
        assert_eq!(candle.low, dec!(14.25));
        assert_eq!(candle.close, dec!(15.82));
        assert_eq!(candle.volume, 0);
        // Timestamp should be 14:30 UTC on the date
        assert_eq!(
            candle.timestamp,
            Utc.with_ymd_and_hms(2025, 2, 21, 14, 30, 0).unwrap()
        );
    }

    #[test]
    fn csv_filename_mapping() {
        assert_eq!(csv_filename("VIX"), Some("VIX_History.csv"));
        assert_eq!(csv_filename("^VIX"), Some("VIX_History.csv"));
        assert_eq!(csv_filename("vix"), Some("VIX_History.csv"));
        assert_eq!(csv_filename("VVIX"), Some("VVIX_History.csv"));
        assert_eq!(csv_filename("VIX9D"), Some("VIX9D_History.csv"));
        assert_eq!(csv_filename("OVX"), Some("OVX_History.csv"));
        assert_eq!(csv_filename("GVZ"), Some("GVZ_History.csv"));
        assert_eq!(csv_filename("AAPL"), None);
    }

    #[test]
    fn decimal_precision_preserved() {
        let csv = "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2025,17.240000,18.190000,16.500000,17.890000";
        let rows = parse_cboe_csv(csv).unwrap();
        assert_eq!(rows[0].open, dec!(17.240000));
        assert_eq!(rows[0].high, dec!(18.190000));
    }
}
