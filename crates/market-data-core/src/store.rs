use std::path::{Path, PathBuf};

use chrono::{NaiveDate, NaiveTime};

use crate::candle::Candle;
use crate::error::MarketDataError;
use crate::schema;
use crate::session::Session;
use crate::trading_calendar;

/// Filesystem-backed store for 5-minute candle data in Parquet format.
///
/// Directory layout: `{root}/data/{SYMBOL}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet`
pub struct CandleStore {
    data_dir: PathBuf,
}

impl CandleStore {
    /// Create a store rooted at the given directory.
    /// The `data/` subdirectory is used automatically.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            data_dir: root.as_ref().join("data"),
        }
    }

    /// Create a store pointing directly at the data directory (no `data/` suffix).
    pub fn from_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    /// Path to the Parquet file for a given symbol and date.
    pub fn file_path(&self, symbol: &str, date: NaiveDate) -> PathBuf {
        self.data_dir
            .join(symbol)
            .join(date.format("%Y").to_string())
            .join(date.format("%m").to_string())
            .join(format!("{}.parquet", date.format("%Y-%m-%d")))
    }

    /// Check if data exists for a symbol on a given date.
    pub fn has_data(&self, symbol: &str, date: NaiveDate) -> bool {
        self.file_path(symbol, date).exists()
    }

    /// Find which weekdays in a range are missing data for a symbol.
    pub fn missing_dates(&self, symbol: &str, start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        trading_calendar::weekdays(start, end)
            .into_iter()
            .filter(|d| !self.has_data(symbol, *d))
            .collect()
    }

    /// Write candles for a single date to a Parquet file.
    /// Creates parent directories as needed. Overwrites if file already exists.
    pub fn write_day(
        &self,
        symbol: &str,
        date: NaiveDate,
        candles: &[Candle],
    ) -> Result<(), MarketDataError> {
        let path = self.file_path(symbol, date);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        schema::write_parquet(&path, candles)
    }

    /// Read all candles for a symbol on a specific date.
    pub fn read_day(&self, symbol: &str, date: NaiveDate) -> Result<Vec<Candle>, MarketDataError> {
        let path = self.file_path(symbol, date);
        if !path.exists() {
            return Err(MarketDataError::NoData {
                symbol: symbol.to_string(),
                date,
            });
        }
        schema::read_parquet(&path)
    }

    /// Read candles for a symbol across a date range (inclusive).
    /// Returns candles sorted by timestamp. Skips dates without data.
    pub fn read_range(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<Candle>, MarketDataError> {
        let dates = trading_calendar::weekdays(start, end);
        let mut all_candles = Vec::new();

        for date in dates {
            if self.has_data(symbol, date) {
                let mut candles = schema::read_parquet(&self.file_path(symbol, date))?;
                all_candles.append(&mut candles);
            }
        }

        all_candles.sort_by_key(|c| c.timestamp);
        Ok(all_candles)
    }

    /// Read candles filtered by session type.
    pub fn read_range_session(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        session: Session,
    ) -> Result<Vec<Candle>, MarketDataError> {
        let candles = self.read_range(symbol, start, end)?;
        Ok(candles
            .into_iter()
            .filter(|c| Session::classify(&c.timestamp) == Some(session))
            .collect())
    }

    /// Read candles for a specific date within a time range (UTC).
    pub fn read_time_range(
        &self,
        symbol: &str,
        date: NaiveDate,
        start_time: NaiveTime,
        end_time: NaiveTime,
    ) -> Result<Vec<Candle>, MarketDataError> {
        let candles = self.read_day(symbol, date)?;
        Ok(candles
            .into_iter()
            .filter(|c| {
                let time = c.timestamp.time();
                time >= start_time && time <= end_time
            })
            .collect())
    }

    /// List all symbols that have data in the store.
    pub fn list_symbols(&self) -> Result<Vec<String>, MarketDataError> {
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }

        let mut symbols = Vec::new();
        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                symbols.push(name.to_string());
            }
        }
        symbols.sort();
        Ok(symbols)
    }

    /// List all dates with data for a given symbol, sorted ascending.
    pub fn list_dates(&self, symbol: &str) -> Result<Vec<NaiveDate>, MarketDataError> {
        let symbol_dir = self.data_dir.join(symbol);
        if !symbol_dir.exists() {
            return Ok(Vec::new());
        }

        let mut dates = Vec::new();

        // Walk year directories
        for year_entry in std::fs::read_dir(&symbol_dir)? {
            let year_entry = year_entry?;
            if !year_entry.file_type()?.is_dir() {
                continue;
            }

            // Walk month directories
            for month_entry in std::fs::read_dir(year_entry.path())? {
                let month_entry = month_entry?;
                if !month_entry.file_type()?.is_dir() {
                    continue;
                }

                // Walk parquet files
                for file_entry in std::fs::read_dir(month_entry.path())? {
                    let file_entry = file_entry?;
                    let file_name = file_entry.file_name();
                    let name = file_name.to_string_lossy();
                    if let Some(date_str) = name.strip_suffix(".parquet")
                        && let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                    {
                        dates.push(date);
                    }
                }
            }
        }

        dates.sort();
        Ok(dates)
    }

    /// Get the date range (earliest, latest) for a symbol, or None if no data.
    pub fn date_range(
        &self,
        symbol: &str,
    ) -> Result<Option<(NaiveDate, NaiveDate)>, MarketDataError> {
        let dates = self.list_dates(symbol)?;
        Ok(dates.first().copied().zip(dates.last().copied()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike, Utc};
    use rust_decimal_macros::dec;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn make_candle(hour: u32, min: u32) -> Candle {
        Candle {
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, hour, min, 0).unwrap(),
            open: dec!(150.00),
            high: dec!(151.00),
            low: dec!(149.00),
            close: dec!(150.50),
            volume: 1000,
        }
    }

    fn make_candles_for_date(year: i32, month: u32, day: u32) -> Vec<Candle> {
        vec![
            Candle {
                timestamp: Utc.with_ymd_and_hms(year, month, day, 14, 30, 0).unwrap(),
                open: dec!(150.00),
                high: dec!(151.00),
                low: dec!(149.00),
                close: dec!(150.50),
                volume: 1000,
            },
            Candle {
                timestamp: Utc.with_ymd_and_hms(year, month, day, 14, 35, 0).unwrap(),
                open: dec!(150.50),
                high: dec!(152.00),
                low: dec!(150.00),
                close: dec!(151.00),
                volume: 2000,
            },
        ]
    }

    #[test]
    fn file_path_format() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        let path = store.file_path("AAPL", date(2025, 1, 15));
        let expected = dir.path().join("data/AAPL/2025/01/2025-01-15.parquet");
        assert_eq!(path, expected);
    }

    #[test]
    fn has_data_false_before_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        assert!(!store.has_data("AAPL", date(2025, 1, 15)));
    }

    #[test]
    fn write_and_read_day() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        let candles = make_candles_for_date(2025, 1, 15);

        store
            .write_day("AAPL", date(2025, 1, 15), &candles)
            .unwrap();
        assert!(store.has_data("AAPL", date(2025, 1, 15)));

        let result = store.read_day("AAPL", date(2025, 1, 15)).unwrap();
        assert_eq!(result, candles);
    }

    #[test]
    fn read_day_missing_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        let result = store.read_day("AAPL", date(2025, 1, 15));
        assert!(result.is_err());
    }

    #[test]
    fn missing_dates_returns_weekdays_without_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        // Write data for Wed Jan 15
        store
            .write_day(
                "AAPL",
                date(2025, 1, 15),
                &make_candles_for_date(2025, 1, 15),
            )
            .unwrap();

        // Mon-Fri Jan 13-17: should be missing Mon, Tue, Thu, Fri
        let missing = store.missing_dates("AAPL", date(2025, 1, 13), date(2025, 1, 17));
        assert_eq!(
            missing,
            vec![
                date(2025, 1, 13),
                date(2025, 1, 14),
                date(2025, 1, 16),
                date(2025, 1, 17),
            ]
        );
    }

    #[test]
    fn read_range_multiple_days() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        let candles_15 = make_candles_for_date(2025, 1, 15);
        let candles_16 = make_candles_for_date(2025, 1, 16);

        store
            .write_day("AAPL", date(2025, 1, 15), &candles_15)
            .unwrap();
        store
            .write_day("AAPL", date(2025, 1, 16), &candles_16)
            .unwrap();

        let result = store
            .read_range("AAPL", date(2025, 1, 15), date(2025, 1, 16))
            .unwrap();
        assert_eq!(result.len(), 4);
        // Should be sorted by timestamp
        for i in 1..result.len() {
            assert!(result[i].timestamp >= result[i - 1].timestamp);
        }
    }

    #[test]
    fn read_range_skips_missing_dates() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        let candles = make_candles_for_date(2025, 1, 15);
        store
            .write_day("AAPL", date(2025, 1, 15), &candles)
            .unwrap();

        // Range includes Jan 13-17 but only Jan 15 has data
        let result = store
            .read_range("AAPL", date(2025, 1, 13), date(2025, 1, 17))
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn read_range_session_filters() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        // 14:30 UTC = 9:30 ET (Regular), 21:00 UTC = 16:00 ET (AfterHours)
        let candles = vec![
            make_candle(14, 30), // 9:30 ET = Regular
            Candle {
                timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 21, 0, 0).unwrap(),
                open: dec!(150.00),
                high: dec!(151.00),
                low: dec!(149.00),
                close: dec!(150.50),
                volume: 500,
            }, // 16:00 ET = AfterHours
        ];

        store
            .write_day("AAPL", date(2025, 1, 15), &candles)
            .unwrap();

        let regular = store
            .read_range_session(
                "AAPL",
                date(2025, 1, 15),
                date(2025, 1, 15),
                Session::Regular,
            )
            .unwrap();
        assert_eq!(regular.len(), 1);
        assert_eq!(regular[0].timestamp.hour(), 14); // 14:30 UTC

        let after = store
            .read_range_session(
                "AAPL",
                date(2025, 1, 15),
                date(2025, 1, 15),
                Session::AfterHours,
            )
            .unwrap();
        assert_eq!(after.len(), 1);
    }

    #[test]
    fn read_time_range_filters() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        let candles = vec![
            make_candle(14, 30),
            make_candle(15, 0),
            make_candle(15, 30),
            make_candle(16, 0),
        ];

        store
            .write_day("AAPL", date(2025, 1, 15), &candles)
            .unwrap();

        let result = store
            .read_time_range(
                "AAPL",
                date(2025, 1, 15),
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
            )
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn list_symbols() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        store
            .write_day(
                "AAPL",
                date(2025, 1, 15),
                &make_candles_for_date(2025, 1, 15),
            )
            .unwrap();
        store
            .write_day(
                "MSFT",
                date(2025, 1, 15),
                &make_candles_for_date(2025, 1, 15),
            )
            .unwrap();

        let symbols = store.list_symbols().unwrap();
        assert_eq!(symbols, vec!["AAPL", "MSFT"]);
    }

    #[test]
    fn list_symbols_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        let symbols = store.list_symbols().unwrap();
        assert!(symbols.is_empty());
    }

    #[test]
    fn list_dates_and_date_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        store
            .write_day(
                "AAPL",
                date(2025, 1, 15),
                &make_candles_for_date(2025, 1, 15),
            )
            .unwrap();
        store
            .write_day(
                "AAPL",
                date(2025, 1, 16),
                &make_candles_for_date(2025, 1, 16),
            )
            .unwrap();
        store
            .write_day("AAPL", date(2025, 2, 3), &make_candles_for_date(2025, 2, 3))
            .unwrap();

        let dates = store.list_dates("AAPL").unwrap();
        assert_eq!(
            dates,
            vec![date(2025, 1, 15), date(2025, 1, 16), date(2025, 2, 3)]
        );

        let range = store.date_range("AAPL").unwrap();
        assert_eq!(range, Some((date(2025, 1, 15), date(2025, 2, 3))));
    }

    #[test]
    fn date_range_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        assert_eq!(store.date_range("AAPL").unwrap(), None);
    }

    #[test]
    fn write_day_creates_directories() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());

        store
            .write_day(
                "AAPL",
                date(2025, 1, 15),
                &make_candles_for_date(2025, 1, 15),
            )
            .unwrap();

        assert!(dir.path().join("data/AAPL/2025/01").exists());
    }

    #[test]
    fn write_day_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(dir.path());
        let d = date(2025, 1, 15);

        let candles1 = make_candles_for_date(2025, 1, 15);
        store.write_day("AAPL", d, &candles1).unwrap();

        let candles2 = vec![make_candles_for_date(2025, 1, 15)[0].clone()];
        store.write_day("AAPL", d, &candles2).unwrap();

        let result = store.read_day("AAPL", d).unwrap();
        assert_eq!(result.len(), 1);
    }
}
