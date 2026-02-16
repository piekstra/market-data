use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use market_data_core::store::CandleStore;
use market_data_providers::alpaca::AlpacaProvider;
use market_data_providers::provider::CandleProvider;
use market_data_providers::yahoo::YahooProvider;
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    name = "market-data",
    about = "Populate and manage 5-minute candle data"
)]
struct Cli {
    /// Root directory for data storage (default: current directory)
    #[arg(long, default_value = ".")]
    data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Populate candle data for symbols
    Populate {
        /// Symbols to fetch (comma-separated)
        #[arg(short, long, value_delimiter = ',', required = true)]
        symbols: Vec<String>,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start: NaiveDate,

        /// End date (YYYY-MM-DD, defaults to yesterday)
        #[arg(long)]
        end: Option<NaiveDate>,

        /// Data provider: alpaca, yahoo
        #[arg(long, default_value = "alpaca")]
        provider: String,

        /// Force re-download even if data exists
        #[arg(long)]
        force: bool,
    },

    /// Show what data exists in the store
    Status {
        /// Filter by symbol (shows all if omitted)
        #[arg(short, long)]
        symbol: Option<String>,
    },

    /// Validate Parquet files and report issues
    Validate {
        /// Symbols to validate (all if omitted, comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        symbols: Option<Vec<String>>,
    },
}

fn create_provider(name: &str) -> Result<Box<dyn CandleProvider>> {
    match name {
        "alpaca" => Ok(Box::new(
            AlpacaProvider::from_env().context("failed to create Alpaca provider")?,
        )),
        "yahoo" => Ok(Box::new(YahooProvider::new())),
        other => anyhow::bail!("unknown provider: {other}. Expected: alpaca, yahoo"),
    }
}

/// Find contiguous date ranges from a sorted list of dates.
/// Groups consecutive weekdays together to minimize API calls.
fn contiguous_ranges(dates: &[NaiveDate]) -> Vec<(NaiveDate, NaiveDate)> {
    if dates.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut range_start = dates[0];
    let mut prev = dates[0];

    for &date in &dates[1..] {
        // Allow gaps of up to 3 days (weekend + possible holiday)
        // to keep ranges together and reduce API calls
        let gap = (date - prev).num_days();
        if gap > 4 {
            ranges.push((range_start, prev));
            range_start = date;
        }
        prev = date;
    }
    ranges.push((range_start, prev));
    ranges
}

async fn cmd_populate(
    store: &CandleStore,
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
    provider_name: &str,
    force: bool,
) -> Result<()> {
    let provider = create_provider(provider_name)?;
    info!("Using provider: {}", provider.name());

    for symbol in symbols {
        let symbol = symbol.to_uppercase();
        let dates_to_fetch = if force {
            market_data_core::trading_calendar::weekdays(start, end)
        } else {
            store.missing_dates(&symbol, start, end)
        };

        if dates_to_fetch.is_empty() {
            info!("{symbol}: all data present, skipping");
            continue;
        }

        info!(
            "{symbol}: {} missing date(s) from {} to {}",
            dates_to_fetch.len(),
            dates_to_fetch.first().unwrap(),
            dates_to_fetch.last().unwrap(),
        );

        // Group missing dates into contiguous ranges for efficient bulk fetching
        let ranges = contiguous_ranges(&dates_to_fetch);
        info!("{symbol}: fetching in {} range(s)", ranges.len());

        for (range_start, range_end) in &ranges {
            match provider
                .fetch_candles_range(&symbol, *range_start, *range_end)
                .await
            {
                Ok(day_groups) => {
                    let mut days_written = 0;
                    let mut total_candles = 0;
                    for (date, candles) in &day_groups {
                        if candles.is_empty() {
                            continue;
                        }
                        store
                            .write_day(&symbol, *date, candles)
                            .with_context(|| format!("failed to write {symbol} {date}"))?;
                        days_written += 1;
                        total_candles += candles.len();
                    }
                    info!(
                        "{symbol}: {range_start} to {range_end}: wrote {total_candles} candle(s) across {days_written} day(s)"
                    );
                }
                Err(e) => {
                    warn!("{symbol}: {range_start} to {range_end}: fetch failed: {e}");
                }
            }
        }
    }

    Ok(())
}

fn cmd_status(store: &CandleStore, symbol: Option<&str>) -> Result<()> {
    let symbols = match symbol {
        Some(s) => vec![s.to_uppercase()],
        None => store.list_symbols().context("failed to list symbols")?,
    };

    if symbols.is_empty() {
        println!("No data in store.");
        return Ok(());
    }

    for sym in &symbols {
        let dates = store
            .list_dates(sym)
            .with_context(|| format!("failed to list dates for {sym}"))?;

        if dates.is_empty() {
            println!("{sym}: no data");
            continue;
        }

        let first = dates.first().unwrap();
        let last = dates.last().unwrap();
        println!("{sym}: {} day(s), {first} to {last}", dates.len());
    }

    Ok(())
}

fn cmd_validate(store: &CandleStore, symbols: Option<&[String]>) -> Result<()> {
    let all_symbols = store.list_symbols().context("failed to list symbols")?;

    let symbols_to_check: Vec<&str> = match symbols {
        Some(list) => list.iter().map(|s| s.as_str()).collect(),
        None => all_symbols.iter().map(|s| s.as_str()).collect(),
    };

    if symbols_to_check.is_empty() {
        println!("No data to validate.");
        return Ok(());
    }

    let mut issues = 0;

    for sym in &symbols_to_check {
        let dates = store
            .list_dates(sym)
            .with_context(|| format!("failed to list dates for {sym}"))?;

        for date in &dates {
            match store.read_day(sym, *date) {
                Ok(candles) => {
                    if candles.is_empty() {
                        println!("WARN: {sym} {date}: empty file");
                        issues += 1;
                        continue;
                    }

                    // Check timestamp ordering
                    for i in 1..candles.len() {
                        if candles[i].timestamp <= candles[i - 1].timestamp {
                            println!(
                                "WARN: {sym} {date}: timestamps not strictly ascending at index {i}"
                            );
                            issues += 1;
                            break;
                        }
                    }

                    // Check volume
                    let zero_volume = candles.iter().filter(|c| c.volume == 0).count();
                    if zero_volume > 0 {
                        println!("WARN: {sym} {date}: {zero_volume} candle(s) with zero volume");
                        issues += 1;
                    }
                }
                Err(e) => {
                    println!("ERROR: {sym} {date}: failed to read: {e}");
                    issues += 1;
                }
            }
        }
    }

    if issues == 0 {
        println!("All files valid.");
    } else {
        println!("{issues} issue(s) found.");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    let store = CandleStore::new(&cli.data_dir);

    match &cli.command {
        Commands::Populate {
            symbols,
            start,
            end,
            provider,
            force,
        } => {
            let end_date = end
                .unwrap_or_else(|| (chrono::Utc::now() - chrono::Duration::days(1)).date_naive());
            cmd_populate(&store, symbols, *start, end_date, provider, *force).await?;
        }
        Commands::Status { symbol } => {
            cmd_status(&store, symbol.as_deref())?;
        }
        Commands::Validate { symbols } => {
            cmd_validate(&store, symbols.as_deref())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_populate_args() {
        let cli = Cli::try_parse_from([
            "market-data",
            "populate",
            "-s",
            "AAPL,MSFT",
            "--start",
            "2025-01-01",
            "--end",
            "2025-01-31",
            "--provider",
            "yahoo",
        ])
        .unwrap();

        match cli.command {
            Commands::Populate {
                symbols,
                start,
                end,
                provider,
                force,
            } => {
                assert_eq!(symbols, vec!["AAPL", "MSFT"]);
                assert_eq!(start, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
                assert_eq!(end, Some(NaiveDate::from_ymd_opt(2025, 1, 31).unwrap()));
                assert_eq!(provider, "yahoo");
                assert!(!force);
            }
            _ => panic!("expected Populate command"),
        }
    }

    #[test]
    fn parse_status_args() {
        let cli = Cli::try_parse_from(["market-data", "status", "-s", "AAPL"]).unwrap();
        match cli.command {
            Commands::Status { symbol } => {
                assert_eq!(symbol, Some("AAPL".to_string()));
            }
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn parse_status_no_symbol() {
        let cli = Cli::try_parse_from(["market-data", "status"]).unwrap();
        match cli.command {
            Commands::Status { symbol } => {
                assert!(symbol.is_none());
            }
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn parse_validate_args() {
        let cli = Cli::try_parse_from(["market-data", "validate", "-s", "AAPL,MSFT"]).unwrap();
        match cli.command {
            Commands::Validate { symbols } => {
                assert_eq!(symbols, Some(vec!["AAPL".to_string(), "MSFT".to_string()]));
            }
            _ => panic!("expected Validate command"),
        }
    }

    #[test]
    fn parse_populate_force() {
        let cli = Cli::try_parse_from([
            "market-data",
            "populate",
            "-s",
            "AAPL",
            "--start",
            "2025-01-01",
            "--force",
        ])
        .unwrap();

        match cli.command {
            Commands::Populate { force, .. } => {
                assert!(force);
            }
            _ => panic!("expected Populate command"),
        }
    }

    #[test]
    fn parse_populate_defaults() {
        let cli = Cli::try_parse_from([
            "market-data",
            "populate",
            "-s",
            "AAPL",
            "--start",
            "2025-01-01",
        ])
        .unwrap();

        match cli.command {
            Commands::Populate {
                provider,
                end,
                force,
                ..
            } => {
                assert_eq!(provider, "alpaca");
                assert!(end.is_none());
                assert!(!force);
            }
            _ => panic!("expected Populate command"),
        }
    }
}
