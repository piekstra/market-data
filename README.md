# market-data

Parquet-backed store for 5-minute stock market candle data, designed for backtesting.

## Overview

This workspace provides:

- **`market-data-core`** — Core library with `Candle` type, `CandleStore` API, and Parquet I/O. This is the primary dependency for consuming data.
- **`market-data-providers`** — Data fetching from Alpaca and Yahoo Finance APIs.
- **`market-data-cli`** — CLI tool for populating and managing data.

## Data Layout

```
data/{SYMBOL}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
```

One Parquet file per symbol per trading day. Each file contains 5-minute OHLCV candles with columns: `timestamp` (UTC microseconds), `open`, `high`, `low`, `close` (decimal strings), `volume` (i64).

## Quick Start

### Populate data

```bash
# Using Alpaca (requires ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY env vars)
cargo run -p market-data-cli -- populate -s AAPL,MSFT --start 2025-01-01

# Using Yahoo Finance (no auth needed, limited to ~60 days)
cargo run -p market-data-cli -- populate -s AAPL --start 2025-12-01 --provider yahoo

# Force re-download existing data
cargo run -p market-data-cli -- populate -s AAPL --start 2025-01-01 --end 2025-01-31 --force
```

The populate command is idempotent — it checks what data already exists and only fetches missing dates.

### Check status

```bash
cargo run -p market-data-cli -- status
cargo run -p market-data-cli -- status -s AAPL
```

### Validate data

```bash
cargo run -p market-data-cli -- validate
```

### Use as a library

Add to your `Cargo.toml`:

```toml
market-data-core = { path = "../market-data/crates/market-data-core" }
```

```rust
use market_data_core::store::CandleStore;
use market_data_core::session::Session;
use chrono::NaiveDate;

let store = CandleStore::new("../market-data");

// Read a date range
let candles = store.read_range("AAPL",
    NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
    NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
)?;

// Filter by session
let regular_hours = store.read_range_session("AAPL",
    NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
    NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
    Session::Regular,
)?;

// Check what's available
let symbols = store.list_symbols()?;
let dates = store.list_dates("AAPL")?;
let missing = store.missing_dates("AAPL",
    NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
    NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
);
```

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```
ALPACA_API_KEY_ID=your_key_id
ALPACA_API_SECRET_KEY=your_secret_key
```

## Development

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Versioning

Semver. All crates share the workspace version. Releases are triggered by git tags (`v0.1.0`).
