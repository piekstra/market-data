# market-data

Parquet-backed store for 5-minute stock market candle data, designed for backtesting.

## Purpose and Principles

This repository exists to solve one problem well: **reliable, efficient access to intraday candle data for equities and ETFs.**

It is part of a modular ecosystem of independent repositories. Each repo has a focused responsibility and can be used on its own or composed with others. This repo is the data layer — it owns the storage format, the retrieval API, and the tools to populate data. It does not contain trading logic, strategy evaluation, or real-time streaming.

### Core Principles

- **Data is the product.** The Parquet files committed to this repo are first-class artifacts. Any project can clone this repo and immediately have access to historical candle data without running population scripts or configuring API keys.

- **Single canonical format.** All data is stored as daily Parquet files with a fixed schema (timestamp, OHLCV). One file per symbol per trading day. This convention is non-negotiable — it is what makes the store predictable for consumers.

- **Idempotent population.** Running the populate command is always safe. It checks what exists, fetches what's missing, and never re-downloads data that's already present. Data can be incrementally extended forward or backward in time — including fetching today's candles.

- **Historical and recent, not real-time.** This repo covers historical data and recent data up to and including today. Populate can fetch the current day's candles on demand. However, true real-time streaming (WebSocket feeds, sub-second updates) belongs in a separate service.

- **Read-optimized for Rust.** The core library (`market-data-core`) is the primary public API. It is designed to be a dependency for Rust projects that need candle data. The provider and CLI crates are supporting infrastructure.

- **Precision over convenience.** Prices are stored as exact decimal strings, not floating-point. Timestamps are UTC microseconds. Session classification is derived at query time, not baked into the data. These choices prioritize correctness for financial applications.

### What This Repo Is Not

- **Not a real-time streaming feed.** It can fetch today's candles, but WebSocket streaming and sub-second data belong in a dedicated service.
- **Not a trading engine.** No strategy logic, order management, or execution belongs here.
- **Not for options data.** Options have fundamentally different schemas (strike, expiry, greeks, chains). Options data belongs in its own dedicated repo with its own types and store.
- **Not a general-purpose time series database.** It stores 5-minute OHLCV candles for equities/ETFs. If the scope creeps beyond that, it should be a separate repo.
- **Not provider-specific.** The provider layer is an abstraction. Adding data sources is welcome; coupling the core to any single provider is not.

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
