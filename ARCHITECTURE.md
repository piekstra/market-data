# Architecture

## Purpose

`market-data` is a **Parquet-backed storage system for 5-minute OHLCV candle data**. It handles data acquisition from external APIs, local storage in a structured filesystem layout, and retrieval with session filtering. It is the canonical source of historical price data for the trading ecosystem.

## Core Principles

1. **Storage, not computation** — This system stores and retrieves raw candle data. It does not compute indicators, returns, statistics, or any derived metrics. That responsibility belongs to [market-calculations](https://github.com/piekstra/market-calculations).

2. **Filesystem-native** — Data is stored as Parquet files in a structured directory hierarchy (`data/{SYMBOL}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet`). No database required. Files are portable, inspectable, and version-controllable.

3. **Decimal precision** — Prices are stored as `rust_decimal::Decimal` (serialized as UTF-8 strings in Parquet) to avoid floating-point rounding errors inherent in financial data.

4. **Idempotent population** — The CLI checks for missing dates before fetching, fetches only what's needed, and groups requests into contiguous ranges to minimize API calls. Re-running `populate` is safe and efficient.

5. **Provider abstraction** — Data sources are pluggable via the `CandleProvider` trait. Adding a new data source means implementing one trait, not modifying the storage layer.

6. **One file per symbol per day** — Enables efficient range queries (read only the days you need), simple cache invalidation (delete/overwrite one file), and predictable disk usage.

## Crate Structure

```
market-data/
├── crates/
│   ├── market-data-core/        Core types, CandleStore, Parquet I/O, session logic
│   ├── market-data-providers/   CandleProvider trait + implementations (Alpaca, Yahoo)
│   └── market-data-cli/         CLI for populating, validating, and inspecting data
├── data/                        Runtime data directory (gitignored)
```

### market-data-core
The library crate that consumers depend on. Provides:
- **`Candle`** — `timestamp: DateTime<Utc>`, `open/high/low/close: Decimal`, `volume: i64`
- **`CandleStore`** — read/write API: `write_day()`, `read_day()`, `read_range()`, `read_range_session()`, `list_symbols()`, `list_dates()`, `missing_dates()`
- **`Session`** — `PreMarket` (4:00-9:30 ET), `Regular` (9:30-16:00 ET), `AfterHours` (16:00-20:00 ET). Computed from timestamps at read time, not stored.
- **Parquet schema** — `timestamp` (microseconds UTC), `open/high/low/close` (UTF8 strings), `volume` (Int64). SNAPPY compression.

### market-data-providers
Async data fetching. Implements the `CandleProvider` trait:
- **Alpaca** — Requires `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` env vars. Pagination support, rate limit retry.
- **Yahoo** — No auth required (public v8 chart API). Limited to ~60 days of intraday history.

### market-data-cli
Management tool with three commands:
- **`populate`** — Fetch and store candles for symbols/date ranges. Smart: only fetches missing dates.
- **`status`** — Show what data exists (date ranges per symbol).
- **`validate`** — Check Parquet file integrity (readable, non-empty, sorted, valid volumes).

## Data Flow

```
External API (Alpaca / Yahoo)
         ↓
CandleProvider::fetch_candles_range()
         ↓
Vec<(NaiveDate, Vec<Candle>)>
         ↓
CandleStore::write_day()
         ↓
Parquet file: data/{SYMBOL}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
         ↓
CandleStore::read_range() / read_range_session()
         ↓
Vec<Candle> → consumer (tirds-loader, backtesting apps)
```

## Boundaries

### This system IS responsible for:
- Storing and retrieving 5-minute OHLCV candle data
- Parquet serialization/deserialization with decimal precision
- Data provider abstraction and implementations (Alpaca, Yahoo)
- Trading session classification (pre-market, regular, after-hours)
- Weekday calendar logic
- Data integrity validation
- CLI tooling for data management

### This system is NOT responsible for:
- Technical indicators or derived metrics — delegated to [market-calculations](https://github.com/piekstra/market-calculations)
- Trade evaluation or strategy logic — delegated to [tirds](https://github.com/piekstra/tirds)
- Real-time / streaming data — delegated to [trading-data-stream](https://github.com/piekstra/trading-data-stream)
- Multiple timeframes (only 5-minute intervals currently)
- Market holiday calendar (only filters weekends, not exchange holidays)
- Split/dividend price adjustments
- Order execution or trading logic

## Consumer API

Downstream systems should depend on `market-data-core` only:

```rust
use market_data_core::{CandleStore, Candle, Session};

let store = CandleStore::new("./data");
let candles = store.read_range("AAPL", start_date, end_date)?;
let regular_only = store.read_range_session("AAPL", start, end, Session::Regular)?;
```

Note: `market-data-core` uses `Decimal` for prices while `market-calculations` uses `f64`. The conversion is handled by consumers (e.g., `tirds-loader` converts when passing candles to the calculation pipeline).

## Storage Format

```
data/
├── AAPL/
│   └── 2025/
│       └── 01/
│           ├── 2025-01-02.parquet   (~4KB, SNAPPY compressed)
│           ├── 2025-01-03.parquet
│           └── ...
├── TQQQ/
│   └── ...
```

Each file contains all 5-minute candles for one symbol for one trading day (~78 candles for regular hours, more with extended hours).
