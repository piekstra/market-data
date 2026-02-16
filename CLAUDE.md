# market-data

## Repository Structure

```
market-data/
├── crates/
│   ├── market-data-core/       # Core types, Parquet I/O, CandleStore API
│   ├── market-data-providers/  # Alpaca + Yahoo Finance data fetching
│   └── market-data-cli/        # CLI for populate, status, validate
├── data/                       # Gitignored, populated by CLI
└── .github/workflows/          # CI (PRs) and Release (tags)
```

## Key Commands

```bash
# Check everything
cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test --workspace

# Run tests
cargo test --workspace

# Run CLI
cargo run -p market-data-cli -- populate -s AAPL --start 2025-01-01
cargo run -p market-data-cli -- status
cargo run -p market-data-cli -- validate
```

## Architecture

- **Candle**: `{ timestamp: DateTime<Utc>, open: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: i64 }`
- **Session**: `PreMarket` (4:00-9:30 ET), `Regular` (9:30-16:00 ET), `AfterHours` (16:00-20:00 ET) — derived from timestamp, not stored
- **CandleStore**: Filesystem-backed store, one Parquet file per symbol per trading day at `data/{SYMBOL}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet`
- **CandleProvider**: Async trait for data fetching. Alpaca uses direct HTTP. Yahoo uses v8 chart API.
- Prices stored as decimal strings in Parquet for exact `rust_decimal` precision

## Versioning

All crates share `workspace.package.version`. Releases via git tags (`v*`).
