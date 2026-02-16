use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::candle::Candle;
use crate::error::MarketDataError;

pub fn candle_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("open", DataType::Utf8, false),
        Field::new("high", DataType::Utf8, false),
        Field::new("low", DataType::Utf8, false),
        Field::new("close", DataType::Utf8, false),
        Field::new("volume", DataType::Int64, false),
    ])
}

pub fn candles_to_record_batch(candles: &[Candle]) -> Result<RecordBatch, MarketDataError> {
    let schema = Arc::new(candle_schema());

    let timestamps: Vec<i64> = candles
        .iter()
        .map(|c| c.timestamp.timestamp_micros())
        .collect();

    let opens: Vec<String> = candles.iter().map(|c| c.open.to_string()).collect();
    let highs: Vec<String> = candles.iter().map(|c| c.high.to_string()).collect();
    let lows: Vec<String> = candles.iter().map(|c| c.low.to_string()).collect();
    let closes: Vec<String> = candles.iter().map(|c| c.close.to_string()).collect();
    let volumes: Vec<i64> = candles.iter().map(|c| c.volume).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
        Arc::new(StringArray::from(
            opens.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
        )),
        Arc::new(StringArray::from(
            highs.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
        )),
        Arc::new(StringArray::from(
            lows.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
        )),
        Arc::new(StringArray::from(
            closes.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
        )),
        Arc::new(Int64Array::from(volumes)),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn record_batch_to_candles(batch: &RecordBatch) -> Result<Vec<Candle>, MarketDataError> {
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| MarketDataError::InvalidData("expected timestamp column".into()))?;

    let opens = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MarketDataError::InvalidData("expected open column".into()))?;

    let highs = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MarketDataError::InvalidData("expected high column".into()))?;

    let lows = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MarketDataError::InvalidData("expected low column".into()))?;

    let closes = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MarketDataError::InvalidData("expected close column".into()))?;

    let volumes = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| MarketDataError::InvalidData("expected volume column".into()))?;

    let mut candles = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let micros = timestamps.value(i);
        let timestamp = chrono::DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| MarketDataError::InvalidData(format!("invalid timestamp: {micros}")))?;

        let open = opens
            .value(i)
            .parse()
            .map_err(|e| MarketDataError::InvalidData(format!("invalid open: {e}")))?;
        let high = highs
            .value(i)
            .parse()
            .map_err(|e| MarketDataError::InvalidData(format!("invalid high: {e}")))?;
        let low = lows
            .value(i)
            .parse()
            .map_err(|e| MarketDataError::InvalidData(format!("invalid low: {e}")))?;
        let close = closes
            .value(i)
            .parse()
            .map_err(|e| MarketDataError::InvalidData(format!("invalid close: {e}")))?;
        let volume = volumes.value(i);

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

pub fn write_parquet(path: &Path, candles: &[Candle]) -> Result<(), MarketDataError> {
    let batch = candles_to_record_batch(candles)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn read_parquet(path: &Path) -> Result<Vec<Candle>, MarketDataError> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut all_candles = Vec::new();
    for batch in reader {
        let batch = batch?;
        let mut candles = record_batch_to_candles(&batch)?;
        all_candles.append(&mut candles);
    }

    Ok(all_candles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn sample_candles() -> Vec<Candle> {
        vec![
            Candle {
                timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap(),
                open: dec!(150.1234),
                high: dec!(151.5678),
                low: dec!(149.0001),
                close: dec!(150.9999),
                volume: 1000,
            },
            Candle {
                timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 14, 35, 0).unwrap(),
                open: dec!(150.9999),
                high: dec!(152.00),
                low: dec!(150.50),
                close: dec!(151.75),
                volume: 2000,
            },
        ]
    }

    #[test]
    fn record_batch_roundtrip() {
        let candles = sample_candles();
        let batch = candles_to_record_batch(&candles).unwrap();
        let result = record_batch_to_candles(&batch).unwrap();
        assert_eq!(candles, result);
    }

    #[test]
    fn empty_candles_roundtrip() {
        let candles: Vec<Candle> = vec![];
        let batch = candles_to_record_batch(&candles).unwrap();
        assert_eq!(batch.num_rows(), 0);
        let result = record_batch_to_candles(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parquet_file_roundtrip() {
        let candles = sample_candles();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        write_parquet(&path, &candles).unwrap();
        let result = read_parquet(&path).unwrap();
        assert_eq!(candles, result);
    }

    #[test]
    fn decimal_precision_preserved() {
        let candle = Candle {
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap(),
            open: dec!(123.4567),
            high: dec!(200.0000),
            low: dec!(0.0001),
            close: dec!(99999.9999),
            volume: 0,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("precision.parquet");

        write_parquet(&path, &[candle.clone()]).unwrap();
        let result = read_parquet(&path).unwrap();

        assert_eq!(result[0].open, dec!(123.4567));
        assert_eq!(result[0].high, dec!(200.0000));
        assert_eq!(result[0].low, dec!(0.0001));
        assert_eq!(result[0].close, dec!(99999.9999));
    }
}
