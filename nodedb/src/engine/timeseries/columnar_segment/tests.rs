use nodedb_codec::ColumnCodec;
use nodedb_types::timeseries::{MetricSample, PartitionMeta, PartitionState};
use tempfile::TempDir;

use super::super::columnar_memtable::{
    ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use super::reader::ColumnarSegmentReader;
use super::schema::{SchemaJson, schema_from_parsed, schema_to_json};
use super::writer::ColumnarSegmentWriter;

fn test_config() -> ColumnarMemtableConfig {
    ColumnarMemtableConfig {
        max_memory_bytes: 10 * 1024 * 1024,
        hard_memory_limit: 20 * 1024 * 1024,
        max_tag_cardinality: 1000,
    }
}

#[test]
fn write_and_read_simple_partition() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..100 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1000 + i,
                value: i as f64 * 0.5,
            },
        );
    }
    let drain = mt.drain();

    let meta = writer
        .write_partition("ts-test", &drain, 86_400_000, 42)
        .unwrap();
    assert_eq!(meta.row_count, 100);
    assert_eq!(meta.min_ts, 1000);
    assert_eq!(meta.max_ts, 1099);
    assert_eq!(meta.last_flushed_wal_lsn, 42);
    assert!(meta.size_bytes > 0);

    assert!(meta.column_stats.contains_key("timestamp"));
    assert!(meta.column_stats.contains_key("value"));
    let ts_stats = &meta.column_stats["timestamp"];
    assert_eq!(ts_stats.count, 100);
    assert_eq!(ts_stats.codec, ColumnCodec::DoubleDelta);
    let val_stats = &meta.column_stats["value"];
    assert_eq!(val_stats.count, 100);
    assert_eq!(val_stats.codec, ColumnCodec::Gorilla);

    let part_dir = tmp.path().join("ts-test");
    let read_meta = ColumnarSegmentReader::read_meta(&part_dir).unwrap();
    assert_eq!(read_meta.row_count, 100);
    assert!(read_meta.column_stats.contains_key("timestamp"));

    let schema = ColumnarSegmentReader::read_schema(&part_dir).unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.codec(0), ColumnCodec::DoubleDelta);
    assert_eq!(schema.codec(1), ColumnCodec::Gorilla);

    let ts_col = ColumnarSegmentReader::read_column_with_codec(
        &part_dir,
        "timestamp",
        ColumnType::Timestamp,
        Some(ColumnCodec::DoubleDelta),
    )
    .unwrap();
    let timestamps = ts_col.as_timestamps();
    assert_eq!(timestamps.len(), 100);
    assert_eq!(timestamps[0], 1000);
    assert_eq!(timestamps[99], 1099);

    let val_col = ColumnarSegmentReader::read_column_with_codec(
        &part_dir,
        "value",
        ColumnType::Float64,
        Some(ColumnCodec::Gorilla),
    )
    .unwrap();
    let values = val_col.as_f64();
    assert_eq!(values.len(), 100);
    assert!((values[0] - 0.0).abs() < f64::EPSILON);
    assert!((values[99] - 49.5).abs() < f64::EPSILON);
}

#[test]
fn write_and_read_with_tags() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("cpu".into(), ColumnType::Float64),
            ("host".into(), ColumnType::Symbol),
        ],
        timestamp_idx: 0,
        codecs: vec![ColumnCodec::Auto; 3],
    };
    let mut mt = ColumnarMemtable::new(schema, test_config());

    for i in 0..50 {
        let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
        mt.ingest_row(
            i % 2,
            &[
                ColumnValue::Timestamp(5000 + i as i64),
                ColumnValue::Float64(50.0 + i as f64),
                ColumnValue::Symbol(host),
            ],
        )
        .unwrap();
    }
    let drain = mt.drain();

    let meta = writer
        .write_partition("ts-tags", &drain, 86_400_000, 99)
        .unwrap();
    assert_eq!(meta.row_count, 50);
    assert_eq!(meta.column_stats["host"].codec, ColumnCodec::FastLanesLz4);
    assert_eq!(meta.column_stats["host"].cardinality, Some(2));

    let part_dir = tmp.path().join("ts-tags");
    let host_col = ColumnarSegmentReader::read_column_with_codec(
        &part_dir,
        "host",
        ColumnType::Symbol,
        Some(ColumnCodec::FastLanesLz4),
    )
    .unwrap();
    assert_eq!(host_col.as_symbols().len(), 50);

    let host_dict = ColumnarSegmentReader::read_symbol_dict(&part_dir, "host").unwrap();
    assert_eq!(host_dict.len(), 2);
}

#[test]
fn column_projection() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
            ("extra".into(), ColumnType::Int64),
        ],
        timestamp_idx: 0,
        codecs: vec![ColumnCodec::Auto; 3],
    };
    let mut mt = ColumnarMemtable::new(schema, test_config());
    for i in 0..20 {
        mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(i * 100),
                ColumnValue::Float64(i as f64),
                ColumnValue::Int64(i * 10),
            ],
        )
        .unwrap();
    }
    let drain = mt.drain();
    let meta = writer
        .write_partition("ts-proj", &drain, 86_400_000, 0)
        .unwrap();

    assert!(matches!(
        meta.column_stats["extra"].codec,
        ColumnCodec::Delta | ColumnCodec::DoubleDelta
    ));

    let part_dir = tmp.path().join("ts-proj");
    let projected = ColumnarSegmentReader::read_columns(
        &part_dir,
        &[
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
        ],
    )
    .unwrap();
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].len(), 20);
    assert_eq!(projected[1].len(), 20);
}

#[test]
fn explicit_codec_override() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
        ],
        timestamp_idx: 0,
        codecs: vec![ColumnCodec::Gorilla, ColumnCodec::Gorilla],
    };
    let mut mt = ColumnarMemtable::new(schema, test_config());
    for i in 0..100 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1_700_000_000_000 + i * 10_000,
                value: 42.0 + i as f64 * 0.1,
            },
        );
    }
    let drain = mt.drain();
    let meta = writer
        .write_partition("ts-gorilla", &drain, 86_400_000, 0)
        .unwrap();

    assert_eq!(meta.column_stats["timestamp"].codec, ColumnCodec::Gorilla);
    assert_eq!(meta.column_stats["value"].codec, ColumnCodec::Gorilla);

    let part_dir = tmp.path().join("ts-gorilla");
    let ts_col = ColumnarSegmentReader::read_column_with_codec(
        &part_dir,
        "timestamp",
        ColumnType::Timestamp,
        Some(ColumnCodec::Gorilla),
    )
    .unwrap();
    let timestamps = ts_col.as_timestamps();
    assert_eq!(timestamps.len(), 100);
    assert_eq!(timestamps[0], 1_700_000_000_000);
}

#[test]
fn compression_stats_in_metadata() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..10_000 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1_700_000_000_000 + i * 10_000,
                value: 42.0,
            },
        );
    }
    let drain = mt.drain();
    let meta = writer
        .write_partition("ts-stats", &drain, 86_400_000, 0)
        .unwrap();

    let ts_stats = &meta.column_stats["timestamp"];
    assert!(ts_stats.compression_ratio() > 3.0);
    assert_eq!(ts_stats.min, Some(1_700_000_000_000.0));
    assert_eq!(ts_stats.count, 10_000);

    let val_stats = &meta.column_stats["value"];
    assert!(val_stats.compression_ratio() > 1.0);
    assert_eq!(val_stats.min, Some(42.0));
    assert_eq!(val_stats.max, Some(42.0));
    assert_eq!(val_stats.sum, Some(420_000.0));
}

#[test]
fn schema_v2_roundtrip() {
    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("cpu".into(), ColumnType::Float64),
            ("host".into(), ColumnType::Symbol),
        ],
        timestamp_idx: 0,
        codecs: vec![
            ColumnCodec::DoubleDelta,
            ColumnCodec::Gorilla,
            ColumnCodec::Raw,
        ],
    };
    let json = sonic_rs::to_vec(&schema_to_json(&schema)).unwrap();
    let parsed: SchemaJson = sonic_rs::from_slice(&json).unwrap();
    let recovered = schema_from_parsed(&parsed).unwrap();

    assert_eq!(recovered.columns, schema.columns);
    assert_eq!(recovered.timestamp_idx, 0);
    assert_eq!(recovered.codecs, schema.codecs);
}

#[test]
fn sparse_index_written_during_flush() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..2048 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1_700_000_000_000 + i * 10_000,
                value: (i % 100) as f64,
            },
        );
    }
    let drain = mt.drain();
    writer
        .write_partition("ts-sparse", &drain, 86_400_000, 0)
        .unwrap();

    let part_dir = tmp.path().join("ts-sparse");
    let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
        .unwrap()
        .expect("sparse index should exist");

    assert_eq!(sparse.total_rows(), 2048);
    assert_eq!(sparse.block_count(), 2);
    assert_eq!(sparse.column_names, vec!["timestamp", "value"]);

    assert_eq!(sparse.blocks[0].min_ts, 1_700_000_000_000);
    assert_eq!(sparse.blocks[0].max_ts, 1_700_000_000_000 + 1023 * 10_000);
    assert_eq!(sparse.blocks[1].min_ts, 1_700_000_000_000 + 1024 * 10_000);
}

#[test]
fn sparse_index_time_range_query() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..5000 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1_700_000_000_000 + i * 1000,
                value: i as f64,
            },
        );
    }
    let drain = mt.drain();
    writer
        .write_partition("ts-range", &drain, 86_400_000, 0)
        .unwrap();

    let part_dir = tmp.path().join("ts-range");
    let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
        .unwrap()
        .unwrap();

    assert_eq!(sparse.block_count(), 5);

    let start = 1_700_000_000_000 + 2000 * 1000;
    let end = 1_700_000_000_000 + 3000 * 1000;
    let matching = sparse.blocks_in_time_range(start, end);
    assert!(matching.len() < 5, "should skip at least 1 block");
    assert!(!matching.is_empty());
}

#[test]
fn sparse_index_predicate_pushdown() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("cpu".into(), ColumnType::Float64),
        ],
        timestamp_idx: 0,
        codecs: vec![ColumnCodec::Auto; 2],
    };
    let mut mt = ColumnarMemtable::new(schema, test_config());
    for i in 0..2048 {
        mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(1_700_000_000_000 + i as i64 * 1000),
                ColumnValue::Float64(if i < 1024 {
                    (i % 50) as f64
                } else {
                    50.0 + (i % 50) as f64
                }),
            ],
        )
        .unwrap();
    }
    let drain = mt.drain();
    writer
        .write_partition("ts-pred", &drain, 86_400_000, 0)
        .unwrap();

    let part_dir = tmp.path().join("ts-pred");
    let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
        .unwrap()
        .unwrap();

    use crate::engine::timeseries::sparse_index::BlockPredicate;
    let preds = vec![BlockPredicate::GreaterThan {
        column_idx: 1,
        threshold: 60.0,
    }];
    let matching = sparse.filter_blocks(i64::MIN, i64::MAX, &preds);
    assert_eq!(matching, vec![1]);
}

#[test]
fn metadata_only_queries() {
    let tmp = TempDir::new().unwrap();
    let writer = ColumnarSegmentWriter::new(tmp.path());

    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..1000 {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1_700_000_000_000 + i * 10_000,
                value: 42.0 + i as f64 * 0.1,
            },
        );
    }
    let drain = mt.drain();
    writer
        .write_partition("ts-meta", &drain, 86_400_000, 0)
        .unwrap();

    let part_dir = tmp.path().join("ts-meta");

    let count = ColumnarSegmentReader::metadata_row_count(&part_dir).unwrap();
    assert_eq!(count, 1000);

    let (min_ts, max_ts) = ColumnarSegmentReader::metadata_ts_range(&part_dir).unwrap();
    assert_eq!(min_ts, 1_700_000_000_000);
    assert_eq!(max_ts, 1_700_000_000_000 + 999 * 10_000);

    let stats = ColumnarSegmentReader::metadata_column_stats(&part_dir, "value")
        .unwrap()
        .unwrap();
    assert_eq!(stats.count, 1000);
    assert!(stats.min.unwrap() < 43.0);
    assert!(stats.max.unwrap() > 140.0);

    use crate::engine::timeseries::sparse_index::BlockPredicate;
    let pred = BlockPredicate::GreaterThan {
        column_idx: 0,
        threshold: 200.0,
    };
    let might = ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred).unwrap();
    assert!(!might);

    let pred2 = BlockPredicate::GreaterThan {
        column_idx: 0,
        threshold: 50.0,
    };
    let might2 = ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred2).unwrap();
    assert!(might2);
}

#[test]
fn legacy_partition_no_sparse_index() {
    let tmp = TempDir::new().unwrap();
    let part_dir = tmp.path().join("ts-legacy");
    std::fs::create_dir_all(&part_dir).unwrap();
    std::fs::write(
        part_dir.join("partition.meta"),
        sonic_rs::to_vec(&PartitionMeta {
            min_ts: 0,
            max_ts: 100,
            row_count: 10,
            size_bytes: 100,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 0,
            column_stats: std::collections::HashMap::new(),
            max_system_ts: 0,
        })
        .unwrap(),
    )
    .unwrap();

    let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir).unwrap();
    assert!(sparse.is_none());
}
