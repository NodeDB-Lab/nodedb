use std::path::{Path, PathBuf};

use nodedb_types::timeseries::{MetricSample, PartitionInterval, TieredPartitionConfig};
use tempfile::TempDir;

use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, ColumnarSegmentWriter};
use crate::engine::timeseries::partition_registry::PartitionRegistry;

use super::cycle::run_merge_cycle;
use super::partitions::merge_partitions;

fn test_config() -> ColumnarMemtableConfig {
    ColumnarMemtableConfig {
        max_memory_bytes: 10 * 1024 * 1024,
        hard_memory_limit: 20 * 1024 * 1024,
        max_tag_cardinality: 1000,
    }
}

fn write_test_partition(base_dir: &Path, name: &str, start_ts: i64, count: usize) -> PathBuf {
    let writer = ColumnarSegmentWriter::new(base_dir);
    let mut mt = ColumnarMemtable::new_metric(test_config());
    for i in 0..count {
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: start_ts + i as i64,
                value: (start_ts + i as i64) as f64,
            },
        );
    }
    let drain = mt.drain();
    writer.write_partition(name, &drain, 86_400_000, 0).unwrap();
    base_dir.join(name)
}

#[test]
fn merge_two_simple_partitions() {
    let tmp = TempDir::new().unwrap();
    let dir1 = write_test_partition(tmp.path(), "ts-part1", 1000, 50);
    let dir2 = write_test_partition(tmp.path(), "ts-part2", 2000, 50);

    let result = merge_partitions(tmp.path(), &[dir1, dir2], "ts-merged").unwrap();

    assert_eq!(result.meta.row_count, 100);
    assert_eq!(result.meta.min_ts, 1000);
    assert_eq!(result.meta.max_ts, 2049);
    assert_eq!(result.source_count, 2);

    // Read back merged data.
    let merged_dir = tmp.path().join("ts-merged");
    let ts_col =
        ColumnarSegmentReader::read_column(&merged_dir, "timestamp", ColumnType::Timestamp)
            .unwrap();
    let timestamps = ts_col.as_timestamps();
    assert_eq!(timestamps.len(), 100);
    // Should be sorted.
    for w in timestamps.windows(2) {
        assert!(w[0] <= w[1], "not sorted: {} > {}", w[0], w[1]);
    }
}

#[test]
fn merge_with_tags() {
    let tmp = TempDir::new().unwrap();
    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
            ("host".into(), ColumnType::Symbol),
        ],
        timestamp_idx: 0,
        codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
    };

    // Write two partitions with different host values.
    let writer = ColumnarSegmentWriter::new(tmp.path());
    for (part_name, host, start) in [("ts-p1", "host-a", 1000i64), ("ts-p2", "host-b", 2000)] {
        let mut mt = ColumnarMemtable::new(schema.clone(), test_config());
        for i in 0..10 {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(start + i),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol(host),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        writer
            .write_partition(part_name, &drain, 86_400_000, 0)
            .unwrap();
    }

    let result = merge_partitions(
        tmp.path(),
        &[tmp.path().join("ts-p1"), tmp.path().join("ts-p2")],
        "ts-merged-tags",
    )
    .unwrap();
    assert_eq!(result.meta.row_count, 20);

    // Read merged symbol dict — should have both hosts.
    let merged_dir = tmp.path().join("ts-merged-tags");
    let dict = ColumnarSegmentReader::read_symbol_dict(&merged_dir, "host").unwrap();
    assert_eq!(dict.len(), 2);
    assert!(dict.get_id("host-a").is_some());
    assert!(dict.get_id("host-b").is_some());
}

#[test]
fn merge_preserves_timestamp_order() {
    let tmp = TempDir::new().unwrap();
    // Partition 2 has earlier timestamps than partition 1.
    let dir1 = write_test_partition(tmp.path(), "ts-later", 5000, 20);
    let dir2 = write_test_partition(tmp.path(), "ts-earlier", 1000, 20);

    let merged = merge_partitions(tmp.path(), &[dir1, dir2], "ts-sorted").unwrap();
    assert_eq!(
        merged.meta.row_count, 40,
        "merged partition should have all rows"
    );

    let merged_dir = tmp.path().join("ts-sorted");
    let ts_col =
        ColumnarSegmentReader::read_column(&merged_dir, "timestamp", ColumnType::Timestamp)
            .unwrap();
    let timestamps = ts_col.as_timestamps();
    // All timestamps from partition 2 (1000-1019) should come before partition 1 (5000-5019).
    assert_eq!(timestamps[0], 1000);
    assert_eq!(timestamps[19], 1019);
    assert_eq!(timestamps[20], 5000);
}

#[test]
fn run_merge_cycle_test() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = TieredPartitionConfig::origin_defaults();
    cfg.partition_by = PartitionInterval::Duration(86_400_000);
    cfg.merge_after_ms = 1000; // 1 second for testing
    cfg.merge_count = 3;
    let mut registry = PartitionRegistry::new(cfg);

    let day_ms = 86_400_000i64;

    // Create and seal 3 partitions with data.
    for d in 1..=3 {
        let start = d * day_ms;
        let (entry, _) = registry.get_or_create_partition(start);
        let dir_name = entry.dir_name.clone();

        write_test_partition(tmp.path(), &dir_name, start, 10);

        // Update meta.
        if let Some(e) = registry.get_mut(start) {
            e.meta.row_count = 10;
            e.meta.max_ts = start + 9;
        }
        registry.seal_partition(start);
    }

    assert_eq!(registry.sealed_count(), 3);

    // Run merge — should merge all 3.
    let now = 10 * day_ms; // far enough in the future
    let merged = run_merge_cycle(&mut registry, tmp.path(), now).unwrap();
    assert_eq!(merged, 1);

    // Purge deleted.
    let deleted_dirs = registry.purge_deleted();
    assert_eq!(deleted_dirs.len(), 2); // 2 deleted (1 overwritten by merged)
}
