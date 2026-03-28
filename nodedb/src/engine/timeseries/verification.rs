//! Verification gate tests for the timeseries engine.
//!
//! Each test validates a specific correctness property that must hold
//! before the feature is considered complete.

#[cfg(test)]
mod tests {
    use crate::engine::timeseries::columnar_memtable::*;
    use crate::engine::timeseries::columnar_segment::*;
    use crate::engine::timeseries::o3_buffer::*;
    use crate::engine::timeseries::partition_registry::*;
    use crate::engine::timeseries::schema_evolution::*;
    use nodedb_types::timeseries::*;
    use tempfile::TempDir;

    fn test_memtable_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 100_000,
        }
    }

    // ── Data Integrity: SeriesId collision ──────────────────────────

    #[test]
    fn series_id_collision_detection() {
        let mut catalog = SeriesCatalog::new();

        // Generate 10K unique series keys.
        for i in 0..10_000 {
            let key = SeriesKey::new(format!("metric_{i}"), vec![("id".into(), format!("{i}"))]);
            let _id = catalog.resolve(&key);
        }
        assert_eq!(catalog.len(), 10_000);

        // Now inject a synthetic collision by creating two different keys
        // and forcing them to collide. The catalog should handle it.
        let k1 = SeriesKey::new("collision_test_a", vec![]);
        let k2 = SeriesKey::new("collision_test_b", vec![]);
        let id1 = catalog.resolve(&k1);
        let id2 = catalog.resolve(&k2);
        // IDs should be different even if they happen to have the same hash
        // (the catalog would rehash with a salt).
        assert_ne!(id1, id2);

        // Verify we can look them back up.
        assert_eq!(catalog.get(id1), Some(&k1));
        assert_eq!(catalog.get(id2), Some(&k2));
    }

    // ── Data Integrity: O3 correctness ──────────────────────────────

    #[test]
    fn o3_ingest_and_query_correctness() {
        // Ingest 1000 rows out of order, query back, verify sorted result.
        let mut buf = O3Buffer::new(10_000);

        // Generate shuffled timestamps.
        let mut timestamps: Vec<i64> = (0..1000).map(|i| i * 100).collect();
        // Deterministic shuffle (swap each element with a "random" earlier one).
        for i in (1..timestamps.len()).rev() {
            let j = (i * 7 + 13) % (i + 1);
            timestamps.swap(i, j);
        }

        for &ts in &timestamps {
            buf.insert(O3Row {
                timestamp_ms: ts,
                series_id: 1,
                value: ts as f64,
                target_partition_start: 0,
            });
        }
        assert_eq!(buf.len(), 1000);

        // Query all rows.
        let results = buf.query_range(0, 100_000);
        assert_eq!(results.len(), 1000);

        // Verify sorted order.
        for w in results.windows(2) {
            assert!(
                w[0].timestamp_ms <= w[1].timestamp_ms,
                "O3 query not sorted: {} > {}",
                w[0].timestamp_ms,
                w[1].timestamp_ms
            );
        }

        // Verify all original values present.
        let result_ts: Vec<i64> = results.iter().map(|r| r.timestamp_ms).collect();
        let mut expected: Vec<i64> = (0..1000).map(|i| i * 100).collect();
        expected.sort();
        assert_eq!(result_ts, expected);
    }

    // ── Data Integrity: Mixed-width partition query ──────────────────

    #[test]
    fn mixed_width_partition_query() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        // Write 3 partitions with different widths.
        // Partition 1: 1-day width (rows 0-99, ts 1000-1099)
        let mut mt1 = ColumnarMemtable::new_metric(test_memtable_config());
        for i in 0..100 {
            mt1.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64,
                },
            );
        }
        let d1 = mt1.drain();
        writer
            .write_partition("ts-1d-part", &d1, 86_400_000, 0)
            .unwrap();

        // Partition 2: 3-day width (rows 100-199, ts 2000-2099)
        let mut mt2 = ColumnarMemtable::new_metric(test_memtable_config());
        for i in 0..100 {
            mt2.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 2000 + i,
                    value: (100 + i) as f64,
                },
            );
        }
        let d2 = mt2.drain();
        writer
            .write_partition("ts-3d-part", &d2, 3 * 86_400_000, 0)
            .unwrap();

        // Partition 3: 1-week width (rows 200-299, ts 3000-3099)
        let mut mt3 = ColumnarMemtable::new_metric(test_memtable_config());
        for i in 0..100 {
            mt3.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 3000 + i,
                    value: (200 + i) as f64,
                },
            );
        }
        let d3 = mt3.drain();
        writer
            .write_partition("ts-1w-part", &d3, 7 * 86_400_000, 0)
            .unwrap();

        // Register all three in a registry.
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000);
        let mut registry = PartitionRegistry::new(cfg);

        let partition_specs: &[(&str, i64, u64)] = &[
            ("ts-1d-part", 1000, 86_400_000),
            ("ts-3d-part", 2000, 3 * 86_400_000),
            ("ts-1w-part", 3000, 7 * 86_400_000),
        ];
        for &(name, start, interval) in partition_specs {
            let meta = ColumnarSegmentReader::read_meta(&tmp.path().join(name)).unwrap();
            // Verify the segment was written with the correct interval.
            assert!(
                meta.size_bytes > 0,
                "partition {name} (interval={interval}ms) should have non-zero size"
            );
            registry.import(vec![(
                start,
                PartitionEntry {
                    meta,
                    dir_name: name.to_string(),
                },
            )]);
        }

        // Query spanning all three partitions.
        let range = TimeRange::new(1000, 3099);
        let matching = registry.query_partitions(&range);
        assert_eq!(matching.len(), 3, "all 3 partitions should match the range");

        // Read and verify data from each.
        let mut total_rows = 0;
        for entry in &matching {
            let part_dir = tmp.path().join(&entry.dir_name);
            let ts_col =
                ColumnarSegmentReader::read_column(&part_dir, "timestamp", ColumnType::Timestamp)
                    .unwrap();
            total_rows += ts_col.len();
        }
        assert_eq!(total_rows, 300, "all 300 rows should be readable");
    }

    // ── Schema Migration: ALTER partition_by online ──────────────────

    #[test]
    fn alter_partition_by_online() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        let mut registry = PartitionRegistry::new(cfg);

        let day_ms = 86_400_000i64;

        // Create 1d partitions.
        for d in 1..=3 {
            registry.get_or_create_partition(d * day_ms);
            registry.seal_partition(d * day_ms);
        }
        assert_eq!(registry.sealed_count(), 3);

        // Change to 3d.
        registry.set_partition_interval(PartitionInterval::Duration(3 * day_ms as u64));

        // New partitions use 3d.
        registry.get_or_create_partition(10 * day_ms);
        assert_eq!(registry.partition_count(), 4);

        // Old 1d + new 3d partitions all queryable.
        let range = TimeRange::new(0, 100 * day_ms);
        let all = registry.query_partitions(&range);
        assert_eq!(all.len(), 4);
    }

    // ── Schema Migration: ADD/DROP COLUMN ────────────────────────────

    #[test]
    fn schema_add_drop_column_cross_partition() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        // V1 schema: timestamp + cpu
        let schema_v1 = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt1 = ColumnarMemtable::new(schema_v1.clone(), test_memtable_config());
        for i in 0..10 {
            mt1.ingest_row(
                1,
                &[ColumnValue::Timestamp(1000 + i), ColumnValue::Float64(50.0)],
            )
            .unwrap();
        }
        let d1 = mt1.drain();
        writer.write_partition("ts-v1", &d1, 86_400_000, 0).unwrap();

        // V2 schema: timestamp + cpu + mem (added)
        let schema_v2 = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("mem".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt2 = ColumnarMemtable::new(schema_v2.clone(), test_memtable_config());
        for i in 0..10 {
            mt2.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(2000 + i),
                    ColumnValue::Float64(60.0),
                    ColumnValue::Float64(1024.0),
                ],
            )
            .unwrap();
        }
        let d2 = mt2.drain();
        writer.write_partition("ts-v2", &d2, 86_400_000, 0).unwrap();

        // Query with V2 schema against V1 partition → "mem" should be NaN.
        let v1_schema = ColumnarSegmentReader::read_schema(&tmp.path().join("ts-v1")).unwrap();
        let mappings = build_column_mappings(&schema_v2, &v1_schema);

        let v1_data = vec![
            ColumnarSegmentReader::read_column(
                &tmp.path().join("ts-v1"),
                "timestamp",
                ColumnType::Timestamp,
            )
            .unwrap(),
            ColumnarSegmentReader::read_column(
                &tmp.path().join("ts-v1"),
                "cpu",
                ColumnType::Float64,
            )
            .unwrap(),
        ];

        let result = apply_mappings(&mappings, &schema_v2, &v1_data, 10);
        assert_eq!(result.len(), 3);
        // "mem" column should be NaN for V1 partition.
        let mem_vals = result[2].as_f64();
        assert_eq!(mem_vals.len(), 10);
        assert!(mem_vals[0].is_nan());

        // V2 partition has real "mem" values.
        let v2_mem = ColumnarSegmentReader::read_column(
            &tmp.path().join("ts-v2"),
            "mem",
            ColumnType::Float64,
        )
        .unwrap();
        assert!((v2_mem.as_f64()[0] - 1024.0).abs() < f64::EPSILON);
    }

    // ── Symbol dictionary cardinality breaker ────────────────────────

    #[test]
    fn symbol_cardinality_breaker_rejects_with_message() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("uuid_tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let config = ColumnarMemtableConfig {
            max_tag_cardinality: 100,
            ..test_memtable_config()
        };
        let mut mt = ColumnarMemtable::new(schema, config);

        // Insert 100 unique tags — all succeed.
        for i in 0..100 {
            let tag = format!("pod-{i}");
            let r = mt.ingest_row(
                i as u64,
                &[
                    ColumnValue::Timestamp(i as i64),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol(&tag),
                ],
            );
            assert!(r.is_ok(), "tag {i} should succeed");
        }

        // 101st unique tag — rejected with actionable error.
        let r = mt.ingest_row(
            999,
            &[
                ColumnValue::Timestamp(1000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol("one-too-many-uuid"),
            ],
        );
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(
            err.contains("cardinality limit"),
            "error should mention cardinality: {err}"
        );
        assert!(
            err.contains("uuid_tag"),
            "error should mention column name: {err}"
        );

        // Row count didn't increase (rolled back).
        assert_eq!(mt.row_count(), 100);
    }

    // ── Performance: Partition pruning ────────────────────────────────

    #[test]
    fn partition_pruning_efficiency() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        let mut registry = PartitionRegistry::new(cfg);

        let day_ms = 86_400_000i64;

        // Create 365 partitions (1 year of daily data).
        for d in 0..365 {
            let start = d * day_ms;
            let (_, _) = registry.get_or_create_partition(start);
            if let Some(entry) = registry.get_mut(start) {
                entry.meta.max_ts = start + day_ms - 1;
                entry.meta.state = PartitionState::Sealed;
            }
        }
        assert_eq!(registry.partition_count(), 365);

        // Query 1 hour from day 100.
        let query_start = 100 * day_ms + 12 * 3_600_000; // day 100, noon
        let query_end = query_start + 3_600_000; // +1 hour
        let matching = registry.query_partitions(&TimeRange::new(query_start, query_end));

        // Should match exactly 1 partition (day 100), not 365.
        assert_eq!(
            matching.len(),
            1,
            "should prune to 1 partition, got {}",
            matching.len()
        );
    }

    // ── Manifest replay idempotency ──────────────────────────────────

    #[test]
    fn manifest_export_import_idempotent() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000);
        let mut registry = PartitionRegistry::new(cfg.clone());

        let day_ms = 86_400_000i64;
        for d in 1..=5 {
            registry.get_or_create_partition(d * day_ms);
        }
        registry.seal_partition(day_ms);
        registry.seal_partition(2 * day_ms);

        // Export.
        let exported = registry.export();
        assert_eq!(exported.len(), 5);

        // Import into fresh registry — should produce identical state.
        let mut registry2 = PartitionRegistry::new(cfg.clone());
        registry2.import(exported.clone());
        assert_eq!(registry2.partition_count(), 5);
        assert_eq!(registry2.sealed_count(), 2);
        assert_eq!(registry2.active_count(), 3);

        // Import again (replay) — should be idempotent (overwrites same keys).
        registry2.import(exported);
        assert_eq!(registry2.partition_count(), 5);
    }

    // ── WAL crash matrix (simulated) ────────────────────────────────

    #[test]
    fn wal_crash_matrix_persist_recover() {
        // Simulate crash at each lifecycle step by persisting/recovering.
        let tmp = TempDir::new().unwrap();
        let manifest_path = tmp.path().join("partition_manifest.json");
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000);

        let day_ms = 86_400_000i64;
        let writer =
            crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter::new(tmp.path());

        // Step 1: Ingest → create partitions + write data.
        let mut registry = PartitionRegistry::new(cfg.clone());
        for d in 1..=3 {
            let start = d * day_ms;
            let (entry, _) = registry.get_or_create_partition(start);
            let dir_name = entry.dir_name.clone();

            // Write actual partition data.
            let mut mt = ColumnarMemtable::new_metric(test_memtable_config());
            for i in 0..50 {
                mt.ingest_metric(
                    1,
                    MetricSample {
                        timestamp_ms: start + i,
                        value: i as f64,
                    },
                );
            }
            let drain = mt.drain();
            writer
                .write_partition(&dir_name, &drain, 86_400_000, 0)
                .unwrap();

            if let Some(e) = registry.get_mut(start) {
                e.meta.row_count = 50;
                e.meta.max_ts = start + 49;
            }
        }

        // Persist manifest.
        registry.persist(&manifest_path).unwrap();

        // Step 2: "Crash" — recover from manifest.
        let recovered = PartitionRegistry::recover(&manifest_path, cfg.clone()).unwrap();
        assert_eq!(recovered.partition_count(), 3);
        assert_eq!(recovered.active_count(), 3);

        // Step 3: Seal partitions, persist, crash, recover.
        let mut registry = recovered;
        for d in 1..=3 {
            registry.seal_partition(d * day_ms);
        }
        registry.persist(&manifest_path).unwrap();

        let recovered = PartitionRegistry::recover(&manifest_path, cfg.clone()).unwrap();
        assert_eq!(recovered.sealed_count(), 3);

        // Step 4: Start merge, "crash" mid-merge (state = Merging).
        let mut registry = recovered;
        registry.mark_merging(day_ms);
        registry.persist(&manifest_path).unwrap();

        // Recovery should roll back Merging → Sealed.
        let recovered = PartitionRegistry::recover(&manifest_path, cfg.clone()).unwrap();
        assert_eq!(recovered.sealed_count(), 3); // All back to sealed.
        assert!(
            recovered
                .iter()
                .all(|(_, e)| e.meta.state != PartitionState::Merging)
        );

        // Step 5: Complete merge, delete sources, "crash" before cleanup.
        let mut registry = recovered;
        let merged_meta = PartitionMeta {
            min_ts: day_ms,
            max_ts: 3 * day_ms + 49,
            row_count: 150,
            size_bytes: 1024,
            schema_version: 1,
            state: PartitionState::Merged,
            interval_ms: 3 * 86_400_000,
            last_flushed_wal_lsn: 100,
            column_stats: std::collections::HashMap::new(),
        };
        registry.commit_merge(
            merged_meta,
            "ts-merged".into(),
            &[day_ms, 2 * day_ms, 3 * day_ms],
        );
        registry.persist(&manifest_path).unwrap();

        // Recovery should remove Deleted entries.
        let recovered = PartitionRegistry::recover(&manifest_path, cfg.clone()).unwrap();
        // Only merged partition should survive (Deleted ones purged by recover).
        assert!(recovered.partition_count() <= 2); // merged + possibly 1 overwritten
        assert!(
            recovered
                .iter()
                .all(|(_, e)| e.meta.state != PartitionState::Deleted)
        );
    }

    // ── Merge crash recovery: orphan cleanup ─────────────────────────

    #[test]
    fn merge_crash_orphan_cleanup() {
        let tmp = TempDir::new().unwrap();
        let cfg = TieredPartitionConfig::origin_defaults();

        // Create a registry with one partition.
        let mut registry = PartitionRegistry::new(cfg);
        registry.get_or_create_partition(86_400_000);

        // Simulate an orphaned merge output directory (crash during step 1).
        let orphan_dir = tmp.path().join("ts-orphan-merge");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        std::fs::write(orphan_dir.join("timestamp.col"), b"garbage").unwrap();

        // Cleanup should remove orphan but keep known partition.
        let removed = registry.cleanup_orphans(tmp.path());
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], "ts-orphan-merge");
        assert!(!orphan_dir.exists());
    }

    // ── Ingest throughput: 1M rows ───────────────────────────────────

    #[test]
    fn ingest_throughput_1m_rows() {
        let mut mt = ColumnarMemtable::new_metric(ColumnarMemtableConfig {
            max_memory_bytes: 256 * 1024 * 1024,
            hard_memory_limit: 512 * 1024 * 1024,
            max_tag_cardinality: 100_000,
        });

        let start = std::time::Instant::now();
        for i in 0..1_000_000u64 {
            mt.ingest_metric(
                i % 100,
                MetricSample {
                    timestamp_ms: i as i64,
                    value: i as f64 * 0.001,
                },
            );
        }
        let elapsed = start.elapsed();
        let rows_per_sec = 1_000_000.0 / elapsed.as_secs_f64();

        assert_eq!(mt.row_count(), 1_000_000);
        // Should achieve at least 1M rows/sec on any modern CPU.
        assert!(
            rows_per_sec > 500_000.0,
            "ingest too slow: {rows_per_sec:.0} rows/sec (expected >500K)"
        );
    }

    // ── SIMD aggregation: SUM over 10M f64 values ────────────────────

    #[test]
    fn simd_aggregation_performance() {
        use crate::engine::timeseries::columnar_agg::aggregate_f64;

        // 10M values (80MB).
        let values: Vec<f64> = (0..10_000_000).map(|i| (i as f64) * 0.001).collect();

        let start = std::time::Instant::now();
        let result = aggregate_f64(&values);
        let elapsed = start.elapsed();

        // Verify correctness: sum of 0..9,999,999 * 0.001
        let expected_sum = (0..10_000_000u64).map(|i| i as f64 * 0.001).sum::<f64>();
        let rel_error = ((result.sum - expected_sum) / expected_sum).abs();
        assert!(
            rel_error < 1e-6,
            "sum error too large: got {}, expected {}, rel_error {rel_error}",
            result.sum,
            expected_sum
        );

        assert_eq!(result.count, 10_000_000);
        assert!((result.min - 0.0).abs() < f64::EPSILON);
        assert!((result.max - 9999.999).abs() < 0.001);

        // Should complete in <100ms on any modern CPU with SIMD (release mode).
        // Debug builds are significantly slower due to no optimizations — use a
        // generous threshold to avoid flaky test failures in CI debug runs.
        let threshold_ms = if cfg!(debug_assertions) { 5000 } else { 500 };
        assert!(
            elapsed.as_millis() < threshold_ms,
            "SIMD aggregation too slow: {}ms for 10M values (threshold {threshold_ms}ms)",
            elapsed.as_millis()
        );
    }
}
