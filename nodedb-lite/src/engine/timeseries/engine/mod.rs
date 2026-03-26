mod compaction;
pub(crate) mod core;
mod flush;
mod ingest;
mod lifecycle;
mod query;
mod retention;
mod sync;
mod wal;

pub use compaction::{CompactionResult, MaintenanceResult};
pub use core::TimeseriesEngine;
pub use flush::{FlushResult, RedbEntry};
pub use lifecycle::{
    BackupResult, BudgetAction, BudgetCheckResult, BudgetPolicy, CompactionScheduler,
    DownsamplePlan,
};
pub use retention::UnsyncedDropWarning;
pub use wal::WalEntry;

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::{MetricSample, SeriesKey, TieredPartitionConfig, TimeRange};

    #[test]
    fn ingest_and_scan() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu_usage",
                vec![("host".into(), "prod-1".into())],
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: 50.0 + (i as f64) * 0.1,
                },
            );
        }
        assert_eq!(engine.row_count("metrics"), 100);
        let results = engine.scan("metrics", &TimeRange::new(1000, 1099));
        assert_eq!(results.len(), 100);
        assert_eq!(results[0].0, 1000);
        assert_eq!(results[99].0, 1099);
    }

    #[test]
    fn aggregate_by_bucket() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 10,
                    value: i as f64,
                },
            );
        }
        let buckets = engine.aggregate_by_bucket("metrics", &TimeRange::new(0, 999), 100);
        assert_eq!(buckets.len(), 10);
        assert_eq!(buckets[0].1, 10);
    }

    #[test]
    fn flush_and_decode() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..50 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: 5000 + i * 100,
                    value: (i as f64) * 0.5,
                },
            );
        }
        let flush = engine.flush("metrics").expect("flush non-empty collection");
        assert_eq!(flush.meta.row_count, 50);
        assert_eq!(flush.meta.min_ts, 5000);

        let decoded_ts = TimeseriesEngine::decode_timestamps(&flush.ts_block);
        assert_eq!(decoded_ts.len(), 50);
        assert_eq!(decoded_ts[0], 5000);

        let decoded_vals = TimeseriesEngine::decode_values(&flush.val_block);
        assert_eq!(decoded_vals.len(), 50);
        assert!((decoded_vals[0] - 0.0).abs() < f64::EPSILON);

        assert_eq!(engine.row_count("metrics"), 0);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn retention() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000,
            ..TieredPartitionConfig::lite_defaults()
        });

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("metrics");
        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 2000,
                value: 2.0,
            },
        );
        engine.flush("metrics");

        assert_eq!(engine.partition_count("metrics"), 2);
        let dropped = engine.apply_retention(2500);
        assert_eq!(dropped.len(), 1);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn series_catalog_integration() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "b".into())],
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "mem",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 300,
                value: 3.0,
            },
        );
        assert_eq!(engine.catalog().len(), 3);
    }

    #[test]
    fn flush_result_redb_entries() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 1000,
                value: 42.0,
            },
        );
        let flush = engine.flush("m").expect("flush non-empty");
        let entries = flush.to_redb_entries().expect("serialize meta");
        assert_eq!(entries.len(), 4);
        assert!(entries[0].0.starts_with(b"ts:m:1000:ts"));
    }

    #[test]
    fn empty_scan() {
        let engine = TimeseriesEngine::new();
        assert!(
            engine
                .scan("nonexistent", &TimeRange::new(0, 1000))
                .is_empty()
        );
    }

    #[test]
    fn batch_ingest() {
        let mut engine = TimeseriesEngine::new();
        let samples: Vec<MetricSample> = (0..1000)
            .map(|i| MetricSample {
                timestamp_ms: i * 10,
                value: i as f64,
            })
            .collect();
        engine.ingest_batch("metrics", "cpu", vec![], &samples);
        assert_eq!(engine.row_count("metrics"), 1000);
    }

    #[test]
    fn batch_ingest_sets_dirty() {
        let mut engine = TimeseriesEngine::new();
        assert!(engine.dirty_collections().is_empty());
        engine.ingest_batch(
            "metrics",
            "cpu",
            vec![],
            &[MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            }],
        );
        assert_eq!(engine.dirty_collections(), vec!["metrics"]);
    }

    #[test]
    fn sync_watermark_defaults_to_zero() {
        let engine = TimeseriesEngine::new();
        assert_eq!(engine.sync_watermark(42), 0);
    }

    #[test]
    fn acknowledge_sync_updates_watermarks() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![("h".into(), "a".into())],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.ingest_metric(
            "m",
            "cpu",
            vec![("h".into(), "a".into())],
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );

        let sid = engine
            .catalog
            .resolve(&SeriesKey::new("cpu", vec![("h".into(), "a".into())]));
        assert_eq!(engine.sync_watermark(sid), 0);
        engine.acknowledge_sync("m", 100);
        assert_eq!(engine.sync_watermark(sid), 100);
        engine.acknowledge_sync("m", 200);
        assert_eq!(engine.sync_watermark(sid), 200);
        engine.acknowledge_sync("m", 50);
        assert_eq!(engine.sync_watermark(sid), 200); // Monotonic.
    }

    #[test]
    fn acknowledge_sync_clears_dirty() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        assert!(!engine.dirty_collections().is_empty());
        engine.acknowledge_sync("m", 100);
        engine.flush("m");
        engine.acknowledge_sync("m", 100);
        assert!(engine.dirty_collections().is_empty());
    }

    #[test]
    fn export_import_watermarks() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.acknowledge_sync("m", 100);
        let exported = engine.export_watermarks().clone();
        assert!(!exported.is_empty());
        let mut engine2 = TimeseriesEngine::new();
        engine2.import_watermarks(exported.clone());
        assert_eq!(engine2.export_watermarks(), &exported);
    }

    #[test]
    fn assign_sync_lsn_monotonic() {
        let mut engine = TimeseriesEngine::new();
        let lsn1 = engine.assign_sync_lsn();
        let lsn2 = engine.assign_sync_lsn();
        assert!(lsn1 < lsn2);
    }

    #[test]
    fn retention_with_sync_default_drops_unsynced() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000,
            retain_until_synced: false,
            ..TieredPartitionConfig::lite_defaults()
        });
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");
        let (dropped, warnings) = engine.apply_retention_with_sync(5000);
        assert_eq!(dropped.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].collection, "m");
    }

    #[test]
    fn retention_with_sync_guarded_retains_unsynced() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000,
            retain_until_synced: true,
            ..TieredPartitionConfig::lite_defaults()
        });
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");
        let (dropped, warnings) = engine.apply_retention_with_sync(5000);
        assert!(dropped.is_empty());
        assert!(warnings.is_empty());
        assert_eq!(engine.partition_count("m"), 1);
    }
}
