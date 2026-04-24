//! Shared timeseries types for multi-model database engines.
//!
//! Used by both `nodedb` (server) and `nodedb-lite` (embedded) for
//! timeseries ingest, storage, and query.

pub mod config;
pub mod continuous_agg;
pub mod ingest;
pub mod partition;
pub mod series;
pub mod sync;

// Re-export all public types for backward compatibility.
// Existing code uses `nodedb_types::timeseries::SeriesKey` etc.
pub use config::{ArchiveCompression, ConfigValidationError, TieredPartitionConfig};
pub use continuous_agg::{AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy};
pub use ingest::{IngestResult, LogEntry, MetricSample, SymbolDictionary, TimeRange};
pub use partition::{
    FlushedKind, FlushedSeries, IntervalParseError, PartitionInterval, PartitionMeta,
    PartitionState, SegmentKind, SegmentRef,
};
pub use series::{BatteryState, LiteId, SeriesCatalog, SeriesId, SeriesKey};
pub use sync::{LogWalBatch, TimeseriesDelta, TimeseriesWalBatch};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn series_key_sorted_tags() {
        let k1 = SeriesKey::new(
            "cpu",
            vec![("host".into(), "a".into()), ("dc".into(), "us".into())],
        );
        let k2 = SeriesKey::new(
            "cpu",
            vec![("dc".into(), "us".into()), ("host".into(), "a".into())],
        );
        assert_eq!(k1, k2);
        assert_eq!(k1.to_series_id(0), k2.to_series_id(0));
    }

    #[test]
    fn series_catalog_resolve() {
        let mut catalog = SeriesCatalog::new();
        let k = SeriesKey::new("cpu", vec![("host".into(), "prod-1".into())]);
        let id1 = catalog.resolve(&k);
        let id2 = catalog.resolve(&k);
        assert_eq!(id1, id2);
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn series_catalog_different_keys() {
        let mut catalog = SeriesCatalog::new();
        let k1 = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let k2 = SeriesKey::new("mem", vec![("host".into(), "a".into())]);
        let id1 = catalog.resolve(&k1);
        let id2 = catalog.resolve(&k2);
        assert_ne!(id1, id2);
        assert_eq!(catalog.len(), 2);
    }

    #[test]
    fn symbol_dictionary_basic() {
        let mut dict = SymbolDictionary::new();
        assert_eq!(dict.resolve("us-east-1", 100_000), Some(0));
        assert_eq!(dict.resolve("us-west-2", 100_000), Some(1));
        assert_eq!(dict.resolve("us-east-1", 100_000), Some(0));
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.get(0), Some("us-east-1"));
        assert_eq!(dict.get(1), Some("us-west-2"));
        assert_eq!(dict.get_id("us-east-1"), Some(0));
    }

    #[test]
    fn symbol_dictionary_cardinality_breaker() {
        let mut dict = SymbolDictionary::new();
        for i in 0..100 {
            assert!(dict.resolve(&format!("val-{i}"), 100).is_some());
        }
        assert!(dict.resolve("one-too-many", 100).is_none());
        assert_eq!(dict.len(), 100);
    }

    #[test]
    fn symbol_dictionary_merge() {
        let mut d1 = SymbolDictionary::new();
        d1.resolve("a", 1000);
        d1.resolve("b", 1000);

        let mut d2 = SymbolDictionary::new();
        d2.resolve("b", 1000);
        d2.resolve("c", 1000);

        let remap = d1.merge(&d2, 1000);
        assert_eq!(d1.len(), 3);
        assert_eq!(remap[0], d1.get_id("b").unwrap());
        assert_eq!(remap[1], d1.get_id("c").unwrap());
    }

    #[test]
    fn partition_interval_parse() {
        assert_eq!(
            PartitionInterval::parse("1h").unwrap(),
            PartitionInterval::Duration(3_600_000)
        );
        assert_eq!(
            PartitionInterval::parse("3d").unwrap(),
            PartitionInterval::Duration(3 * 86_400_000)
        );
        assert_eq!(
            PartitionInterval::parse("2w").unwrap(),
            PartitionInterval::Duration(2 * 604_800_000)
        );
        assert_eq!(
            PartitionInterval::parse("1M").unwrap(),
            PartitionInterval::Month
        );
        assert_eq!(
            PartitionInterval::parse("1y").unwrap(),
            PartitionInterval::Year
        );
        assert_eq!(
            PartitionInterval::parse("AUTO").unwrap(),
            PartitionInterval::Auto
        );
        assert_eq!(
            PartitionInterval::parse("UNBOUNDED").unwrap(),
            PartitionInterval::Unbounded
        );
        assert!(matches!(
            PartitionInterval::parse("0h"),
            Err(IntervalParseError::ZeroInterval)
        ));
        assert!(matches!(
            PartitionInterval::parse("2M"),
            Err(IntervalParseError::UnsupportedCalendar { .. })
        ));
    }

    #[test]
    fn partition_interval_display_roundtrip() {
        let cases = ["1h", "3d", "2w", "1M", "1y", "AUTO", "UNBOUNDED"];
        for s in cases {
            let parsed = PartitionInterval::parse(s).unwrap();
            let displayed = parsed.to_string();
            let reparsed = PartitionInterval::parse(&displayed).unwrap();
            assert_eq!(parsed, reparsed, "roundtrip failed for {s}");
        }
    }

    #[test]
    fn partition_meta_queryable() {
        let meta = PartitionMeta {
            min_ts: 1000,
            max_ts: 2000,
            row_count: 500,
            size_bytes: 1024,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 42,
            column_stats: HashMap::new(),
            max_system_ts: 0,
        };
        assert!(meta.is_queryable());
        assert!(meta.overlaps(&TimeRange::new(1500, 2500)));
        assert!(!meta.overlaps(&TimeRange::new(3000, 4000)));
    }

    #[test]
    fn partition_meta_not_queryable_when_deleted() {
        let meta = PartitionMeta {
            min_ts: 0,
            max_ts: 0,
            row_count: 0,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Deleted,
            interval_ms: 0,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 0,
        };
        assert!(!meta.is_queryable());
    }

    #[test]
    fn tiered_config_validation() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        assert!(cfg.validate().is_ok());

        cfg.merge_count = 1;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "merge_count");

        cfg.merge_count = 10;
        cfg.retention_period_ms = 1000;
        cfg.archive_after_ms = 2000;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "retention_period");
    }

    #[test]
    fn time_range_overlap() {
        let r1 = TimeRange::new(100, 200);
        let r2 = TimeRange::new(150, 250);
        let r3 = TimeRange::new(300, 400);
        assert!(r1.overlaps(&r2));
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn timeseries_delta_serialization() {
        let delta = TimeseriesDelta {
            source_id: "clxyz1234test".into(),
            series_id: 12345,
            series_key: SeriesKey::new("cpu", vec![("host".into(), "prod".into())]),
            min_ts: 1000,
            max_ts: 2000,
            encoded_block: vec![1, 2, 3, 4],
            sample_count: 100,
        };
        let json = sonic_rs::to_string(&delta).unwrap();
        let back: TimeseriesDelta = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.source_id, "clxyz1234test");
        assert_eq!(back.series_id, 12345);
        assert_eq!(back.sample_count, 100);
    }
}
