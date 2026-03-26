//! Continuous aggregate manager: registry, lifecycle, and query.
//!
//! Lives on the Data Plane (!Send). One per core. Manages all continuous
//! aggregates for this core's timeseries collections.

use std::collections::HashMap;

use super::definition::{ContinuousAggregateDef, RefreshPolicy};
use super::partial::PartialAggregate;
use super::refresh;
use super::watermark::WatermarkState;
use crate::engine::timeseries::columnar_memtable::ColumnarDrainResult;

/// Manages all continuous aggregates for a timeseries engine instance.
pub struct ContinuousAggregateManager {
    /// Registered aggregate definitions, keyed by aggregate name.
    definitions: HashMap<String, ContinuousAggregateDef>,
    /// Per-aggregate watermark state.
    watermarks: HashMap<String, WatermarkState>,
    /// Materialized aggregate data:
    /// `agg_name → (bucket_ts, group_key) → PartialAggregate`.
    materialized: HashMap<String, HashMap<(i64, Vec<u32>), PartialAggregate>>,
    /// Dependency graph: `source → [aggregates that depend on it]`.
    dependencies: HashMap<String, Vec<String>>,
}

impl ContinuousAggregateManager {
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            watermarks: HashMap::new(),
            materialized: HashMap::new(),
            dependencies: HashMap::new(),
        }
    }

    // -- Registration --

    /// Register a new continuous aggregate.
    pub fn register(&mut self, def: ContinuousAggregateDef) {
        let source = def.source.clone();
        let name = def.name.clone();

        self.watermarks.entry(name.clone()).or_default();
        self.materialized.entry(name.clone()).or_default();
        self.dependencies
            .entry(source)
            .or_default()
            .push(name.clone());
        self.definitions.insert(name, def);
    }

    /// Remove a continuous aggregate.
    pub fn unregister(&mut self, name: &str) {
        if let Some(def) = self.definitions.remove(name) {
            self.watermarks.remove(name);
            self.materialized.remove(name);
            if let Some(deps) = self.dependencies.get_mut(&def.source) {
                deps.retain(|n| n != name);
            }
        }
    }

    /// Get a registered definition.
    pub fn get_definition(&self, name: &str) -> Option<&ContinuousAggregateDef> {
        self.definitions.get(name)
    }

    /// Get watermark state for an aggregate.
    pub fn get_watermark(&self, name: &str) -> Option<&WatermarkState> {
        self.watermarks.get(name)
    }

    /// Number of registered aggregates.
    pub fn aggregate_count(&self) -> usize {
        self.definitions.len()
    }

    // -- Flush-triggered refresh --

    /// Process a flush event from a source collection.
    ///
    /// Finds all aggregates that depend on `source_collection` with
    /// `RefreshPolicy::OnFlush` and refreshes them incrementally.
    ///
    /// Returns the names of aggregates that were refreshed.
    pub fn on_flush(
        &mut self,
        source_collection: &str,
        drain: &ColumnarDrainResult,
        now_ms: i64,
    ) -> Vec<String> {
        let agg_names: Vec<String> = self
            .dependencies
            .get(source_collection)
            .cloned()
            .unwrap_or_default();

        let mut refreshed = Vec::new();

        for agg_name in &agg_names {
            let Some(def) = self.definitions.get(agg_name) else {
                continue;
            };
            if def.refresh_policy != RefreshPolicy::OnFlush || def.stale {
                continue;
            }

            let def_clone = def.clone();
            let watermark = self.watermarks.get(agg_name).cloned().unwrap_or_default();
            let mat = self.materialized.entry(agg_name.clone()).or_default();

            let result = refresh::refresh_from_drain(&def_clone, drain, &watermark, mat);

            // Update watermark.
            if let Some(wm) = self.watermarks.get_mut(agg_name) {
                wm.advance(result.max_ts, result.rows_processed, now_ms);
                if let Some(o3_ts) = result.o3_min_ts {
                    wm.record_o3(o3_ts);
                }
            }

            refreshed.push(agg_name.clone());
        }

        // Multi-tier chaining: check if refreshed aggregates have downstream dependents.
        let mut chain_refreshed = Vec::new();
        for name in &refreshed {
            if let Some(downstream) = self.dependencies.get(name).cloned() {
                for ds_name in &downstream {
                    if let Some(ds_def) = self.definitions.get(ds_name)
                        && ds_def.refresh_policy == RefreshPolicy::OnFlush
                        && !ds_def.stale
                    {
                        chain_refreshed.push(ds_name.clone());
                    }
                }
            }
        }
        refreshed.extend(chain_refreshed);
        refreshed
    }

    /// Manually refresh an aggregate (for Manual or Periodic policies).
    pub fn manual_refresh(&mut self, agg_name: &str, drain: &ColumnarDrainResult, now_ms: i64) {
        let Some(def) = self.definitions.get(agg_name).cloned() else {
            return;
        };
        let watermark = self.watermarks.get(agg_name).cloned().unwrap_or_default();
        let mat = self.materialized.entry(agg_name.to_string()).or_default();

        let result = refresh::refresh_from_drain(&def, drain, &watermark, mat);

        if let Some(wm) = self.watermarks.get_mut(agg_name) {
            wm.advance(result.max_ts, result.rows_processed, now_ms);
            if let Some(o3_ts) = result.o3_min_ts {
                wm.record_o3(o3_ts);
            }
        }
    }

    // -- Query --

    /// Get materialized results for an aggregate, sorted by bucket.
    pub fn get_materialized(&self, agg_name: &str) -> Option<Vec<&PartialAggregate>> {
        self.materialized.get(agg_name).map(|m| {
            let mut results: Vec<&PartialAggregate> = m.values().collect();
            results.sort_by_key(|p| p.bucket_ts);
            results
        })
    }

    /// Get materialized results within a time range.
    pub fn get_materialized_range(
        &self,
        agg_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Option<Vec<&PartialAggregate>> {
        self.materialized.get(agg_name).map(|m| {
            let mut results: Vec<&PartialAggregate> = m
                .values()
                .filter(|p| p.bucket_ts >= start_ms && p.bucket_ts <= end_ms)
                .collect();
            results.sort_by_key(|p| p.bucket_ts);
            results
        })
    }

    // -- Retention --

    /// Apply retention: remove materialized buckets older than retention period.
    pub fn apply_retention(&mut self, now_ms: i64) -> usize {
        let mut total_removed = 0;
        let defs: Vec<(String, u64)> = self
            .definitions
            .values()
            .map(|d| (d.name.clone(), d.retention_period_ms))
            .collect();

        for (name, retention_ms) in defs {
            if retention_ms == 0 {
                continue;
            }
            let cutoff = now_ms - retention_ms as i64;
            if let Some(mat) = self.materialized.get_mut(&name) {
                let before = mat.len();
                mat.retain(|&(bucket_ts, _), _| bucket_ts > cutoff);
                total_removed += before - mat.len();
            }
        }
        total_removed
    }

    // -- Schema invalidation --

    /// Mark aggregates as stale after source schema change.
    pub fn invalidate_for_source(&mut self, source: &str) {
        if let Some(agg_names) = self.dependencies.get(source).cloned() {
            for name in &agg_names {
                if let Some(def) = self.definitions.get_mut(name.as_str()) {
                    def.stale = true;
                }
            }
        }
    }

    /// Mark a specific aggregate as stale.
    pub fn invalidate(&mut self, name: &str) {
        if let Some(def) = self.definitions.get_mut(name) {
            def.stale = true;
        }
    }

    // -- Introspection --

    /// List all registered aggregates with status.
    pub fn list_aggregates(&self) -> Vec<AggregateInfo> {
        self.definitions
            .values()
            .map(|def| {
                let wm = self.watermarks.get(&def.name);
                let bucket_count = self
                    .materialized
                    .get(&def.name)
                    .map_or(0, |m| m.len() as u64);
                AggregateInfo {
                    name: def.name.clone(),
                    source: def.source.clone(),
                    bucket_interval: def.bucket_interval.clone(),
                    refresh_policy: def.refresh_policy.clone(),
                    watermark_ts: wm.map_or(i64::MIN, |w| w.watermark_ts),
                    rows_aggregated: wm.map_or(0, |w| w.rows_aggregated),
                    materialized_buckets: bucket_count,
                    stale: def.stale,
                }
            })
            .collect()
    }
}

impl Default for ContinuousAggregateManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary info for `SHOW CONTINUOUS AGGREGATES`.
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    pub name: String,
    pub source: String,
    pub bucket_interval: String,
    pub refresh_policy: RefreshPolicy,
    pub watermark_ts: i64,
    pub rows_aggregated: u64,
    pub materialized_buckets: u64,
    pub stale: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use crate::engine::timeseries::continuous_agg::definition::{
        AggFunction, AggregateExpr, RefreshPolicy,
    };
    use crate::engine::timeseries::time_bucket;
    use nodedb_types::timeseries::MetricSample;

    fn test_memtable_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn make_agg_def(name: &str, source: &str, bucket: &str) -> ContinuousAggregateDef {
        ContinuousAggregateDef {
            name: name.into(),
            source: source.into(),
            bucket_interval: bucket.into(),
            bucket_interval_ms: time_bucket::parse_interval_ms(bucket).unwrap(),
            group_by: vec![],
            aggregates: vec![
                AggregateExpr {
                    function: AggFunction::Avg,
                    source_column: "value".into(),
                    output_column: "value_avg".into(),
                },
                AggregateExpr {
                    function: AggFunction::Count,
                    source_column: "*".into(),
                    output_column: "cnt".into(),
                },
            ],
            refresh_policy: RefreshPolicy::OnFlush,
            retention_period_ms: 0,
            stale: false,
        }
    }

    fn make_drain(count: usize, start_ts: i64, interval_ms: i64) -> ColumnarDrainResult {
        let mut mt = ColumnarMemtable::new_metric(test_memtable_config());
        for i in 0..count {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: start_ts + i as i64 * interval_ms,
                    value: 50.0 + (i % 100) as f64,
                },
            );
        }
        mt.drain()
    }

    #[test]
    fn register_and_list() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));
        mgr.register(make_agg_def("metrics_1h", "metrics_1m", "1h"));

        assert_eq!(mgr.aggregate_count(), 2);
        assert_eq!(mgr.list_aggregates().len(), 2);
    }

    #[test]
    fn unregister() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));
        mgr.unregister("metrics_1m");
        assert_eq!(mgr.aggregate_count(), 0);
    }

    #[test]
    fn incremental_refresh() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        // 6000 samples at 1s intervals = 100 minutes.
        let drain = make_drain(6000, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush("metrics", &drain, 1_700_000_100_000);
        assert_eq!(refreshed, vec!["metrics_1m"]);

        let results = mgr.get_materialized("metrics_1m").unwrap();
        assert!(results.len() >= 90);

        let wm = mgr.get_watermark("metrics_1m").unwrap();
        assert!(wm.watermark_ts > 1_700_000_000_000);
        assert_eq!(wm.rows_aggregated, 6000);
    }

    #[test]
    fn incremental_accumulates() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain1 = make_drain(1000, 1_700_000_000_000, 1000);
        mgr.on_flush("metrics", &drain1, 1_700_000_001_000);
        let count1 = mgr.get_materialized("metrics_1m").unwrap().len();

        let drain2 = make_drain(1000, 1_700_000_000_000, 1000);
        mgr.on_flush("metrics", &drain2, 1_700_000_002_000);

        // Same time range → same bucket count, but counts doubled.
        let results = mgr.get_materialized("metrics_1m").unwrap();
        assert_eq!(results.len(), count1);
        let mid = &results[results.len() / 2];
        assert!(mid.count > 60); // doubled from ~30 each.
    }

    #[test]
    fn group_by_tags() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut def = make_agg_def("metrics_1m", "metrics", "1m");
        def.group_by = vec!["host".into()];
        mgr.register(def);

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, test_memtable_config());
        for i in 0..600 {
            let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
            mt.ingest_row(
                (i % 2) as u64,
                &[
                    ColumnValue::Timestamp(1_700_000_000_000 + i as i64 * 1000),
                    ColumnValue::Float64(50.0 + i as f64),
                    ColumnValue::Symbol(host),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        mgr.on_flush("metrics", &drain, 1_700_000_001_000);

        let results = mgr.get_materialized("metrics_1m").unwrap();
        let unique_keys: std::collections::HashSet<&Vec<u32>> =
            results.iter().map(|p| &p.group_key).collect();
        assert_eq!(unique_keys.len(), 2);
    }

    #[test]
    fn o3_detection() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain1 = make_drain(100, 1_700_000_060_000, 1000);
        mgr.on_flush("metrics", &drain1, 1_700_000_200_000);

        // O3: older data below watermark.
        let drain2 = make_drain(100, 1_700_000_000_000, 1000);
        mgr.on_flush("metrics", &drain2, 1_700_000_300_000);

        let wm = mgr.get_watermark("metrics_1m").unwrap();
        assert!(wm.o3_watermark_ts.is_some());
    }

    #[test]
    fn retention() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut def = make_agg_def("metrics_1m", "metrics", "1m");
        def.retention_period_ms = 600_000; // 10 minutes
        mgr.register(def);

        let drain = make_drain(1200, 1_700_000_000_000, 1000);
        mgr.on_flush("metrics", &drain, 1_700_000_000_000);

        let before = mgr.get_materialized("metrics_1m").unwrap().len();
        let now = 1_700_000_000_000 + 15 * 60_000;
        let removed = mgr.apply_retention(now);
        assert!(removed > 0);
        assert!(mgr.get_materialized("metrics_1m").unwrap().len() < before);
    }

    #[test]
    fn invalidation() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        mgr.invalidate_for_source("metrics");
        assert!(mgr.get_definition("metrics_1m").unwrap().stale);

        let drain = make_drain(100, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush("metrics", &drain, 1_700_000_100_000);
        assert!(refreshed.is_empty()); // Stale → skipped.
    }

    #[test]
    fn manual_refresh_policy() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut def = make_agg_def("metrics_1m", "metrics", "1m");
        def.refresh_policy = RefreshPolicy::Manual;
        mgr.register(def);

        let drain = make_drain(100, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush("metrics", &drain, 1_700_000_100_000);
        assert!(refreshed.is_empty()); // Manual → not triggered by flush.

        mgr.manual_refresh("metrics_1m", &drain, 1_700_000_100_000);
        assert!(!mgr.get_materialized("metrics_1m").unwrap().is_empty());
    }

    #[test]
    fn time_range_query() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain = make_drain(3600, 1_700_000_000_000, 1000);
        mgr.on_flush("metrics", &drain, 1_700_000_000_000);

        let start = 1_700_000_000_000 + 20 * 60_000;
        let end = start + 10 * 60_000;
        let results = mgr
            .get_materialized_range("metrics_1m", start, end)
            .unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 11);
    }
}
