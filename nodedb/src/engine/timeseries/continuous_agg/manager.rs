// SPDX-License-Identifier: BUSL-1.1

//! Continuous aggregate manager: registry, lifecycle, and query.
//!
//! Lives on the Data Plane (!Send). One per core. Manages all continuous
//! aggregates for this core's timeseries collections.

use std::collections::HashMap;

use super::definition::{ContinuousAggregateDef, RefreshPolicy};
use super::partial::PartialAggregate;
use super::refresh::{self, RefreshResult};
use super::rollup;
use super::watermark::WatermarkState;
use crate::engine::timeseries::columnar_memtable::ColumnarDrainResult;

/// Key scoping every continuous-aggregate map by database: `(database_id, name)`
/// (or `(database_id, source)` for the dependency graph).
type AggKey = (u64, String);

/// Materialized partials for one aggregate:
/// `(bucket_ts, group_key) → PartialAggregate`.
type MaterializedBuckets = refresh::Buckets;

/// Whether an aggregate sourced from another aggregate takes that
/// aggregate's refreshes. A `Manual` or `Periodic` aggregate refreshes only
/// on its own trigger.
fn takes_upstream_refresh(policy: &RefreshPolicy) -> bool {
    matches!(policy, RefreshPolicy::OnFlush | RefreshPolicy::OnSeal)
}

/// Whether two definitions of one aggregate build the same partial state.
/// A change to any of these makes the materialized buckets of the old
/// definition meaningless under the new one.
fn same_shape(a: &ContinuousAggregateDef, b: &ContinuousAggregateDef) -> bool {
    a.source == b.source
        && a.bucket_interval_ms == b.bucket_interval_ms
        && a.group_by == b.group_by
        && a.aggregates == b.aggregates
}

/// Manages all continuous aggregates for a timeseries engine instance.
///
/// Every map is scoped by `(database_id, name)` (or `(database_id, source)`
/// for the dependency graph) so an aggregate named identically in two
/// databases never collides and never materializes against the wrong
/// database's storage.
pub struct ContinuousAggregateManager {
    /// Registered aggregate definitions, keyed by `(database_id, agg_name)`.
    definitions: HashMap<AggKey, ContinuousAggregateDef>,
    /// Per-aggregate watermark state, keyed by `(database_id, agg_name)`.
    watermarks: HashMap<AggKey, WatermarkState>,
    /// Materialized aggregate data:
    /// `(database_id, agg_name) → (bucket_ts, group_key) → PartialAggregate`.
    materialized: HashMap<AggKey, MaterializedBuckets>,
    /// Dependency graph: `(database_id, source) → [aggregate names that
    /// depend on it within that database]`.
    dependencies: HashMap<AggKey, Vec<String>>,
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

    /// Register a new continuous aggregate. The database scope is taken
    /// from `def.database_id` so every internal map is keyed consistently.
    ///
    /// Idempotent: boot re-registration and a replayed post-apply both
    /// register a definition the core can already hold. A repeated dependency
    /// edge refreshes the aggregate twice per flush.
    ///
    /// A definition of another shape (source, bucket, GROUP BY, or
    /// aggregates) replaces the old one with no materialized buckets and a
    /// default watermark: buckets built for the old shape do not hold the
    /// new one's columns.
    pub fn register(&mut self, def: ContinuousAggregateDef) {
        let database_id = def.database_id;
        let source = def.source.clone();
        let name = def.name.clone();
        let key = (database_id, name.clone());

        if let Some(previous) = self.definitions.get(&key) {
            if previous.source != source
                && let Some(deps) = self
                    .dependencies
                    .get_mut(&(database_id, previous.source.clone()))
            {
                deps.retain(|n| n != &name);
            }
            if !same_shape(previous, &def) {
                self.materialized.remove(&key);
                self.watermarks.remove(&key);
            }
        }
        self.watermarks
            .entry((database_id, name.clone()))
            .or_default();
        self.materialized
            .entry((database_id, name.clone()))
            .or_default();
        let deps = self.dependencies.entry((database_id, source)).or_default();
        if !deps.contains(&name) {
            deps.push(name.clone());
        }
        self.definitions.insert((database_id, name), def);
    }

    /// Remove a continuous aggregate.
    pub fn unregister(&mut self, database_id: u64, name: &str) {
        let key = (database_id, name.to_string());
        if let Some(def) = self.definitions.remove(&key) {
            self.watermarks.remove(&key);
            self.materialized.remove(&key);
            if let Some(deps) = self
                .dependencies
                .get_mut(&(database_id, def.source.clone()))
            {
                deps.retain(|n| n != name);
            }
        }
    }

    /// Get a registered definition.
    pub fn get_definition(&self, database_id: u64, name: &str) -> Option<&ContinuousAggregateDef> {
        self.definitions.get(&(database_id, name.to_string()))
    }

    /// Get watermark state for an aggregate.
    pub fn get_watermark(&self, database_id: u64, name: &str) -> Option<&WatermarkState> {
        self.watermarks.get(&(database_id, name.to_string()))
    }

    /// Number of registered aggregates.
    pub fn aggregate_count(&self) -> usize {
        self.definitions.len()
    }

    // -- Flush-triggered refresh --

    /// Process a flush event from a source collection.
    ///
    /// Finds all aggregates that depend on `source_collection` with
    /// `RefreshPolicy::OnFlush` and refreshes them incrementally, then rolls
    /// each refresh up into the aggregates sourced from it, transitively.
    ///
    /// Returns the names of aggregates that were refreshed.
    pub fn on_flush(
        &mut self,
        database_id: u64,
        source_collection: &str,
        drain: &ColumnarDrainResult,
        now_ms: i64,
    ) -> Vec<String> {
        let agg_names: Vec<String> = self
            .dependencies
            .get(&(database_id, source_collection.to_string()))
            .cloned()
            .unwrap_or_default();

        let mut refreshed = Vec::new();
        for agg_name in &agg_names {
            let key = (database_id, agg_name.clone());
            let Some(def) = self.definitions.get(&key) else {
                continue;
            };
            if def.refresh_policy != RefreshPolicy::OnFlush || def.stale {
                continue;
            }
            let watermark = self.watermarks.get(&key).cloned().unwrap_or_default();
            let result = refresh::refresh_from_drain(def, drain, &watermark);
            self.apply_refresh(database_id, agg_name, result, now_ms, &mut refreshed);
        }
        refreshed
    }

    /// Manually refresh an aggregate (for Manual or Periodic policies), and
    /// roll the refresh up into the aggregates sourced from it.
    pub fn manual_refresh(
        &mut self,
        database_id: u64,
        agg_name: &str,
        drain: &ColumnarDrainResult,
        now_ms: i64,
    ) {
        let key = (database_id, agg_name.to_string());
        let Some(def) = self.definitions.get(&key) else {
            return;
        };
        let watermark = self.watermarks.get(&key).cloned().unwrap_or_default();
        let result = refresh::refresh_from_drain(def, drain, &watermark);
        let mut refreshed = Vec::new();
        self.apply_refresh(database_id, agg_name, result, now_ms, &mut refreshed);
    }

    /// Merge `result` into `name`'s materialized buckets and advance its
    /// watermark. Then roll the refresh delta up into every non-stale
    /// aggregate sourced from `name` that takes upstream refreshes, and so on
    /// down the chain. Each aggregate takes one refresh per call, so a chain
    /// that loops back stops at the first repeat. Appends every refreshed
    /// name to `refreshed`.
    fn apply_refresh(
        &mut self,
        database_id: u64,
        name: &str,
        result: RefreshResult,
        now_ms: i64,
        refreshed: &mut Vec<String>,
    ) {
        let mut pending = vec![(name.to_string(), result)];
        while let Some((name, result)) = pending.pop() {
            if refreshed.contains(&name) {
                continue;
            }
            let key = (database_id, name);
            let Some(def) = self.definitions.get(&key) else {
                continue;
            };
            let RefreshResult {
                rows_processed,
                max_ts,
                o3_min_ts,
                delta,
            } = result;

            if let Some(downstream) = self.dependencies.get(&key) {
                for ds_name in downstream {
                    let Some(ds_def) = self.definitions.get(&(database_id, ds_name.clone())) else {
                        continue;
                    };
                    if ds_def.stale || !takes_upstream_refresh(&ds_def.refresh_policy) {
                        continue;
                    }
                    let rolled = RefreshResult {
                        rows_processed,
                        max_ts,
                        o3_min_ts,
                        delta: rollup::rollup_delta(def, ds_def, &delta),
                    };
                    pending.push((ds_name.clone(), rolled));
                }
            }

            refresh::merge_delta(self.materialized.entry(key.clone()).or_default(), delta);
            let wm = self.watermarks.entry(key.clone()).or_default();
            wm.advance(max_ts, rows_processed, now_ms);
            if let Some(o3_ts) = o3_min_ts {
                wm.record_o3(o3_ts);
            }
            refreshed.push(key.1);
        }
    }

    // -- Query --

    /// Get materialized results for an aggregate, sorted by bucket.
    pub fn get_materialized(
        &self,
        database_id: u64,
        agg_name: &str,
    ) -> Option<Vec<&PartialAggregate>> {
        self.materialized
            .get(&(database_id, agg_name.to_string()))
            .map(|m| {
                let mut results: Vec<&PartialAggregate> = m.values().collect();
                results.sort_by_key(|p| p.bucket_ts);
                results
            })
    }

    /// Get materialized results within a time range.
    pub fn get_materialized_range(
        &self,
        database_id: u64,
        agg_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Option<Vec<&PartialAggregate>> {
        self.materialized
            .get(&(database_id, agg_name.to_string()))
            .map(|m| {
                let mut results: Vec<&PartialAggregate> = m
                    .values()
                    .filter(|p| p.bucket_ts >= start_ms && p.bucket_ts <= end_ms)
                    .collect();
                results.sort_by_key(|p| p.bucket_ts);
                results
            })
    }

    // -- Retention --

    /// Apply retention across every registered aggregate (all databases on
    /// this core): remove materialized buckets older than each aggregate's
    /// retention period. Keys are `(database_id, agg_name)`.
    pub fn apply_retention(&mut self, now_ms: i64) -> usize {
        let mut total_removed = 0;
        let defs: Vec<((u64, String), u64)> = self
            .definitions
            .iter()
            .map(|(key, d)| (key.clone(), d.retention_period_ms))
            .collect();

        for (key, retention_ms) in defs {
            if retention_ms == 0 {
                continue;
            }
            let cutoff = now_ms - retention_ms as i64;
            if let Some(mat) = self.materialized.get_mut(&key) {
                let before = mat.len();
                mat.retain(|&(bucket_ts, _), _| bucket_ts > cutoff);
                total_removed += before - mat.len();
            }
        }
        total_removed
    }

    // -- Schema invalidation --

    /// Reset every aggregate that depends on `source`, directly or through a
    /// chain, to its freshly-registered state: no materialized buckets and a
    /// default watermark. The definitions stay registered and active, so the
    /// next flush of the source rebuilds them from the rows it carries. This
    /// is what a `TRUNCATE` of the source owes its aggregates.
    pub fn reset_for_source(&mut self, database_id: u64, source: &str) {
        let mut pending: Vec<String> = self
            .dependencies
            .get(&(database_id, source.to_string()))
            .cloned()
            .unwrap_or_default();
        while let Some(name) = pending.pop() {
            let key = (database_id, name.clone());
            if !self.definitions.contains_key(&key) {
                continue;
            }
            self.materialized
                .insert(key.clone(), MaterializedBuckets::default());
            self.watermarks.insert(key, WatermarkState::default());
            if let Some(downstream) = self.dependencies.get(&(database_id, name)) {
                pending.extend(downstream.iter().cloned());
            }
        }
    }

    /// Mark aggregates as stale after source schema change.
    pub fn invalidate_for_source(&mut self, database_id: u64, source: &str) {
        if let Some(agg_names) = self
            .dependencies
            .get(&(database_id, source.to_string()))
            .cloned()
        {
            for name in &agg_names {
                if let Some(def) = self.definitions.get_mut(&(database_id, name.clone())) {
                    def.stale = true;
                }
            }
        }
    }

    /// Mark a specific aggregate as stale.
    pub fn invalidate(&mut self, database_id: u64, name: &str) {
        if let Some(def) = self.definitions.get_mut(&(database_id, name.to_string())) {
            def.stale = true;
        }
    }

    // -- Introspection --

    /// List all registered aggregates with status.
    pub fn list_aggregates(&self) -> Vec<AggregateInfo> {
        self.definitions
            .values()
            .map(|def| {
                let key = (def.database_id, def.name.clone());
                let wm = self.watermarks.get(&key);
                let bucket_count = self.materialized.get(&key).map_or(0, |m| m.len() as u64);
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use crate::engine::timeseries::continuous_agg::definition::{
        AggFunction, AggregateExpr, RefreshPolicy,
    };
    use crate::engine::timeseries::time_bucket;
    use nodedb_types::Value;
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
            database_id: 0,
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

    /// A second register of the same definition refreshes it once per flush.
    #[test]
    fn repeated_register_refreshes_once_per_flush() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain = make_drain(600, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush(0, "metrics", &drain, 1_700_000_100_000);
        assert_eq!(refreshed, vec!["metrics_1m"]);
        let wm = mgr.get_watermark(0, "metrics_1m").unwrap();
        assert_eq!(wm.rows_aggregated, 600);
    }

    #[test]
    fn unregister() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));
        mgr.unregister(0, "metrics_1m");
        assert_eq!(mgr.aggregate_count(), 0);
    }

    #[test]
    fn incremental_refresh() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        // 6000 samples at 1s intervals = 100 minutes.
        let drain = make_drain(6000, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush(0, "metrics", &drain, 1_700_000_100_000);
        assert_eq!(refreshed, vec!["metrics_1m"]);

        let results = mgr.get_materialized(0, "metrics_1m").unwrap();
        assert!(results.len() >= 90);

        let wm = mgr.get_watermark(0, "metrics_1m").unwrap();
        assert!(wm.watermark_ts > 1_700_000_000_000);
        assert_eq!(wm.rows_aggregated, 6000);
    }

    /// A truncate of the source empties the aggregate and rewinds its
    /// watermark, and leaves it active for the next flush.
    #[test]
    fn reset_for_source_empties_buckets_and_keeps_the_aggregate_live() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));
        let drain = make_drain(1000, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain, 1_700_000_001_000);
        assert!(!mgr.get_materialized(0, "metrics_1m").unwrap().is_empty());

        mgr.reset_for_source(0, "metrics");

        assert!(mgr.get_materialized(0, "metrics_1m").unwrap().is_empty());
        let wm = mgr.get_watermark(0, "metrics_1m").unwrap();
        assert_eq!(wm.rows_aggregated, 0);
        assert!(!mgr.get_definition(0, "metrics_1m").unwrap().stale);

        let refreshed = mgr.on_flush(0, "metrics", &drain, 1_700_000_002_000);
        assert_eq!(refreshed, vec!["metrics_1m"]);
        assert!(!mgr.get_materialized(0, "metrics_1m").unwrap().is_empty());
    }

    #[test]
    fn incremental_accumulates() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain1 = make_drain(1000, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain1, 1_700_000_001_000);
        let count1 = mgr.get_materialized(0, "metrics_1m").unwrap().len();

        let drain2 = make_drain(1000, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain2, 1_700_000_002_000);

        // Same time range → same bucket count, but counts doubled.
        let results = mgr.get_materialized(0, "metrics_1m").unwrap();
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
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
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
                    ColumnValue::Symbol(host.to_string()),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        mgr.on_flush(0, "metrics", &drain, 1_700_000_001_000);

        let results = mgr.get_materialized(0, "metrics_1m").unwrap();
        let unique_keys: std::collections::HashSet<&Vec<u32>> =
            results.iter().map(|p| &p.group_key).collect();
        assert_eq!(unique_keys.len(), 2);
    }

    #[test]
    fn o3_detection() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain1 = make_drain(100, 1_700_000_060_000, 1000);
        mgr.on_flush(0, "metrics", &drain1, 1_700_000_200_000);

        // O3: older data below watermark.
        let drain2 = make_drain(100, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain2, 1_700_000_300_000);

        let wm = mgr.get_watermark(0, "metrics_1m").unwrap();
        assert!(wm.o3_watermark_ts.is_some());
    }

    #[test]
    fn retention() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut def = make_agg_def("metrics_1m", "metrics", "1m");
        def.retention_period_ms = 600_000; // 10 minutes
        mgr.register(def);

        let drain = make_drain(1200, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain, 1_700_000_000_000);

        let before = mgr.get_materialized(0, "metrics_1m").unwrap().len();
        let now = 1_700_000_000_000 + 15 * 60_000;
        let removed = mgr.apply_retention(now);
        assert!(removed > 0);
        assert!(mgr.get_materialized(0, "metrics_1m").unwrap().len() < before);
    }

    #[test]
    fn invalidation() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        mgr.invalidate_for_source(0, "metrics");
        assert!(mgr.get_definition(0, "metrics_1m").unwrap().stale);

        let drain = make_drain(100, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush(0, "metrics", &drain, 1_700_000_100_000);
        assert!(refreshed.is_empty()); // Stale → skipped.
    }

    #[test]
    fn manual_refresh_policy() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut def = make_agg_def("metrics_1m", "metrics", "1m");
        def.refresh_policy = RefreshPolicy::Manual;
        mgr.register(def);

        let drain = make_drain(100, 1_700_000_000_000, 1000);
        let refreshed = mgr.on_flush(0, "metrics", &drain, 1_700_000_100_000);
        assert!(refreshed.is_empty()); // Manual → not triggered by flush.

        mgr.manual_refresh(0, "metrics_1m", &drain, 1_700_000_100_000);
        assert!(!mgr.get_materialized(0, "metrics_1m").unwrap().is_empty());
    }

    #[test]
    fn time_range_query() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(make_agg_def("metrics_1m", "metrics", "1m"));

        let drain = make_drain(3600, 1_700_000_000_000, 1000);
        mgr.on_flush(0, "metrics", &drain, 1_700_000_000_000);

        let start = 1_700_000_000_000 + 20 * 60_000;
        let end = start + 10 * 60_000;
        let results = mgr
            .get_materialized_range(0, "metrics_1m", start, end)
            .unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 11);
    }

    // ── Exactness: materialized buckets equal the ad-hoc aggregate ──

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;
    const T0: i64 = 1_700_000_040_000;

    /// `(timestamp_ms, v)` rows: integers one apart above 2^53, nanosecond
    /// timestamps one tick apart, and `i64::MAX` twice so a bucket total
    /// leaves `i64`.
    fn exact_rows() -> Vec<(i64, i64)> {
        vec![
            (T0, ABOVE),
            (T0 + 1_000, AT),
            (T0 + 2_000, 1_700_000_000_000_000_002),
            (T0 + 61_000, 1_700_000_000_000_000_001),
            (T0 + 62_000, i64::MAX),
            (T0 + 63_000, i64::MAX),
            (T0 + 3_600_000, 1_700_000_000_000_000_003),
        ]
    }

    fn exact_def(name: &str, source: &str, bucket: &str) -> ContinuousAggregateDef {
        let expr = |function, column: &str| AggregateExpr {
            function,
            source_column: column.into(),
            output_column: String::new(),
        };
        ContinuousAggregateDef {
            aggregates: vec![
                expr(AggFunction::Count, "*"),
                expr(AggFunction::Sum, "v"),
                expr(AggFunction::Min, "v"),
                expr(AggFunction::Max, "v"),
                expr(AggFunction::Avg, "v"),
                expr(AggFunction::First, "v"),
                expr(AggFunction::Last, "v"),
            ],
            ..make_agg_def(name, source, bucket)
        }
    }

    /// One drain of `rows` over a `(timestamp, v BIGINT)` schema.
    fn int_drain(rows: &[(i64, i64)]) -> ColumnarDrainResult {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("v".into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, test_memtable_config());
        for &(ts, v) in rows {
            mt.ingest_row(1, &[ColumnValue::Timestamp(ts), ColumnValue::Int64(v)])
                .unwrap();
        }
        mt.drain()
    }

    /// The ad-hoc timeseries aggregate of `rows` per bucket of
    /// `bucket_ms`, in `exact_def` order: COUNT, SUM, MIN, MAX, AVG, FIRST,
    /// LAST.
    fn ad_hoc(rows: &[(i64, i64)], bucket_ms: i64) -> Vec<(i64, Vec<Value>)> {
        use crate::engine::timeseries::columnar_agg::AggAccum;
        let mut sorted = rows.to_vec();
        sorted.sort_by_key(|&(ts, _)| ts);
        let mut buckets: std::collections::BTreeMap<i64, AggAccum> = Default::default();
        for (ts, v) in sorted {
            buckets
                .entry(time_bucket::time_bucket(bucket_ms, ts))
                .or_default()
                .feed_int(v);
        }
        buckets
            .into_iter()
            .map(|(bucket, a)| {
                let cell = |v: Option<&Value>| v.cloned().unwrap_or(Value::Null);
                let values = vec![
                    Value::Integer(a.count as i64),
                    a.sum_value().unwrap(),
                    cell(a.min()),
                    cell(a.max()),
                    a.avg_f64().unwrap().map_or(Value::Null, Value::Float),
                    cell(a.first()),
                    cell(a.last()),
                ];
                (bucket, values)
            })
            .collect()
    }

    /// Every materialized bucket of `name`, finalized.
    fn materialized(mgr: &ContinuousAggregateManager, name: &str) -> Vec<(i64, Vec<Value>)> {
        let def = mgr.get_definition(0, name).unwrap();
        let layout = crate::engine::timeseries::continuous_agg::ColumnLayout::of(def);
        mgr.get_materialized(0, name)
            .unwrap()
            .into_iter()
            .map(|p| {
                let values = def
                    .aggregates
                    .iter()
                    .map(|e| p.finalize(e, &layout).unwrap())
                    .collect();
                (p.bucket_ts, values)
            })
            .collect()
    }

    #[test]
    fn materialized_integers_equal_ad_hoc_across_refreshes_and_o3() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(exact_def("v_1m", "metrics", "1m"));
        let rows = exact_rows();

        // Three refreshes; the last carries rows below the watermark.
        mgr.on_flush(0, "metrics", &int_drain(&rows[3..5]), T0);
        mgr.on_flush(0, "metrics", &int_drain(&rows[5..]), T0);
        mgr.on_flush(0, "metrics", &int_drain(&rows[..3]), T0);

        assert!(
            mgr.get_watermark(0, "v_1m")
                .unwrap()
                .o3_watermark_ts
                .is_some()
        );
        let got = materialized(&mgr, "v_1m");
        assert_eq!(got, ad_hoc(&rows, 60_000));
        // The second bucket's total left `i64`: an exact Decimal.
        assert!(
            matches!(got[1].1[1], Value::Decimal(_)),
            "{:?}",
            got[1].1[1]
        );
    }

    #[test]
    fn rollup_tier_equals_ad_hoc_over_raw_rows() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(exact_def("v_1m", "metrics", "1m"));
        let mut tier2 = exact_def("v_1h", "v_1m", "1h");
        tier2.refresh_policy = RefreshPolicy::OnSeal;
        mgr.register(tier2);
        let rows = exact_rows();

        let refreshed = mgr.on_flush(0, "metrics", &int_drain(&rows[4..]), T0);
        assert_eq!(refreshed, vec!["v_1m", "v_1h"]);
        mgr.on_flush(0, "metrics", &int_drain(&rows[..4]), T0);

        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
        assert_eq!(mgr.get_watermark(0, "v_1h").unwrap().rows_aggregated, 7);
    }

    #[test]
    fn manual_refresh_rolls_up_into_downstream() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut tier1 = exact_def("v_1m", "metrics", "1m");
        tier1.refresh_policy = RefreshPolicy::Manual;
        mgr.register(tier1);
        mgr.register(exact_def("v_1h", "v_1m", "1h"));
        let rows = exact_rows();

        mgr.manual_refresh(0, "v_1m", &int_drain(&rows), T0);
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
    }

    #[test]
    fn three_tier_chain_equals_ad_hoc() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(exact_def("a", "metrics", "1m"));
        mgr.register(exact_def("b", "a", "1m"));
        mgr.register(exact_def("c", "b", "1h"));
        let rows = exact_rows();

        let refreshed = mgr.on_flush(0, "metrics", &int_drain(&rows), T0);
        assert_eq!(refreshed, vec!["a", "b", "c"]);
        assert_eq!(materialized(&mgr, "b"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "c"), ad_hoc(&rows, 3_600_000));
    }

    /// Two aggregates sourced from each other: a refresh of one reaches the
    /// other and stops there instead of cycling.
    #[test]
    fn chain_that_loops_back_refreshes_each_aggregate_once() {
        let mut mgr = ContinuousAggregateManager::new();
        let mut x = exact_def("x", "y", "1m");
        x.refresh_policy = RefreshPolicy::OnSeal;
        mgr.register(x);
        mgr.register(exact_def("y", "x", "1m"));
        let rows = exact_rows();

        mgr.manual_refresh(0, "x", &int_drain(&rows), T0);
        assert_eq!(materialized(&mgr, "x"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "y"), ad_hoc(&rows, 60_000));
    }

    #[test]
    fn reregister_with_another_shape_drops_old_buckets() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(exact_def("v_1m", "metrics", "1m"));
        mgr.on_flush(0, "metrics", &int_drain(&exact_rows()), T0);
        assert!(!mgr.get_materialized(0, "v_1m").unwrap().is_empty());

        // Same shape: buckets stay.
        mgr.register(exact_def("v_1m", "metrics", "1m"));
        assert!(!mgr.get_materialized(0, "v_1m").unwrap().is_empty());

        // Another bucket interval: buckets and watermark reset.
        mgr.register(exact_def("v_1m", "metrics", "5m"));
        assert!(mgr.get_materialized(0, "v_1m").unwrap().is_empty());
        assert_eq!(mgr.get_watermark(0, "v_1m").unwrap().rows_aggregated, 0);
    }
}
