// SPDX-License-Identifier: BUSL-1.1

//! Continuous aggregate manager: registry, lifecycle, and query.
//!
//! Lives on the Data Plane (!Send). One per core. Every aggregate rooted at
//! a timeseries collection keeps its buckets on the core that owns the
//! collection's vShard, because every flush and partition of the
//! collection happens there.
//!
//! Every map is scoped by `(database_id, tenant_id, name)` so an aggregate
//! named identically in two databases or two tenants never collides and
//! never folds another scope's rows.
//!
//! The fold path lives in `fold.rs` and the image path in `persist.rs`.

use std::collections::{HashMap, HashSet};

use super::cut::{CutState, OrphanCut};
use super::definition::ContinuousAggregateDef;
use super::image::CaggImageEntry;
use super::partial::PartialAggregate;
use super::refresh::Buckets;
use super::watermark::WatermarkState;
use crate::engine::timeseries::columnar_memtable::TimeKind;

/// Key scoping every continuous-aggregate map: `(database_id, tenant_id,
/// name)`, or `(database_id, tenant_id, source)` for the dependency graph
/// and for per-root state.
pub type AggKey = (u64, u64, String);

/// Whether two definitions of one aggregate build the same partial state
/// from the same CREATE. A change to any of these makes the buckets of the
/// old definition meaningless under the new one.
pub(super) fn same_shape(a: &ContinuousAggregateDef, b: &ContinuousAggregateDef) -> bool {
    a.source == b.source
        && a.bucket_interval_ms == b.bucket_interval_ms
        && a.group_by == b.group_by
        && a.aggregates == b.aggregates
        && a.incarnation == b.incarnation
}

/// Manages all continuous aggregates for a timeseries engine instance.
///
/// Each aggregate's buckets equal the fold of exactly the root-source
/// partitions named in its folded set, minus the buckets its own retention
/// removed.
pub struct ContinuousAggregateManager {
    pub(super) definitions: HashMap<AggKey, ContinuousAggregateDef>,
    pub(super) watermarks: HashMap<AggKey, WatermarkState>,
    pub(super) materialized: HashMap<AggKey, Buckets>,
    /// `(database_id, tenant_id, source)` → aggregates sourced from it.
    pub(super) dependencies: HashMap<AggKey, Vec<String>>,
    /// Root-source partition directory names folded into each aggregate.
    pub(super) folded: HashMap<AggKey, HashSet<String>>,
    /// Aggregates whose persisted image has been read into memory. An
    /// aggregate takes no fold until it is loaded.
    pub(super) loaded: HashSet<AggKey>,
    /// Kind of the root source's time column, per aggregate.
    pub(super) time_kinds: HashMap<AggKey, TimeKind>,
    /// Root sources whose image file does not hold the current state.
    pub(super) dirty_images: HashSet<AggKey>,
    /// Root sources with a tier that can lack a partition it folds.
    pub(super) backfill_roots: HashSet<AggKey>,
    /// Refresh-cut state per aggregate.
    pub(super) cuts: HashMap<AggKey, CutState>,
    /// Cuts, per root, whose aggregate is not registered and loaded here at
    /// the cut's incarnation yet.
    pub(super) orphan_cuts: HashMap<AggKey, Vec<OrphanCut>>,
    /// Image entries, per root, of aggregates not registered on this core
    /// when the image was read: a downstream tier whose registration
    /// arrives after its root's. Every image write carries them, so a tier
    /// registered late still finds its state on disk.
    pub(super) unclaimed: HashMap<AggKey, Vec<CaggImageEntry>>,
    /// Roots whose image or partitions could not be read. Their image is
    /// never rewritten, so the state it holds stays for repair.
    pub(super) quarantined: HashSet<AggKey>,
    /// Roots whose image was read into memory.
    pub(super) images_read: HashSet<AggKey>,
    /// Boot registered every aggregate the catalog holds. An image entry of
    /// an aggregate not registered from then on belongs to a dropped one.
    pub(super) boot_registered: bool,
    /// The newest time-key value of a partition retention removed, per
    /// root. See [`super::image::CaggImage::raw_dropped_through`].
    pub(super) raw_dropped_through: HashMap<AggKey, i64>,
    /// Per root, the newest system-time ordinal at which each aggregate's
    /// buckets changed other than by folding rows: a refresh cut or a
    /// bucket retention pass. Keyed by aggregate name.
    pub(super) rewritten_at: HashMap<AggKey, HashMap<String, i64>>,
}

impl ContinuousAggregateManager {
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            watermarks: HashMap::new(),
            materialized: HashMap::new(),
            dependencies: HashMap::new(),
            folded: HashMap::new(),
            loaded: HashSet::new(),
            time_kinds: HashMap::new(),
            dirty_images: HashSet::new(),
            backfill_roots: HashSet::new(),
            cuts: HashMap::new(),
            orphan_cuts: HashMap::new(),
            unclaimed: HashMap::new(),
            quarantined: HashSet::new(),
            images_read: HashSet::new(),
            boot_registered: false,
            raw_dropped_through: HashMap::new(),
            rewritten_at: HashMap::new(),
        }
    }

    /// Record that retention removed root partitions whose rows reach the
    /// time-key value `max_ts`. The tiers then hold the only copy of those
    /// rows, so a raw read of their range is incomplete.
    pub fn note_raw_dropped(&mut self, database_id: u64, tenant_id: u64, root: &str, max_ts: i64) {
        let key = (database_id, tenant_id, root.to_string());
        let through = self
            .raw_dropped_through
            .entry(key.clone())
            .or_insert(max_ts);
        *through = (*through).max(max_ts);
        self.dirty_images.insert(key);
    }

    /// The newest time-key value of a partition of `root` retention
    /// removed, `None` when it removed none.
    pub fn raw_dropped_through(&self, database_id: u64, tenant_id: u64, root: &str) -> Option<i64> {
        self.raw_dropped_through
            .get(&(database_id, tenant_id, root.to_string()))
            .copied()
    }

    // -- Registration --

    /// Register a continuous aggregate for `tenant_id`. The database scope
    /// is taken from `def.database_id`.
    ///
    /// Idempotent: boot re-registration and a replayed post-apply both
    /// register a definition the core can already hold, and keep its state.
    ///
    /// A definition of another shape (source, bucket, GROUP BY, aggregates,
    /// or incarnation) replaces the old one with empty state. The aggregate
    /// is then unloaded: the maintenance tick reads its persisted image and
    /// folds every partition the image does not name.
    pub fn register(&mut self, tenant_id: u64, def: ContinuousAggregateDef) {
        let database_id = def.database_id;
        let name = def.name.clone();
        let key = (database_id, tenant_id, name.clone());

        let fresh = match self.definitions.get(&key) {
            Some(previous) => {
                if previous.source != def.source
                    && let Some(deps) = self.dependencies.get_mut(&(
                        database_id,
                        tenant_id,
                        previous.source.clone(),
                    ))
                {
                    deps.retain(|n| n != &name);
                }
                !same_shape(previous, &def)
            }
            None => true,
        };
        if fresh {
            self.mark_root_dirty(&key);
            self.clear_state(&key);
        }
        let deps = self
            .dependencies
            .entry((database_id, tenant_id, def.source.clone()))
            .or_default();
        if !deps.contains(&name) {
            deps.push(name.clone());
        }
        self.definitions.insert(key.clone(), def);
        self.watermarks.entry(key.clone()).or_default();
        self.materialized.entry(key.clone()).or_default();
        self.folded.entry(key).or_default();
    }

    /// Remove a continuous aggregate. Its root's image is rewritten without
    /// it at the next image write.
    pub fn unregister(&mut self, database_id: u64, tenant_id: u64, name: &str) {
        let key = (database_id, tenant_id, name.to_string());
        self.mark_root_dirty(&key);
        if let Some(def) = self.definitions.remove(&key) {
            self.clear_state(&key);
            self.drop_orphan_cuts(&key, def.incarnation);
            if let Some(deps) =
                self.dependencies
                    .get_mut(&(database_id, tenant_id, def.source.clone()))
            {
                deps.retain(|n| n != name);
            }
        }
    }

    /// Drop every piece of per-aggregate state but the definition.
    pub(super) fn clear_state(&mut self, key: &AggKey) {
        self.watermarks.remove(key);
        self.materialized.remove(key);
        self.folded.remove(key);
        self.loaded.remove(key);
        self.time_kinds.remove(key);
        self.cuts.remove(key);
    }

    /// Mark the image of `key`'s root dirty, when the root resolves.
    pub(super) fn mark_root_dirty(&mut self, key: &AggKey) {
        if let Some(root) = self.root_of(key.0, key.1, &key.2) {
            self.dirty_images.insert((key.0, key.1, root));
        }
    }

    /// Get a registered definition.
    pub fn get_definition(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Option<&ContinuousAggregateDef> {
        self.definitions
            .get(&(database_id, tenant_id, name.to_string()))
    }

    /// Whether `name` is registered at `incarnation` or a later one.
    pub fn is_registered_at(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        incarnation: nodedb_types::Hlc,
    ) -> bool {
        self.get_definition(database_id, tenant_id, name)
            .is_some_and(|def| def.incarnation >= incarnation)
    }

    /// Get watermark state for an aggregate.
    pub fn get_watermark(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Option<&WatermarkState> {
        self.watermarks
            .get(&(database_id, tenant_id, name.to_string()))
    }

    /// Kind of the root source's time column, once a fold has seen it.
    pub fn time_kind(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<TimeKind> {
        self.time_kinds
            .get(&(database_id, tenant_id, name.to_string()))
            .copied()
    }

    /// Number of registered aggregates.
    pub fn aggregate_count(&self) -> usize {
        self.definitions.len()
    }

    // -- Query --

    /// Get materialized results for an aggregate, sorted by bucket and
    /// group.
    pub fn get_materialized(
        &self,
        database_id: u64,
        tenant_id: u64,
        agg_name: &str,
    ) -> Option<Vec<&PartialAggregate>> {
        self.materialized
            .get(&(database_id, tenant_id, agg_name.to_string()))
            .map(|m| {
                let mut results: Vec<&PartialAggregate> = m.values().collect();
                results
                    .sort_by(|a, b| (a.bucket_ts, &a.group_key).cmp(&(b.bucket_ts, &b.group_key)));
                results
            })
    }

    /// Get materialized results within a time range.
    pub fn get_materialized_range(
        &self,
        database_id: u64,
        tenant_id: u64,
        agg_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Option<Vec<&PartialAggregate>> {
        self.get_materialized(database_id, tenant_id, agg_name)
            .map(|all| {
                all.into_iter()
                    .filter(|p| p.bucket_ts >= start_ms && p.bucket_ts <= end_ms)
                    .collect()
            })
    }

    // -- Retention --

    /// Apply retention across one tenant's aggregates of `database` on this core:
    /// remove materialized buckets older than each aggregate's retention
    /// period. An aggregate that loses a bucket records `ordinal`, the
    /// system time of the pass, as its rewrite.
    pub fn apply_retention(
        &mut self,
        now_ms: i64,
        ordinal: i64,
        database: u64,
        tenant: u64,
    ) -> usize {
        let mut total_removed = 0;
        let defs: Vec<(AggKey, u64)> = self
            .definitions
            .iter()
            .filter(|(key, _)| key.0 == database && key.1 == tenant)
            .map(|(key, d)| (key.clone(), d.retention_period_ms))
            .collect();

        for (key, retention_ms) in defs {
            if retention_ms == 0 {
                continue;
            }
            let cutoff = now_ms.saturating_sub(i64::try_from(retention_ms).unwrap_or(i64::MAX));
            let removed = match self.materialized.get_mut(&key) {
                Some(mat) => {
                    let before = mat.len();
                    mat.retain(|&(bucket_ts, _), _| bucket_ts > cutoff);
                    before - mat.len()
                }
                None => 0,
            };
            if removed > 0 {
                if let Some(root) = self.root_of(key.0, key.1, &key.2) {
                    self.note_rewritten(key.0, key.1, &root, &key.2, ordinal);
                }
                self.mark_root_dirty(&key);
            }
            total_removed += removed;
        }
        total_removed
    }

    // -- Schema invalidation --

    /// Reset every aggregate that depends on `source`, directly or through a
    /// chain, to its freshly-registered state: no buckets, no folded
    /// partitions, and unloaded. The definitions stay registered and active.
    /// The maintenance tick reloads the root's image from disk and folds
    /// every partition the image does not name. This is what a `TRUNCATE`
    /// of the source, a clear-then-install, and a truncate undo owe their
    /// aggregates.
    pub fn reset_for_source(&mut self, database_id: u64, tenant_id: u64, source: &str) {
        let bare = super::fold::source_name(database_id, source);
        let mut pending: Vec<String> = self
            .dependencies
            .get(&(database_id, tenant_id, bare.to_string()))
            .cloned()
            .unwrap_or_default();
        let mut seen = HashSet::new();
        while let Some(name) = pending.pop() {
            if !seen.insert(name.clone()) {
                continue;
            }
            let key = (database_id, tenant_id, name.clone());
            if !self.definitions.contains_key(&key) {
                continue;
            }
            // Every row the cuts named is gone: pending cuts complete with
            // nothing left to fold, and parked rows go with the source.
            let cut = self.cuts.remove(&key).map(|mut state| {
                state.complete_pending();
                state.parked.clear();
                state.parked_rows = 0;
                state.parked_max_ts = i64::MIN;
                state
            });
            self.clear_state(&key);
            if let Some(cut) = cut {
                self.cuts.insert(key.clone(), cut);
            }
            self.watermarks
                .insert(key.clone(), WatermarkState::default());
            self.materialized.insert(key.clone(), Buckets::default());
            self.folded.insert(key, HashSet::new());
            if let Some(downstream) = self.dependencies.get(&(database_id, tenant_id, name)) {
                pending.extend(downstream.iter().cloned());
            }
        }
        let root = (database_id, tenant_id, source.to_string());
        if let Some(orphans) = self.orphan_cuts.get_mut(&root) {
            for orphan in orphans {
                orphan.partitions.clear();
            }
        }
        self.unclaimed.remove(&root);
        self.quarantined.remove(&root);
        self.images_read.remove(&root);
        self.dirty_images.insert(root);
    }

    /// Mark aggregates as stale after source schema change.
    pub fn invalidate_for_source(&mut self, database_id: u64, tenant_id: u64, source: &str) {
        if let Some(agg_names) = self
            .dependencies
            .get(&(database_id, tenant_id, source.to_string()))
            .cloned()
        {
            for name in &agg_names {
                self.invalidate(database_id, tenant_id, name);
            }
        }
    }

    /// Mark a specific aggregate as stale.
    pub fn invalidate(&mut self, database_id: u64, tenant_id: u64, name: &str) {
        if let Some(def) = self
            .definitions
            .get_mut(&(database_id, tenant_id, name.to_string()))
        {
            def.stale = true;
        }
    }

    // -- Introspection --

    /// List the aggregates of one database and tenant with status.
    pub fn list_aggregates(&self, database_id: u64, tenant_id: u64) -> Vec<AggregateInfo> {
        let mut infos: Vec<AggregateInfo> = self
            .definitions
            .iter()
            .filter(|((db, tid, _), _)| *db == database_id && *tid == tenant_id)
            .map(|(key, def)| {
                let wm = self.watermarks.get(key);
                let bucket_count = self.materialized.get(key).map_or(0, |m| m.len() as u64);
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
            .collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        infos
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
    pub refresh_policy: super::definition::RefreshPolicy,
    pub watermark_ts: i64,
    pub rows_aggregated: u64,
    pub materialized_buckets: u64,
    pub stale: bool,
}

#[cfg(test)]
mod tests {
    use crate::engine::timeseries::continuous_agg::definition::RefreshPolicy;
    use crate::engine::timeseries::continuous_agg::test_support::{
        T0, ad_hoc, exact_def, exact_rows, int_drain, loaded_manager, materialized,
    };

    #[test]
    fn register_and_list() {
        let mgr = loaded_manager(vec![
            exact_def("metrics_1m", "metrics", "1m"),
            exact_def("metrics_1h", "metrics_1m", "1h"),
        ]);
        assert_eq!(mgr.aggregate_count(), 2);
        assert_eq!(mgr.list_aggregates(0, 1).len(), 2);
        assert!(mgr.list_aggregates(0, 2).is_empty());
    }

    /// A second register of the same definition keeps its state and folds
    /// each partition once.
    #[test]
    fn repeated_register_keeps_state() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        mgr.register(1, exact_def("v_1m", "metrics", "1m"));
        assert_eq!(
            mgr.get_watermark(0, 1, "v_1m").map(|w| w.rows_aggregated),
            Some(7)
        );
        assert_eq!(mgr.chain(0, 1, "metrics"), vec!["v_1m"]);
    }

    #[test]
    fn unregister() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.unregister(0, 1, "v_1m");
        assert_eq!(mgr.aggregate_count(), 0);
        assert!(mgr.chain(0, 1, "metrics").is_empty());
    }

    /// Late rows below the watermark fold exactly: the buckets equal one
    /// pass over every row.
    #[test]
    fn late_rows_fold_exactly_without_recompute() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        let rows = exact_rows();
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&rows[3..5]), T0);
        mgr.on_flush(0, 1, "metrics", "p2", &int_drain(&rows[5..]), T0);
        mgr.on_flush(0, 1, "metrics", "p3", &int_drain(&rows[..3]), T0);
        let got = materialized(&mgr, "v_1m");
        assert_eq!(got, ad_hoc(&rows, 60_000));
        assert!(
            matches!(got[1].1[1], nodedb_types::Value::Decimal(_)),
            "{:?}",
            got[1].1[1]
        );
        assert_eq!(
            mgr.get_watermark(0, 1, "v_1m").map(|w| w.watermark_ts),
            Some(T0 + 3_600_000)
        );
    }

    /// A truncate of the source empties the aggregate, unloads it, and
    /// leaves it registered.
    #[test]
    fn reset_for_source_empties_and_unloads() {
        let mut mgr = loaded_manager(vec![
            exact_def("v_1m", "metrics", "1m"),
            exact_def("v_1h", "v_1m", "1h"),
        ]);
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        mgr.reset_for_source(0, 1, "metrics");
        for name in ["v_1m", "v_1h"] {
            assert!(
                mgr.get_materialized(0, 1, name)
                    .expect("registered")
                    .is_empty()
            );
            assert_eq!(
                mgr.get_watermark(0, 1, name).map(|w| w.rows_aggregated),
                Some(0)
            );
            assert!(!mgr.get_definition(0, 1, name).expect("registered").stale);
        }
        assert!(mgr.has_unloaded(0, 1, "metrics"));
        assert!(mgr.image_dirty(0, 1, "metrics"));
    }

    #[test]
    fn retention_drops_old_buckets() {
        let mut def = exact_def("v_1m", "metrics", "1m");
        def.retention_period_ms = 600_000;
        let mut mgr = loaded_manager(vec![def]);
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        let before = mgr
            .get_materialized(0, 1, "v_1m")
            .expect("registered")
            .len();
        assert_eq!(
            mgr.apply_retention(T0 + 15 * 60_000, 1, 9, 1),
            0,
            "another database's pass keeps these buckets"
        );
        let removed = mgr.apply_retention(T0 + 15 * 60_000, 1, 0, 1);
        assert!(removed > 0);
        assert!(
            mgr.get_materialized(0, 1, "v_1m")
                .expect("registered")
                .len()
                < before
        );
    }

    #[test]
    fn invalidated_aggregate_takes_no_fold() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.invalidate_for_source(0, 1, "metrics");
        assert!(mgr.get_definition(0, 1, "v_1m").expect("registered").stale);
        assert!(
            mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0)
                .is_empty()
        );
    }

    #[test]
    fn time_range_query() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        let hits = mgr
            .get_materialized_range(0, 1, "v_1m", T0, T0 + 60_000)
            .expect("registered");
        assert_eq!(hits.len(), 2);
        assert!(
            hits.iter()
                .all(|p| p.bucket_ts >= T0 && p.bucket_ts <= T0 + 60_000)
        );
    }

    #[test]
    fn rollup_tier_equals_ad_hoc_over_raw_rows() {
        let mut tier2 = exact_def("v_1h", "v_1m", "1h");
        tier2.refresh_policy = RefreshPolicy::OnSeal;
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m"), tier2]);
        let rows = exact_rows();
        let refreshed = mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&rows[4..]), T0);
        assert_eq!(refreshed, vec!["v_1m", "v_1h"]);
        mgr.on_flush(0, 1, "metrics", "p2", &int_drain(&rows[..4]), T0);
        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
        assert_eq!(
            mgr.get_watermark(0, 1, "v_1h").map(|w| w.rows_aggregated),
            Some(7)
        );
    }

    #[test]
    fn three_tier_chain_equals_ad_hoc() {
        let mut mgr = loaded_manager(vec![
            exact_def("a", "metrics", "1m"),
            exact_def("b", "a", "1m"),
            exact_def("c", "b", "1h"),
        ]);
        let rows = exact_rows();
        let refreshed = mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&rows), T0);
        assert_eq!(refreshed, vec!["a", "b", "c"]);
        assert_eq!(materialized(&mgr, "b"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "c"), ad_hoc(&rows, 3_600_000));
    }

    /// Two aggregates sourced from each other have no root and take no fold.
    #[test]
    fn chain_that_loops_back_has_no_root() {
        let mgr = loaded_manager(vec![exact_def("x", "y", "1m"), exact_def("y", "x", "1m")]);
        assert_eq!(mgr.root_of(0, 1, "x"), None);
        assert!(mgr.tick_roots().is_empty());
    }

    #[test]
    fn reregister_with_another_shape_drops_old_state() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        mgr.register(1, exact_def("v_1m", "metrics", "5m"));
        assert!(
            mgr.get_materialized(0, 1, "v_1m")
                .expect("registered")
                .is_empty()
        );
        assert_eq!(
            mgr.get_watermark(0, 1, "v_1m").map(|w| w.rows_aggregated),
            Some(0)
        );
        assert!(mgr.has_unloaded(0, 1, "metrics"));
    }
}
