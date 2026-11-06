// SPDX-License-Identifier: BUSL-1.1

//! Partition folding: the only path rows take into an aggregate.
//!
//! Every aggregate records the root-source partitions it has folded, by
//! directory name. Folding partition P computes the raw tier's delta from
//! P's rows and rolls it down the chain tier by tier. A tier that lacks P
//! and is in scope merges its delta and records P. A tier that already
//! holds P, or is out of scope, only passes its delta on. Each tier is
//! therefore exact on its own, whatever its upstream holds.
//!
//! When a tier folds depends on its refresh policy:
//!
//! - `OnFlush`, `OnSeal`: at the partition's flush. A partition seals at
//!   its flush. The maintenance tick folds what a flush could not (the tier
//!   was unloaded, or created after the partition).
//! - `Periodic(ms)`, `Manual`: only the partitions a refresh cut names
//!   (`cut.rs`), on the maintenance tick. A `Periodic` tier takes a cut
//!   every `ms`, a `Manual` tier on `REFRESH CONTINUOUS AGGREGATE`. Every
//!   replica applies the same cuts in log order, so every replica serves
//!   the same refresh point.
//!
//! Retention folds a partition into every tier before it removes the
//! partition, whatever the policy, so no aggregate loses rows to raw
//! retention. A cut tier parks the rows of a partition no cut named yet.
//!
//! A read of an `OnFlush` / `OnSeal` aggregate first completes its
//! backfill. A read of a `Periodic` or `Manual` aggregate serves its last
//! completed cut, as a PostgreSQL materialized view serves its last
//! refresh.

use std::collections::{HashMap, HashSet};

use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

use super::cut::folds_at_cuts;
use super::manager::{AggKey, ContinuousAggregateManager};
use super::partial::ColumnLayout;
use super::refresh::{self, Buckets, RefreshSource};
use super::rollup;
use crate::engine::timeseries::columnar_memtable::ColumnarDrainResult;

/// The partition a fold reads, and the root its aggregates hang from.
#[derive(Debug, Clone, Copy)]
pub struct FoldTarget<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Root raw source collection.
    pub root: &'a str,
    /// Partition directory name.
    pub partition: &'a str,
}

/// Which tiers a fold merges into. Every scope takes only loaded,
/// non-stale tiers that lack the partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldScope {
    /// Tiers that fold at flush: `OnFlush` and `OnSeal`.
    AtFlush,
    /// The flush tiers, and every tier whose pending cut names the
    /// partition.
    Tick,
    /// A partition retention is about to remove: every tier. A cut tier
    /// whose pending cut does not name it parks its rows for the next cut.
    Expiring,
}

/// The storage name of `source`, a catalog name. The Data Plane files a
/// collection of a named database under its qualified name, and a root takes
/// that name wherever per-root state and partitions meet.
pub(super) fn storage_root(database_id: u64, source: &str) -> String {
    QualifiedCollection::new(DatabaseId::new(database_id), source)
        .as_str()
        .to_string()
}

/// The catalog name a definition's `source` holds, from a root's storage name.
pub(super) fn source_name(database_id: u64, root: &str) -> &str {
    CollectionKey::from_qualified_str(DatabaseId::new(database_id), root)
        .map_or(root, |key| key.name())
}

/// What one tier does with a partition's delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Take {
    Merge,
    Park,
    Pass,
}

impl ContinuousAggregateManager {
    /// Root raw source of `name`, under the storage name the Data Plane files
    /// its partitions by: the first source in its chain that is not itself a
    /// registered aggregate. `None` when `name` is not registered or its chain
    /// loops.
    pub fn root_of(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<String> {
        let mut current = name.to_string();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current.clone()) {
                return None;
            }
            let def = self
                .definitions
                .get(&(database_id, tenant_id, current.clone()))?;
            if !self
                .definitions
                .contains_key(&(database_id, tenant_id, def.source.clone()))
            {
                return Some(storage_root(database_id, &def.source));
            }
            current = def.source.clone();
        }
    }

    /// Every root collection with a registered aggregate, sorted.
    pub fn root_sources(&self) -> Vec<AggKey> {
        let mut roots: Vec<AggKey> = self
            .definitions
            .keys()
            .filter_map(|(db, tid, name)| self.root_of(*db, *tid, name).map(|r| (*db, *tid, r)))
            .collect();
        roots.sort();
        roots.dedup();
        roots
    }

    /// Every registered aggregate rooted at `root`, upstream before
    /// downstream.
    pub fn chain(&self, database_id: u64, tenant_id: u64, root: &str) -> Vec<String> {
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        let mut frontier = vec![source_name(database_id, root).to_string()];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for source in frontier {
                let Some(deps) = self.dependencies.get(&(database_id, tenant_id, source)) else {
                    continue;
                };
                for name in deps {
                    if seen.insert(name.clone()) {
                        order.push(name.clone());
                        next.push(name.clone());
                    }
                }
            }
            frontier = next;
        }
        order
    }

    /// What `key` does with `partition` under `scope`.
    fn take(&self, key: &AggKey, partition: &str, scope: FoldScope) -> Take {
        let Some(def) = self.definitions.get(key) else {
            return Take::Pass;
        };
        if def.stale || !self.loaded.contains(key) || self.has_folded(key, partition) {
            return Take::Pass;
        }
        let at_flush = !folds_at_cuts(&def.refresh_policy);
        match scope {
            FoldScope::AtFlush if at_flush => Take::Merge,
            FoldScope::AtFlush => Take::Pass,
            FoldScope::Tick | FoldScope::Expiring if at_flush || self.cut_folds(key, partition) => {
                Take::Merge
            }
            FoldScope::Tick => Take::Pass,
            FoldScope::Expiring => Take::Park,
        }
    }

    fn has_folded(&self, key: &AggKey, partition: &str) -> bool {
        self.folded
            .get(key)
            .is_some_and(|set| set.contains(partition))
    }

    /// Whether some tier rooted at `root` in `scope` lacks `partition`.
    pub fn needs_fold(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        partition: &str,
        scope: FoldScope,
    ) -> bool {
        self.chain(database_id, tenant_id, root)
            .into_iter()
            .any(|name| self.take(&(database_id, tenant_id, name), partition, scope) != Take::Pass)
    }

    /// Fold `target`'s partition, whose rows are `src`, into every tier
    /// rooted at `target.root` that is in `scope` and lacks it. Returns the
    /// tiers that merged or parked it.
    pub fn fold_into_chain(
        &mut self,
        target: FoldTarget<'_>,
        src: &RefreshSource<'_>,
        scope: FoldScope,
        now_ms: i64,
    ) -> Vec<String> {
        let FoldTarget {
            database_id,
            tenant_id,
            root,
            partition,
        } = target;
        let tiers = self.chain(database_id, tenant_id, root);
        let source = source_name(database_id, root);
        let mut deltas: HashMap<String, Buckets> = HashMap::new();
        let mut folded_into = Vec::new();
        let mut rows = 0u64;
        let mut max_ts = i64::MIN;
        let time_kind = src.time_kind();

        for name in &tiers {
            let key = (database_id, tenant_id, name.clone());
            let Some(def) = self.definitions.get(&key) else {
                continue;
            };
            let delta = if def.source == source {
                let result = refresh::refresh_rows(def, src);
                rows = result.rows_processed;
                max_ts = max_ts.max(result.max_ts);
                result.delta
            } else {
                let up_key = (database_id, tenant_id, def.source.clone());
                let (Some(up_def), Some(up_delta)) =
                    (self.definitions.get(&up_key), deltas.get(&def.source))
                else {
                    continue;
                };
                rollup::rollup_delta(up_def, def, up_delta)
            };
            let take = self.take(&key, partition, scope);
            if take != Take::Pass {
                if take == Take::Merge {
                    let layout = ColumnLayout::of(def);
                    refresh::merge_delta(
                        self.materialized.entry(key.clone()).or_default(),
                        &delta,
                        &layout,
                    );
                    self.watermarks
                        .entry(key.clone())
                        .or_default()
                        .advance(max_ts, rows, now_ms);
                } else {
                    self.park_delta(&key, &delta, rows, max_ts);
                }
                // A parked partition counts as folded: its rows are in the
                // parked delta, so no later cut names it again.
                self.folded
                    .entry(key.clone())
                    .or_default()
                    .insert(partition.to_string());
                if let Some(kind) = time_kind {
                    self.time_kinds.insert(key, kind);
                }
                folded_into.push(name.clone());
            }
            deltas.insert(name.clone(), delta);
        }
        if !folded_into.is_empty() {
            self.dirty_images
                .insert((database_id, tenant_id, root.to_string()));
        }
        folded_into
    }

    /// Fold a flushed partition into every loaded `OnFlush` / `OnSeal` tier
    /// rooted at `source`. Returns the tiers that took the fold.
    pub fn on_flush(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        source: &str,
        partition: &str,
        drain: &ColumnarDrainResult,
        now_ms: i64,
    ) -> Vec<String> {
        let bare = source_name(database_id, source);
        if !self
            .dependencies
            .contains_key(&(database_id, tenant_id, bare.to_string()))
        {
            return Vec::new();
        }
        self.fold_into_chain(
            FoldTarget {
                database_id,
                tenant_id,
                root: source,
                partition,
            },
            &RefreshSource::of_drain(drain),
            FoldScope::AtFlush,
            now_ms,
        )
    }

    /// Retention removed these partitions of `root`. The buckets folded
    /// from them stay.
    pub fn forget_partitions(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        names: &[String],
    ) {
        if names.is_empty() {
            return;
        }
        for name in self.chain(database_id, tenant_id, root) {
            if let Some(set) = self.folded.get_mut(&(database_id, tenant_id, name)) {
                for partition in names {
                    set.remove(partition);
                }
            }
        }
        self.dirty_images
            .insert((database_id, tenant_id, root.to_string()));
    }

    /// Whether a tier rooted at `root` has not read its image yet.
    pub fn has_unloaded(&self, database_id: u64, tenant_id: u64, root: &str) -> bool {
        self.chain(database_id, tenant_id, root)
            .into_iter()
            .any(|name| !self.loaded.contains(&(database_id, tenant_id, name)))
    }

    /// Roots the maintenance tick has work for: a tier that is unloaded, a
    /// backfill mark, or a pending cut of a tier that is not stale.
    pub fn tick_roots(&self) -> Vec<AggKey> {
        let mut roots: HashSet<AggKey> = self.backfill_roots.clone();
        for (key, state) in &self.cuts {
            let folding = self.definitions.get(key).is_some_and(|def| !def.stale);
            if folding
                && !state.pending_ids.is_empty()
                && let Some(root) = self.root_of(key.0, key.1, &key.2)
            {
                roots.insert((key.0, key.1, root));
            }
        }
        for key in self.definitions.keys() {
            if !self.loaded.contains(key)
                && let Some(root) = self.root_of(key.0, key.1, &key.2)
            {
                roots.insert((key.0, key.1, root));
            }
        }
        let mut roots: Vec<AggKey> = roots.into_iter().collect();
        roots.sort();
        roots
    }

    /// Put `root` on the tick's work list until a pass finds nothing left
    /// to fold for it.
    pub fn mark_backfill(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        self.backfill_roots
            .insert((database_id, tenant_id, root.to_string()));
    }

    /// The tick folded every partition of `root` its `Tick` scope names:
    /// drop the backfill mark and complete the pending cuts.
    pub fn finish_tick(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        self.backfill_roots
            .remove(&(database_id, tenant_id, root.to_string()));
        self.complete_cuts(database_id, tenant_id, root);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::definition::{
        AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy,
    };
    use crate::engine::timeseries::continuous_agg::test_support::{
        T0, ad_hoc, exact_def, exact_rows, int_drain, loaded_manager, materialized,
    };

    const P1: FoldTarget<'static> = FoldTarget {
        database_id: 0,
        tenant_id: 1,
        root: "metrics",
        partition: "p1",
    };

    /// A collection of a named database carries a qualified storage name, and
    /// its definition carries the catalog name. A flush and a tick meet on one
    /// root, so a fold and a refresh cut complete there.
    #[test]
    fn a_named_databases_root_folds_under_its_storage_name() {
        const DB: u64 = 1200;
        const ROOT: &str = "1200/metrics";
        let mut minute = exact_def("v_1m", "metrics", "1m");
        minute.database_id = DB;
        let mut hour = exact_def("v_1h", "v_1m", "1h");
        hour.database_id = DB;
        let mut mgr = loaded_manager(vec![minute, hour]);
        assert_eq!(mgr.root_of(DB, 1, "v_1h").as_deref(), Some(ROOT));
        assert_eq!(mgr.chain(DB, 1, ROOT), vec!["v_1m", "v_1h"]);
        let rows = exact_rows();
        assert_eq!(
            mgr.on_flush(DB, 1, ROOT, "p1", &int_drain(&rows), T0),
            vec!["v_1m", "v_1h"]
        );
        assert!(
            !mgr.needs_fold(DB, 1, ROOT, "p1", FoldScope::Tick),
            "the tick owes the root nothing"
        );
        assert_eq!(
            mgr.get_watermark(DB, 1, "v_1m").map(|w| w.rows_aggregated),
            Some(7)
        );
    }

    #[test]
    fn a_partition_folds_once_per_tier() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        let rows = exact_rows();
        let drain = int_drain(&rows);
        assert_eq!(
            mgr.on_flush(0, 1, "metrics", "p1", &drain, T0),
            vec!["v_1m"]
        );
        assert!(mgr.on_flush(0, 1, "metrics", "p1", &drain, T0).is_empty());
        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert_eq!(
            mgr.get_watermark(0, 1, "v_1m").map(|w| w.rows_aggregated),
            Some(7)
        );
    }

    #[test]
    fn unloaded_tier_takes_no_flush() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(1, exact_def("v_1m", "metrics", "1m"));
        let drain = int_drain(&exact_rows());
        assert!(mgr.on_flush(0, 1, "metrics", "p1", &drain, T0).is_empty());
        assert!(mgr.has_unloaded(0, 1, "metrics"));
        assert_eq!(mgr.tick_roots(), vec![(0, 1, "metrics".to_string())]);
    }

    #[test]
    fn tenants_never_share_an_aggregate() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        mgr.register(2, exact_def("v_1m", "metrics", "1m"));
        let drain = int_drain(&exact_rows());
        mgr.on_flush(0, 1, "metrics", "p1", &drain, T0);
        assert_eq!(
            mgr.get_watermark(0, 2, "v_1m").map(|w| w.rows_aggregated),
            Some(0)
        );
        mgr.unregister(0, 2, "v_1m");
        assert!(mgr.get_definition(0, 1, "v_1m").is_some());
    }

    #[test]
    fn downstream_keeps_history_after_upstream_bucket_retention() {
        let mut minute = exact_def("v_1m", "metrics", "1m");
        minute.retention_period_ms = 600_000;
        let mut mgr = loaded_manager(vec![minute, exact_def("v_1h", "v_1m", "1h")]);
        let rows = exact_rows();
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&rows[..3]), T0);
        mgr.apply_retention(T0 + 3_600_000 + 700_000, 1, 0, 1);
        mgr.on_flush(0, 1, "metrics", "p2", &int_drain(&rows[3..]), T0);
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
    }

    #[test]
    fn a_tier_registered_later_folds_from_partition_rows() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        let rows = exact_rows();
        let drain = int_drain(&rows);
        mgr.on_flush(0, 1, "metrics", "p1", &drain, T0);
        mgr.register(1, exact_def("v_1h", "v_1m", "1h"));
        mgr.loaded.insert((0, 1, "v_1h".to_string()));
        assert!(mgr.needs_fold(0, 1, "metrics", "p1", FoldScope::AtFlush));
        let folded =
            mgr.fold_into_chain(P1, &RefreshSource::of_drain(&drain), FoldScope::AtFlush, T0);
        assert_eq!(folded, vec!["v_1h"]);
        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
    }

    /// A cut tier takes no flush and no tick fold until a cut names the
    /// partition.
    #[test]
    fn manual_and_periodic_tiers_fold_only_at_cuts() {
        let mut manual = exact_def("v_manual", "metrics", "1m");
        manual.refresh_policy = RefreshPolicy::Manual;
        let mut periodic = exact_def("v_periodic", "metrics", "1m");
        periodic.refresh_policy = RefreshPolicy::Periodic(60_000);
        let mut mgr = loaded_manager(vec![manual, periodic]);
        let drain = int_drain(&exact_rows());
        assert!(mgr.on_flush(0, 1, "metrics", "p1", &drain, T0).is_empty());
        assert!(!mgr.needs_fold(0, 1, "metrics", "p1", FoldScope::Tick));
        assert!(mgr.tick_roots().is_empty());

        let cut = nodedb_physical::physical_plan::RefreshCut {
            aggregate: "v_manual".into(),
            incarnation: exact_def("v_manual", "metrics", "1m").incarnation,
            refresh_id: 1,
            periodic: false,
        };
        mgr.apply_cut(0, 1, "metrics", &cut, &["p1".to_string()]);
        assert_eq!(mgr.tick_roots(), vec![(0, 1, "metrics".to_string())]);
        let folded = mgr.fold_into_chain(P1, &RefreshSource::of_drain(&drain), FoldScope::Tick, T0);
        assert_eq!(folded, vec!["v_manual"]);
        mgr.finish_tick(0, 1, "metrics");
        assert!(mgr.tick_roots().is_empty());
    }

    #[test]
    fn group_by_tag_keeps_its_group_across_flushes() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
            TimeKind,
        };
        let def = ContinuousAggregateDef {
            group_by: vec!["host".into()],
            aggregates: vec![
                AggregateExpr {
                    function: AggFunction::Count,
                    source_column: "*".into(),
                    output_column: "c".into(),
                },
                AggregateExpr {
                    function: AggFunction::Sum,
                    source_column: "value".into(),
                    output_column: "s".into(),
                },
            ],
            ..exact_def("by_host", "metrics", "1m")
        };
        let mut mgr = loaded_manager(vec![def]);
        let drain = |rows: &[(&str, f64)]| {
            let schema = ColumnarSchema {
                columns: vec![
                    ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                    ("value".into(), ColumnType::Float64),
                    ("host".into(), ColumnType::Symbol),
                ],
                timestamp_idx: 0,
                codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
            };
            let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
            for (host, v) in rows {
                mt.ingest_row(
                    1,
                    &[
                        ColumnValue::Timestamp(T0),
                        ColumnValue::Float64(*v),
                        ColumnValue::Symbol((*host).to_string()),
                    ],
                )
                .unwrap();
            }
            mt.drain()
        };
        mgr.on_flush(
            0,
            1,
            "metrics",
            "p1",
            &drain(&[("a", 1.0), ("b", 10.0)]),
            T0,
        );
        mgr.on_flush(
            0,
            1,
            "metrics",
            "p2",
            &drain(&[("b", 100.0), ("a", 1000.0)]),
            T0,
        );

        let def = mgr.get_definition(0, 1, "by_host").unwrap().clone();
        let layout = ColumnLayout::of(&def);
        let mut by_host: Vec<(String, Vec<nodedb_types::Value>)> = mgr
            .get_materialized(0, 1, "by_host")
            .unwrap()
            .into_iter()
            .map(|p| {
                (
                    format!("{:?}", p.group_key),
                    def.aggregates
                        .iter()
                        .map(|e| p.finalize(e, &layout).unwrap())
                        .collect(),
                )
            })
            .collect();
        by_host.sort_by(|a, b| a.0.cmp(&b.0));
        use nodedb_types::Value;
        assert_eq!(by_host.len(), 2);
        assert_eq!(by_host[0].1, vec![Value::Integer(2), Value::Float(1001.0)]);
        assert_eq!(by_host[1].1, vec![Value::Integer(2), Value::Float(110.0)]);
    }

    #[test]
    fn group_by_int_column_groups_by_value() {
        let def = ContinuousAggregateDef {
            group_by: vec!["v".into()],
            ..exact_def("by_v", "metrics", "1m")
        };
        let mut mgr = loaded_manager(vec![def]);
        mgr.on_flush(
            0,
            1,
            "metrics",
            "p1",
            &int_drain(&[(T0, 1), (T0, 2), (T0, 2)]),
            T0,
        );
        assert_eq!(mgr.get_materialized(0, 1, "by_v").unwrap().len(), 2);
    }

    #[test]
    fn forget_partitions_keeps_buckets() {
        let mut mgr = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        let rows = exact_rows();
        mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&rows), T0);
        mgr.forget_partitions(0, 1, "metrics", &["p1".to_string()]);
        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert!(mgr.needs_fold(0, 1, "metrics", "p1", FoldScope::AtFlush));
    }
}
