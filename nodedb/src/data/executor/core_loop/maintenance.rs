// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use super::CoreLoop;
use crate::engine::document::store::StorageKey;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;
use crate::engine::sparse::doc_cache::DocCache;

/// Shared parameters for [`CoreLoop::apply_secondary_indexes_in_txn`].
pub(in crate::data::executor) struct SecondaryIndexInputs<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub old_doc: Option<&'a serde_json::Value>,
    pub new_doc: &'a serde_json::Value,
    pub doc_id: &'a StorageKey,
    /// The system-time ordinal of the row version this write appends. Every
    /// index entry the write adds or tombstones carries it.
    pub sys_from: i64,
}

impl CoreLoop {
    /// Set compaction parameters (called after open, before event loop).
    pub fn set_compaction_config(
        &mut self,
        interval: std::time::Duration,
        tombstone_threshold: f64,
    ) {
        self.maintenance.compaction_interval = interval;
        self.maintenance.compaction_tombstone_threshold = tombstone_threshold;
    }

    /// Set shared system metrics reference (called after open, before event loop).
    ///
    /// Adopts the `io_metrics` and `spsc_throttle` Arcs so the Prometheus
    /// handler reads this core's gauges without crossing the plane boundary.
    pub fn set_metrics(&mut self, metrics: Arc<crate::control::metrics::SystemMetrics>) {
        self.io_metrics = Arc::clone(&metrics.io_metrics);
        self.throttle
            .adopt_metrics(Arc::clone(&metrics.spsc_throttle));
        self.metrics = Some(metrics);
    }

    /// The shared system metrics this core was wired with, if any.
    ///
    /// The event loop clones the `Arc` once before entering the loop so it can
    /// hold its heartbeat slot without borrowing the core across every tick.
    pub fn metrics(&self) -> Option<&Arc<crate::control::metrics::SystemMetrics>> {
        self.metrics.as_ref()
    }

    /// Set the shared per-database maintenance CPU budget tracker.
    pub fn set_maintenance_budget(
        &mut self,
        tracker: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
    ) {
        self.maintenance.maintenance_budget = Some(tracker);
    }

    /// Set checkpoint coordinator config (called after open, before event loop).
    pub fn set_checkpoint_config(&mut self, config: crate::storage::checkpoint::CheckpointConfig) {
        self.checkpoint_coordinator =
            crate::storage::checkpoint::CheckpointCoordinator::new(config);
    }

    /// Set L1 segment compaction config.
    pub fn set_segment_compaction_config(
        &mut self,
        config: crate::storage::compaction::CompactionConfig,
    ) {
        self.maintenance.segment_compaction_config = config;
    }

    /// Set the number of Data Plane cores on this node. The committed-redo
    /// apply routes its records through the replay arms, which pick a record's
    /// core as `vshard_id % num_cores`. Called after open, before the event
    /// loop starts.
    pub fn set_num_cores(&mut self, num_cores: usize) {
        self.redo_apply.num_cores = num_cores;
    }

    /// Set query execution tuning parameters (called after open, before event loop).
    ///
    /// Also resizes the doc cache if `doc_cache_entries` differs from the current size.
    /// Resizing clears all cached entries.
    pub fn set_query_tuning(&mut self, tuning: nodedb_types::config::tuning::QueryTuning) {
        if tuning.doc_cache_entries != self.query_tuning.doc_cache_entries {
            self.doc_cache = DocCache::new(tuning.doc_cache_entries);
        }
        self.query_tuning = tuning;
    }

    /// Set graph engine tuning (traversal limits + variable-length expansion
    /// caps), called after open, before the event loop starts. The varlen caps
    /// (`varlen_max_results` / `varlen_max_frontier`) bound a single
    /// variable-length MATCH expansion before it pages via resume; they default
    /// to 100k so an unset config is byte-identical to the prior behaviour.
    pub fn set_graph_tuning(&mut self, tuning: nodedb_types::config::tuning::GraphTuning) {
        self.graph_tuning = tuning;
    }

    /// Set timeseries engine tuning (memtable soft/hard budgets + tag
    /// cardinality ceiling), called after open, before the event loop starts.
    ///
    /// Call this before any ingest or WAL replay. Each collection's memtable
    /// captures these limits when it is CREATED and keeps them for its whole
    /// life, so a memtable built ahead of this call would silently keep the
    /// defaults.
    /// Apply vector engine tuning. Must land before the checkpoint restore:
    /// restored collections take its seal threshold.
    /// Set the commit snapshot window: a commit whose snapshot trails its
    /// own ordinal by more is refused on every replica. Every node runs one
    /// value.
    pub fn set_commit_snapshot_window(&mut self, window: std::time::Duration) {
        self.maintenance.version_purge.commit_window_ns = super::version_purge::window_ns(window);
    }

    /// Set the bytes of whole framed records one batch of a chunked redo's
    /// install holds.
    pub fn set_redo_stream_batch_bytes(&mut self, bytes: usize) {
        self.redo_apply.stream_batch_bytes = bytes;
    }

    pub fn set_vector_tuning(&mut self, tuning: nodedb_types::config::tuning::VectorTuning) {
        self.vector_tuning = tuning;
    }

    pub fn set_timeseries_tuning(
        &mut self,
        tuning: nodedb_types::config::tuning::TimeseriesToning,
    ) {
        self.ts_tuning = tuning;
    }

    /// Apply the secondary-index SET diff within an already-open write txn.
    ///
    /// The diff covers the paths this core maintains for the collection,
    /// never the registered config's paths. In top-level mode those include
    /// each top-level key of either document. Every entry lands in the
    /// versioned index at `sys_from`, the ordinal of
    /// the row version the caller appends, inside the caller's
    /// `WriteTransaction`. There is no own-transaction variant: every caller
    /// already holds the row's write txn, and a nested `begin_write`
    /// deadlocks redb's single-writer lock.
    ///
    /// `old_doc` is the row's current document before the write (`None` when
    /// it has none). Values only in the new set get a live entry. Values only
    /// in the old set get a tombstone, so a read at `sys_from` or later no
    /// longer finds the row under them, and a read below `sys_from` still
    /// does. Returns every `(field, value)` pair written at `sys_from`: a
    /// rollback removes exactly those entries.
    ///
    /// An index write that fails propagates rather than being stepped over.
    /// The entries land in the CALLER'S transaction alongside the row body, so
    /// every caller returns before `txn.commit()` and redb rolls both halves
    /// back — the row does not exist, rather than existing with an index that
    /// cannot find it.
    pub(in crate::data::executor) fn apply_secondary_indexes_in_txn(
        &mut self,
        txn: &redb::WriteTransaction,
        inputs: SecondaryIndexInputs<'_>,
    ) -> crate::Result<Vec<(String, String)>> {
        let SecondaryIndexInputs {
            database_id,
            tid,
            collection,
            old_doc,
            new_doc,
            doc_id,
            sys_from,
        } = inputs;
        let crs = self.crs_context();
        let mut written = Vec::new();
        // In top-level mode the paths include each top-level key of either
        // version, so a key the new version drops tombstones its old value.
        let docs = [new_doc, old_doc.unwrap_or(new_doc)];
        let index_paths =
            self.maintained_indexes
                .paths_for_docs(database_id, tid, collection, &docs);
        for index_path in &index_paths {
            let new_vals = index_values_for(new_doc, index_path, &crs);
            let old_vals = old_doc
                .map(|d| index_values_for(d, index_path, &crs))
                .unwrap_or_default();
            for v in new_vals.difference(&old_vals) {
                self.sparse.versioned_index_put_in_txn(
                    txn,
                    VersionedIndexEntry {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        field: &index_path.path,
                        value: v,
                        doc_id,
                        sys_from,
                    },
                )?;
                written.push((index_path.path.clone(), v.clone()));
            }
            for v in old_vals.difference(&new_vals) {
                self.sparse.versioned_index_tombstone_in_txn(
                    txn,
                    VersionedIndexEntry {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        field: &index_path.path,
                        value: v,
                        doc_id,
                        sys_from,
                    },
                )?;
                written.push((index_path.path.clone(), v.clone()));
            }
        }
        Ok(written)
    }

    /// Pause writes to a vShard (during Phase 3 migration cutover).
    pub fn pause_vshard(&mut self, vshard: crate::types::VShardId) {
        self.paused_vshards.insert(vshard);
    }

    /// Resume writes to a vShard after cutover.
    pub fn resume_vshard(&mut self, vshard: crate::types::VShardId) {
        self.paused_vshards.remove(&vshard);
    }

    /// Check if a vShard is paused for writes.
    pub fn is_vshard_paused(&self, vshard: crate::types::VShardId) -> bool {
        self.paused_vshards.contains(&vshard)
    }

    /// Expand a document into the `(field, value)` tuples it contributes across
    /// the paths this core maintains for `collection`, via the same
    /// [`index_values_for`] extraction the forward write path uses. A delete
    /// recomputes with it the tuples the pre-delete document contributed,
    /// when the delete cascade itself (a prefix scan) does not return them.
    pub(in crate::data::executor) fn index_tuples_for_doc(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc: &serde_json::Value,
    ) -> Vec<(String, String)> {
        let crs = self.crs_context();
        let mut out = Vec::new();
        for index_path in
            self.maintained_indexes
                .paths_for_docs(database_id, tid, collection, &[doc])
        {
            for v in index_values_for(doc, &index_path, &crs) {
                out.push((index_path.path.clone(), v));
            }
        }
        out
    }
}

/// Lowercase `v` iff `case_insensitive` — used so COLLATE NOCASE indexes
/// can be matched with a case-insensitive equality lookup.
fn maybe_lowercase(v: &str, case_insensitive: bool) -> String {
    if case_insensitive {
        v.to_lowercase()
    } else {
        v.to_string()
    }
}

/// Extract the set of stored index values a document contributes for one index
/// path. Applies the path's predicate (empty set when it fails) and the same
/// case-insensitive folding the forward write path uses, so the SET diff in
/// `apply_secondary_indexes_in_txn` compares like-for-like.
fn index_values_for(
    doc: &serde_json::Value,
    index_path: &crate::engine::document::store::IndexPath,
    crs: &nodedb_crs::CrsContext<'_>,
) -> std::collections::HashSet<String> {
    if let Some(ref p) = index_path.predicate
        && !p.evaluate_json(doc, crs)
    {
        return std::collections::HashSet::new();
    }
    crate::engine::document::store::extract_index_values(doc, &index_path.path, index_path.is_array)
        .into_iter()
        .map(|v| maybe_lowercase(&v, index_path.case_insensitive))
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId};

    use super::super::CoreLoop;
    use super::super::tests::{make_core_with_dir, make_request};
    use crate::bridge::envelope::Status;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;
    use crate::engine::sparse::btree::MaintainedIndexSet;

    const COLLECTION: &str = "top_level_rows";

    fn put(core: &mut CoreLoop, value: serde_json::Value) {
        let response = core.execute(&ExecutionTask::new(make_request(PhysicalPlan::Document(
            DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                document_id: "r1".into(),
                value: nodedb_types::json_to_msgpack(&value).expect("encode"),
                surrogate: Surrogate::new(1),
                pk_bytes: b"r1".to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
            },
        ))));
        assert_eq!(response.status, Status::Ok, "{response:?}");
    }

    fn rows(core: &CoreLoop, path: &str, value: &str) -> usize {
        core.sparse
            .versioned_index_lookup_as_of(0, 1, COLLECTION, path, value, None)
            .expect("lookup")
            .len()
    }

    /// Every field of the collection's stored index keys.
    fn fields(core: &CoreLoop) -> std::collections::BTreeSet<String> {
        let prefix = format!("0:1:{COLLECTION}:");
        core.sparse
            .scan_versioned_indexes_for_tenant(0, 1)
            .expect("scan")
            .into_iter()
            .filter_map(|(key, _)| {
                key.strip_prefix(prefix.as_str())
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(field, _)| field.to_string())
            })
            .collect()
    }

    #[test]
    fn top_level_mode_indexes_each_scalar_key_and_tombstones_a_dropped_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
        core.seed_doc_configs(&[(
            (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
            CollectionConfig::new(COLLECTION),
        )]);
        let mut set = MaintainedIndexSet::empty(0, 1, COLLECTION);
        set.top_level = true;
        core.maintained_indexes.install(&set).expect("install");

        put(
            &mut core,
            serde_json::json!({"city": "oslo", "n": 3, "tags": ["a"], "nested": {"x": 1}}),
        );
        assert_eq!(rows(&core, "$.city", "oslo"), 1);
        assert_eq!(rows(&core, "$.n", "3"), 1);
        assert_eq!(
            fields(&core),
            ["$.city", "$.n"].map(String::from).into(),
            "one entry per scalar top-level key"
        );

        put(&mut core, serde_json::json!({"n": 4}));
        assert_eq!(rows(&core, "$.city", "oslo"), 0, "a dropped key tombstones");
        assert_eq!(rows(&core, "$.n", "3"), 0);
        assert_eq!(rows(&core, "$.n", "4"), 1);
    }
}
