// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use super::CoreLoop;
use crate::engine::document::store::StorageKey;
use crate::engine::sparse::doc_cache::DocCache;

/// (added, removed) secondary-index (field, value) tuples.
type SecondaryIndexDiff = (Vec<(String, String)>, Vec<(String, String)>);

/// Shared parameters for [`CoreLoop::apply_secondary_indexes_in_txn`].
pub(in crate::data::executor) struct SecondaryIndexInputs<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub old_doc: Option<&'a serde_json::Value>,
    pub new_doc: &'a serde_json::Value,
    pub doc_id: &'a StorageKey,
    pub index_paths: &'a [crate::engine::document::store::IndexPath],
}

impl CoreLoop {
    /// Set compaction parameters (called after open, before event loop).
    pub fn set_compaction_config(
        &mut self,
        interval: std::time::Duration,
        tombstone_threshold: f64,
    ) {
        self.compaction_interval = interval;
        self.compaction_tombstone_threshold = tombstone_threshold;
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
        self.maintenance_budget = Some(tracker);
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
        self.segment_compaction_config = config;
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
    pub fn set_timeseries_tuning(
        &mut self,
        tuning: nodedb_types::config::tuning::TimeseriesToning,
    ) {
        self.ts_tuning = tuning;
    }

    /// Apply the secondary-index SET diff within an already-open write txn.
    ///
    /// Routes writes through [`SparseEngine::index_put_in_txn`] /
    /// [`SparseEngine::index_remove_in_txn`] so the document + index entries
    /// commit atomically with the caller's `WriteTransaction`. There is no
    /// own-transaction variant: every caller already holds the row's write
    /// txn, and a nested `begin_write` deadlocks redb's single-writer lock.
    ///
    /// `old_doc` is the pre-write document (`None` for a fresh insert). The set
    /// of indexed values is diffed against the new document: values only in the
    /// new set are inserted, values only in the old set are removed — so an
    /// UPDATE that changes a field value drops the stale entry. Returns
    /// `(added, removed)` as `(field, value)` tuples.
    ///
    /// An index write that fails propagates rather than being stepped over.
    /// The entries land in the CALLER'S transaction alongside the row body, so
    /// every caller returns before `txn.commit()` and redb rolls both halves
    /// back — the row does not exist, rather than existing with an index that
    /// cannot find it. Stepping over the failure would be permanent, not
    /// transient: nothing re-derives the missing entry, the next write to the
    /// document diffs against the values this one believed it wrote, and a
    /// lookup on the indexed field can no longer distinguish "no such row"
    /// from "the row is there and the index forgot it".
    pub(in crate::data::executor) fn apply_secondary_indexes_in_txn(
        &mut self,
        txn: &redb::WriteTransaction,
        inputs: SecondaryIndexInputs<'_>,
    ) -> crate::Result<SecondaryIndexDiff> {
        let SecondaryIndexInputs {
            database_id,
            tid,
            collection,
            old_doc,
            new_doc,
            doc_id,
            index_paths,
        } = inputs;
        let mut added = Vec::new();
        let mut removed = Vec::new();
        for index_path in index_paths {
            let new_vals = index_values_for(new_doc, index_path);
            let old_vals = old_doc
                .map(|d| index_values_for(d, index_path))
                .unwrap_or_default();
            for v in new_vals.difference(&old_vals) {
                self.sparse.index_put_in_txn(
                    txn,
                    crate::engine::sparse::btree_index::IndexEntryTxn {
                        database_id,
                        tenant_id: tid,
                        collection,
                        field: &index_path.path,
                        value: v,
                        document_id: doc_id,
                    },
                )?;
                added.push((index_path.path.clone(), v.clone()));
            }
            for v in old_vals.difference(&new_vals) {
                self.sparse.index_remove_in_txn(
                    txn,
                    crate::engine::sparse::btree_index::IndexEntryTxn {
                        database_id,
                        tenant_id: tid,
                        collection,
                        field: &index_path.path,
                        value: v,
                        document_id: doc_id,
                    },
                )?;
                removed.push((index_path.path.clone(), v.clone()));
            }
        }
        Ok((added, removed))
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
    /// a set of index paths, via the same [`index_values_for`] extraction the
    /// forward write path uses. Used to recompute the secondary-index tuples a
    /// pre-delete document would have contributed, when the delete cascade
    /// itself (a prefix scan) does not return them.
    pub(in crate::data::executor) fn index_tuples_for_doc(
        &self,
        doc: &serde_json::Value,
        index_paths: &[crate::engine::document::store::IndexPath],
    ) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for index_path in index_paths {
            for v in index_values_for(doc, index_path) {
                out.push((index_path.path.clone(), v));
            }
        }
        out
    }

    /// Sweep dangling edges: detect edges whose source or destination
    /// node has been deleted (tracked per-tenant in `deleted_nodes`).
    ///
    /// Called periodically from the idle loop. Removes dangling edges
    /// from the tenant's CSR partition and from the tenant-scoped
    /// edge store. Returns the total number of edges removed.
    pub fn sweep_dangling_edges(&mut self) -> usize {
        if self.deleted_nodes.is_empty() {
            return 0;
        }
        let mut removed = 0;
        // Copy (database, tenant, node) tuples so we can mutate `self.csr`
        // and `self.edge_store` without borrowing the map during
        // iteration.
        let work: Vec<(nodedb_types::DatabaseId, crate::types::TenantId, String)> = self
            .deleted_nodes
            .iter()
            .flat_map(|((db, tid), set)| set.iter().map(move |n| (*db, *tid, n.clone())))
            .collect();
        let swept_nodes = work.len();
        for (db, tid, node) in &work {
            let edges = match self.csr.partition_mut(*db, *tid) {
                Some(partition) => partition.remove_node_edges(node),
                None => 0,
            };
            if edges > 0 {
                let ord = self.hlc.next_ordinal();
                if let Err(e) = self
                    .edge_store
                    .delete_edges_for_node(db.as_u64(), *tid, node, ord)
                {
                    tracing::warn!(
                        core = self.core_id,
                        db = db.as_u64(),
                        tid = tid.as_u64(),
                        node = %node,
                        error = %e,
                        "sweep: failed to delete edges from store"
                    );
                }
                removed += edges;
            }
        }
        if removed > 0 {
            tracing::info!(
                core = self.core_id,
                removed,
                deleted_nodes = swept_nodes,
                "dangling edge sweep complete"
            );
        }
        removed
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
) -> std::collections::HashSet<String> {
    if let Some(ref p) = index_path.predicate
        && !p.evaluate_json(doc)
    {
        return std::collections::HashSet::new();
    }
    crate::engine::document::store::extract_index_values(doc, &index_path.path, index_path.is_array)
        .into_iter()
        .map(|v| maybe_lowercase(&v, index_path.case_insensitive))
        .collect()
}
