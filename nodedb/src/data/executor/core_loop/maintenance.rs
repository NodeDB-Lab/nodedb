use std::sync::Arc;

use super::CoreLoop;
use crate::engine::sparse::doc_cache::DocCache;

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
    pub fn set_metrics(&mut self, metrics: Arc<crate::control::metrics::SystemMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Set memory governor for per-engine budget enforcement.
    pub fn set_governor(&mut self, governor: Arc<nodedb_mem::MemoryGovernor>) {
        self.governor = Some(governor);
    }

    /// Set checkpoint coordinator config (called after open, before event loop).
    pub fn set_checkpoint_config(&mut self, config: crate::storage::checkpoint::CheckpointConfig) {
        self.checkpoint_coordinator =
            crate::storage::checkpoint::CheckpointCoordinator::new(config);
        self.checkpoint_coordinator.register_engine("sparse");
        self.checkpoint_coordinator.register_engine("vector");
        self.checkpoint_coordinator.register_engine("crdt");
        self.checkpoint_coordinator.register_engine("timeseries");
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

    /// Apply secondary index extraction for a document.
    ///
    /// Shared by `execute_document_batch_insert` and `execute_point_put`.
    pub(in crate::data::executor) fn apply_secondary_indexes(
        &mut self,
        tid: u32,
        collection: &str,
        doc: &serde_json::Value,
        doc_id: &str,
        index_paths: &[crate::engine::document::store::IndexPath],
    ) {
        for index_path in index_paths {
            let values = crate::engine::document::store::extract_index_values(
                doc,
                &index_path.path,
                index_path.is_array,
            );
            for v in values {
                if let Err(e) = self
                    .sparse
                    .index_put(tid, collection, &index_path.path, &v, doc_id)
                {
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        doc_id = %doc_id,
                        path = %index_path.path,
                        error = %e,
                        "secondary index extraction failed"
                    );
                }
            }
        }
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

    /// Sweep dangling edges: detect edges whose source or destination
    /// node has been deleted (present in `deleted_nodes`).
    ///
    /// Called periodically from the idle loop. Removes dangling edges
    /// from both the CSR and persistent edge store. Returns the number
    /// of edges removed.
    pub fn sweep_dangling_edges(&mut self) -> usize {
        if self.deleted_nodes.is_empty() {
            return 0;
        }
        let mut removed = 0;
        let deleted: Vec<String> = self.deleted_nodes.iter().cloned().collect();
        for node in &deleted {
            let edges = self.csr.remove_node_edges(node);
            if edges > 0 {
                if let Err(e) = self.edge_store.delete_edges_for_node(node) {
                    tracing::warn!(
                        core = self.core_id,
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
                deleted_nodes = deleted.len(),
                "dangling edge sweep complete"
            );
        }
        removed
    }
}
