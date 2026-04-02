//! Compaction handler: periodic and on-demand engine compaction.
//!
//! Compaction removes tombstoned vectors from HNSW indexes, compacts CSR
//! write buffers into dense arrays, and sweeps dangling edges from deleted
//! nodes. All operations run on the Data Plane (single-core, no locks).
//!
//! ## What gets compacted
//!
//! - **Vector engine**: `HnswIndex::compact()` on each sealed segment.
//!   Rebuilds the node array with only live nodes, remaps neighbor IDs,
//!   reclaims jemalloc arena memory. At 768-dim FP32 (~3 KiB/vector),
//!   compacting 1M tombstones reclaims ~3 GB.
//!
//! - **CSR index**: `CsrIndex::compact()` merges the mutable write buffer
//!   into the dense adjacency arrays. Eliminates per-node buffer overhead
//!   and restores cache-friendly sequential access.
//!
//! - **Dangling edges**: Removes edges whose source or destination was
//!   deleted (present in `deleted_nodes` set). Cleans both the in-memory
//!   CSR and the persistent redb edge store.
//!
//! ## Triggering
//!
//! - **Periodic**: The runtime event loop calls `run_maintenance()` every
//!   `COMPACTION_INTERVAL` (default 10 minutes). Only compacts collections
//!   with tombstone ratio above the threshold.
//!
//! - **On-demand**: `PhysicalPlan::Compact` dispatched from the Control
//!   Plane. Forces compaction regardless of tombstone ratio (for operator
//!   use, e.g., after a bulk delete).

use tracing::info;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute an on-demand compaction request.
    ///
    /// Forces compaction of all vector collections (regardless of tombstone
    /// ratio), CSR compaction, and dangling edge sweep. Returns a summary
    /// payload with compaction statistics.
    pub(in crate::data::executor) fn execute_compact(&mut self, task: &ExecutionTask) -> Response {
        let result = self.run_compaction(true);
        let payload = match rmp_serde::to_vec_named(&result) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "failed to encode compaction stats");
                Vec::new()
            }
        };
        self.response_with_payload(task, payload)
    }

    /// Run all maintenance/compaction tasks.
    ///
    /// Called periodically from the runtime event loop (idle maintenance)
    /// and on-demand via `PhysicalPlan::Compact`.
    ///
    /// When `force` is false (periodic), only compacts collections whose
    /// tombstone ratio exceeds the threshold. When `force` is true
    /// (on-demand), compacts everything.
    pub fn run_compaction(&mut self, force: bool) -> CompactionStats {
        let mut stats = CompactionStats::default();

        // 1. Vector compaction: remove tombstoned nodes from HNSW indexes.
        for (key, collection) in &mut self.vector_collections {
            // Check tombstone ratio across all sealed segments.
            let total_tombstones: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.tombstone_count())
                .sum();
            let total_nodes: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.len())
                .sum();

            if total_tombstones == 0 {
                continue;
            }

            let ratio = if total_nodes > 0 {
                total_tombstones as f64 / total_nodes as f64
            } else {
                0.0
            };

            if !force && ratio < self.compaction_tombstone_threshold {
                continue;
            }

            let removed = collection.compact();
            if removed > 0 {
                info!(
                    core = self.core_id,
                    collection = %key,
                    removed,
                    ratio = format!("{ratio:.2}"),
                    "vector compaction: tombstones removed"
                );
                stats.vectors_compacted += removed;
                stats.collections_compacted += 1;
            }
        }

        // 2. CSR compaction: merge write buffers into dense arrays.
        self.csr.compact();
        stats.csr_compacted = true;

        // 3. Dangling edge sweep.
        stats.edges_swept = self.sweep_dangling_edges();

        // 4. L1 segment compaction: merge small/tombstoned timeseries segments.
        stats.segments_merged = self.run_segment_compaction(force);

        if stats.vectors_compacted > 0 || stats.edges_swept > 0 || stats.segments_merged > 0 {
            info!(
                core = self.core_id,
                vectors_compacted = stats.vectors_compacted,
                collections_compacted = stats.collections_compacted,
                edges_swept = stats.edges_swept,
                segments_merged = stats.segments_merged,
                "compaction cycle complete"
            );
        }

        stats
    }

    /// Run maintenance tasks if enough time has elapsed.
    ///
    /// Called from the runtime event loop on every idle wake. Tracks the
    /// last maintenance time internally and skips if the interval hasn't
    /// elapsed. Returns `true` if maintenance was executed.
    pub fn maybe_run_maintenance(&mut self) -> bool {
        // Checkpoint coordinator tick: incremental dirty page flushing.
        // Runs on its own interval (independent from compaction interval).
        let flush_plan = self.checkpoint_coordinator.tick();
        for (engine, pages) in &flush_plan {
            match engine.as_str() {
                "vector" => {
                    let flushed = self.checkpoint_vector_indexes();
                    self.checkpoint_coordinator
                        .record_flush("vector", flushed.min(*pages));
                }
                "crdt" => {
                    let flushed = self.checkpoint_crdt_engines();
                    self.checkpoint_coordinator
                        .record_flush("crdt", flushed.min(*pages));
                }
                "sparse" => {
                    // redb is ACID — writes are already durable.
                    self.checkpoint_coordinator.record_flush("sparse", *pages);
                }
                "timeseries" => {
                    // Idle flush: if no ingest for 5 seconds, flush all
                    // non-empty memtables so data becomes queryable.
                    let idle_threshold = std::time::Duration::from_secs(5);
                    let is_idle = self
                        .last_ts_ingest
                        .map(|t| t.elapsed() >= idle_threshold)
                        .unwrap_or(false);

                    if is_idle {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let collections: Vec<String> = self
                            .columnar_memtables
                            .iter()
                            .filter(|(_, mt)| !mt.is_empty())
                            .map(|(name, _)| name.clone())
                            .collect();
                        let mut flushed = 0usize;
                        for collection in &collections {
                            self.flush_ts_collection(collection, now_ms);
                            flushed += 1;
                        }
                        if flushed > 0 {
                            tracing::info!(
                                core = self.core_id,
                                flushed,
                                "idle flush: timeseries memtables flushed"
                            );
                        }
                        // Reset so we don't re-flush until next ingest.
                        self.last_ts_ingest = None;
                        self.checkpoint_coordinator
                            .record_flush("timeseries", flushed.max(*pages));
                    } else {
                        self.checkpoint_coordinator
                            .record_flush("timeseries", *pages);
                    }
                }
                _ => {}
            }
        }
        if self.checkpoint_coordinator.is_clean()
            && !flush_plan.is_empty()
            && self.checkpoint_coordinator.total_dirty_pages() == 0
        {
            self.checkpoint_coordinator
                .complete_checkpoint(self.watermark.as_u64());
        }

        // KV expiry wheel tick: process expired keys on every maintenance call.
        // Bounded by the per-tick reap budget internally — safe for the reactor.
        // Expired keys are emitted as structured log events for CDC visibility.
        {
            let now_ms = crate::engine::kv::current_ms();
            let expired_keys = self.kv_engine.tick_expiry(now_ms);
            if !expired_keys.is_empty() {
                tracing::debug!(
                    core = self.core_id,
                    reaped = expired_keys.len(),
                    backlog = self.kv_engine.expiry_backlog(),
                    "kv expiry wheel tick"
                );

                for ek in &expired_keys {
                    tracing::info!(
                        target: "nodedb::kv::expired",
                        tenant_id = ek.tenant_id,
                        collection = %ek.collection,
                        key_len = ek.key.len(),
                        "kv key expired"
                    );
                }
            }
        }

        // Compaction: periodic tombstone removal + segment merge.
        let now = std::time::Instant::now();
        if let Some(last) = self.last_maintenance
            && now.duration_since(last) < self.compaction_interval
        {
            return !flush_plan.is_empty();
        }
        self.last_maintenance = Some(now);
        self.run_compaction(false);
        true
    }
}

impl CoreLoop {
    /// Run L1 segment compaction using the partition registry's merge logic.
    ///
    /// Finds eligible sealed partitions via `find_mergeable`, marks them
    /// for merge, and purges expired/deleted partitions. The actual merge
    /// I/O is handled by the partition registry and flush pipeline.
    fn run_segment_compaction(&mut self, force: bool) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let max_per_pass = self.segment_compaction_config.max_segments_per_pass;
        let mut total_merged = 0;

        for (collection, registry) in &mut self.ts_registries {
            // Find mergeable partition groups, limited by config.
            let mut groups = registry.find_mergeable(now_ms);
            if !force {
                groups.truncate(max_per_pass);
            }
            for group in &groups {
                for &start_ts in group {
                    registry.mark_merging(start_ts);
                }
                total_merged += group.len();
            }

            // Retention: find and purge expired partitions.
            let expired = registry.find_expired(now_ms);
            for start_ts in &expired {
                registry.mark_deleted(*start_ts);
            }
            let purged = registry.purge_deleted();

            if !groups.is_empty() || !purged.is_empty() {
                info!(
                    core = self.core_id,
                    collection = %collection,
                    merge_groups = groups.len(),
                    expired = expired.len(),
                    purged = purged.len(),
                    "timeseries partition compaction"
                );
            }

            // Force mode: also compact partitions that wouldn't normally merge.
            if force && total_merged == 0 {
                let sealed_count = registry.sealed_count();
                if sealed_count >= 2 {
                    info!(
                        core = self.core_id,
                        collection = %collection,
                        sealed_count,
                        "forced compaction: marking sealed partitions for merge"
                    );
                }
            }
        }

        total_merged
    }
}

/// Statistics from a compaction cycle.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactionStats {
    /// Number of tombstoned vectors removed across all collections.
    pub vectors_compacted: usize,
    /// Number of collections that had tombstones compacted.
    pub collections_compacted: usize,
    /// Whether CSR write buffers were compacted.
    pub csr_compacted: bool,
    /// Number of dangling edges swept.
    pub edges_swept: usize,
    /// Number of L1 segments selected for merge compaction.
    pub segments_merged: usize,
}

#[cfg(test)]
mod tests {
    use crate::engine::vector::hnsw::graph::HnswParams;

    #[test]
    fn compaction_removes_tombstones() {
        // Test HNSW compaction directly (sealed segment tombstone removal).
        let mut idx = crate::engine::vector::hnsw::graph::HnswIndex::new(4, HnswParams::default());
        for i in 0..20u32 {
            idx.insert(vec![i as f32; 4]);
        }
        for i in 0..10u32 {
            idx.delete(i);
        }
        assert_eq!(idx.tombstone_count(), 10);
        assert_eq!(idx.live_count(), 10);

        let removed = idx.compact();
        assert_eq!(removed, 10);
        assert_eq!(idx.live_count(), 10);
        assert_eq!(idx.tombstone_count(), 0);
    }

    #[test]
    fn maintenance_respects_interval() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        // First call should run.
        assert!(core.maybe_run_maintenance());

        // Immediate second call should skip.
        assert!(!core.maybe_run_maintenance());
    }

    #[test]
    fn forced_compaction_ignores_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        // Force compaction with no data — should succeed without error.
        let stats = core.run_compaction(true);
        assert_eq!(stats.vectors_compacted, 0);
        assert!(stats.csr_compacted);
    }
}
