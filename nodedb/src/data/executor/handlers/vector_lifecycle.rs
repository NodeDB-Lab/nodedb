// SPDX-License-Identifier: BUSL-1.1

//! Vector index lifecycle handlers: stats query, seal, compact, rebuild.
//!
//! Separated from `vector.rs` (write handlers) by concern:
//! write handlers deal with inserts/deletes, these deal with index management.

use tracing::{debug, info};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_vector_rebuild`].
pub(in crate::data::executor) struct VectorRebuildParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
}

impl CoreLoop {
    /// Return live `VectorIndexStats` for a collection/field.
    ///
    /// Serializes stats as MessagePack in the response payload.
    pub(in crate::data::executor) fn execute_vector_query_stats(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            "vector query stats"
        );

        let Some(coll) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        let mut stats = coll.stats();
        stats.builds_queued = self.vector_builds.pending_for(&index_key);
        // Populate arena_bytes from the per-collection arena registry.
        // Only set when the collection was assigned a dedicated arena
        // (vector-primary collections) and the registry is wired.
        if coll.arena_index.is_some()
            && let Some(ref reg) = self.collection_arena_registry
            && let Some(handle) = reg.get(tid, collection)
        {
            stats.arena_bytes = handle.resident_bytes();
        }
        match zerompk::to_msgpack_vec(&stats) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("serialize stats: {e}"),
                },
            ),
        }
    }

    /// Force-seal the growing segment and queue its HNSW build.
    pub(in crate::data::executor) fn execute_vector_seal(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(core = self.core_id, key = &index_key.2, "vector seal");

        let memory = self.vector_build_memory(&index_key);
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if coll.growing_is_empty() {
            info!(
                core = self.core_id,
                key = &index_key.2,
                "seal: growing segment empty, nothing to seal"
            );
            return self.response_ok(task);
        }

        let seal_key = CoreLoop::vector_build_key(&index_key);
        match coll.seal(&seal_key, memory) {
            Some(req) => {
                self.queue_sealed_build(&index_key, req);
                info!(core = self.core_id, key = %seal_key, "growing segment sealed, HNSW build queued");
                self.checkpoint_coordinator.mark_dirty("vector", 1);
                self.response_ok(task)
            }
            None => self.response_ok(task),
        }
    }

    /// Force tombstone compaction on a specific vector collection.
    pub(in crate::data::executor) fn execute_vector_compact_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            "vector compact index"
        );

        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        let removed = coll.compact();
        info!(
            core = self.core_id,
            key = &index_key.2,
            removed,
            "vector index compaction complete"
        );
        if removed > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", removed);
            // A graph codec the compaction dropped is rebuilt off the core.
            self.queue_missing_codecs(&index_key);
        }

        match super::super::response_codec::encode_count("compacted", removed) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Rebuild sealed segments with new HNSW parameters.
    ///
    /// Updates the params so future builds use them. Then re-seals and
    /// re-builds all sealed segments with the new params by extracting their
    /// vectors, creating new HNSW indexes, and swapping atomically.
    pub(in crate::data::executor) fn execute_vector_rebuild(
        &mut self,
        params: VectorRebuildParams<'_>,
    ) -> Response {
        use crate::engine::vector::hnsw::HnswParams;

        let VectorRebuildParams {
            task,
            tid,
            collection,
            field_name,
            m,
            m0,
            ef_construction,
        } = params;

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            m,
            m0,
            ef_construction,
            "vector rebuild"
        );

        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Merge new params with current (0 = keep current).
        let current = coll.params().clone();
        let new_params = HnswParams {
            m: if m > 0 { m } else { current.m },
            m0: if m0 > 0 { m0 } else { current.m0 },
            ef_construction: if ef_construction > 0 {
                ef_construction
            } else {
                current.ef_construction
            },
            metric: current.metric,
            dtype: current.dtype,
        };
        coll.set_params(new_params.clone());
        self.vector_params
            .insert(index_key.clone(), new_params.clone());

        // Each sealed segment is rebuilt on the builder thread with its ids
        // kept, then swapped in on this core; search reads the old graph until
        // then. Segments sealed later build under the new params too.
        let queued = self.queue_vector_rebuild(&index_key);
        info!(
            core = self.core_id,
            key = &index_key.2,
            queued,
            m = new_params.m,
            ef = new_params.ef_construction,
            "vector index rebuild queued"
        );
        self.response_ok(task)
    }
}
