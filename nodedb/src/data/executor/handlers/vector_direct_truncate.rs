// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp::DirectTruncate`: remove every row of a vector-primary
//! collection.
//!
//! Each live surrogate of the primary index is removed through the same
//! per-row path `DirectDelete` takes, so its HNSW node, payload bitmap
//! entries, sidecar row, and cached sidecar all go. A sidecar row with no
//! node behind it is removed too. The index is then reset, which drops the
//! tombstoned nodes and every sealed segment while keeping the collection's
//! configuration and registered payload indexes.

use nodedb_types::{StorageKey, Surrogate};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `VectorOp::DirectTruncate`. Reports the number of rows removed.
    pub(in crate::data::executor) fn execute_vector_direct_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "vector direct truncate");
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // No index means no row was ever written: nothing to remove.
        if !self.vector_collections.contains_key(&index_key) {
            return self.truncate_response(task, 0);
        }

        let surrogates: Vec<Surrogate> =
            match self.scan_vector_sidecar_matches(database_id, tid, collection, &[]) {
                Ok(s) => s,
                Err(e) => return self.response_error(task, e),
            };

        let mut truncated = 0usize;
        for surrogate in &surrogates {
            match self.remove_vector_direct_row(&index_key, tid, collection, *surrogate) {
                Ok(Some(_)) => truncated += 1,
                // A sidecar row with no node bound to it: drop the row so the
                // collection reads back empty.
                Ok(None) => {
                    let key = StorageKey::for_surrogate(*surrogate);
                    if let Err(e) = self.sparse.delete(database_id, tid, collection, &key) {
                        return self.response_error(task, ErrorCode::from(e));
                    }
                    self.doc_cache
                        .invalidate(database_id, tid, collection, &key);
                    truncated += 1;
                }
                Err(e) => return self.response_error(task, e),
            }
        }

        // Every row is gone; reset the index so the tombstones, sealed
        // segments, and payload bitmaps go with them.
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.truncate();
        }
        self.finish_vector_direct_write(task, &index_key, tid, collection, &surrogates);
        self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);

        debug!(core = self.core_id, %collection, truncated, "vector direct truncate complete");
        self.truncate_response(task, truncated)
    }

    /// The `{"truncated": n}` reply every `TRUNCATE` handler returns.
    fn truncate_response(&self, task: &ExecutionTask, truncated: usize) -> Response {
        match response_codec::encode_count("truncated", truncated) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
