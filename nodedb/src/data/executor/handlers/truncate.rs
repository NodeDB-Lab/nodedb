//! TRUNCATE and ESTIMATE_COUNT handlers.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// TRUNCATE: delete all documents in a collection without filter scanning.
    ///
    /// Iterates the DOCUMENTS table prefix and deletes every key. Cascades to
    /// inverted index, secondary indexes, graph edges, and document cache.
    /// Returns `{"truncated": N}` payload.
    pub(in crate::data::executor) fn execute_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "truncate");

        // Collect all document IDs in this collection.
        let all_ids = match self.scan_matching_documents(tid, collection, &[]) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan for truncate: {e}"),
                    },
                );
            }
        };

        // Delete each document with full cascade.
        let mut truncated = 0u64;
        for doc_id in &all_ids {
            if self.sparse.delete(tid, collection, doc_id).unwrap_or(false) {
                let scoped_coll = format!("{tid}:{collection}");
                if let Err(e) = self.inverted.remove_document(&scoped_coll, doc_id) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: inverted removal failed");
                }
                if let Err(e) = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, doc_id)
                {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: index cascade failed");
                }
                let edges = self.csr_partition_mut(tid).remove_node_edges(doc_id);
                if edges > 0
                    && let Err(e) = self
                        .edge_store
                        .delete_edges_for_node(nodedb_types::TenantId::new(tid), doc_id)
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "truncate: edge cascade failed");
                }
                self.doc_cache.invalidate(tid, collection, doc_id);
                truncated += 1;
            }
        }

        // Clear aggregate cache for this collection.
        let cache_prefix = format!("{tid}:{collection}\0");
        self.aggregate_cache
            .retain(|k, _| !k.starts_with(&cache_prefix));

        debug!(core = self.core_id, %collection, truncated, "truncate complete");
        let result = serde_json::json!({ "truncated": truncated });
        match response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// ESTIMATE_COUNT: return approximate row count from HLL cardinality stats.
    pub(in crate::data::executor) fn execute_estimate_count(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        match self.stats_store.get(tid, collection, field) {
            Ok(Some(stats)) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": stats.distinct_count,
                    "row_count": stats.row_count,
                    "null_count": stats.null_count,
                });
                match response_codec::encode_json(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": 0,
                    "row_count": 0,
                    "null_count": 0,
                });
                match response_codec::encode_json(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
