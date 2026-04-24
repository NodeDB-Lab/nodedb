//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point delete");
        match self.sparse.delete(tid, collection, document_id) {
            Ok(prior) => {
                // Cascade 1: Remove from full-text inverted index.
                if let Err(e) = self.inverted.remove_document(
                    crate::types::TenantId::new(tid),
                    collection,
                    document_id,
                ) {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed");
                }

                // Cascade 2: Remove secondary index entries for this document.
                // Secondary indexes use key format "{tenant}:{collection}:{field}:{value}:{doc_id}".
                // We scan and delete all entries ending with this doc_id.
                if let Err(e) =
                    self.sparse
                        .delete_indexes_for_document(tid, collection, document_id)
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "secondary index cascade failed");
                }

                // Cascade 3: Remove graph edges where this document is src or dst.
                let edges_removed = self.csr_partition_mut(tid).remove_node_edges(document_id);
                if edges_removed > 0 {
                    // Also tombstone in persistent edge store.
                    let cascade_ord = self.hlc.next_ordinal();
                    if let Err(e) = self.edge_store.delete_edges_for_node(
                        nodedb_types::TenantId::new(tid),
                        document_id,
                        cascade_ord,
                    ) {
                        warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
                    }
                    tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
                }

                // Cascade 4: Remove from spatial R-tree indexes + reverse map.
                let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                let tid_id = crate::types::TenantId::new(tid);
                let spatial_fields: Vec<String> = self
                    .spatial_indexes
                    .keys()
                    .filter(|(t, c, _)| *t == tid_id && c == collection)
                    .map(|(_, _, f)| f.clone())
                    .collect();
                for field in spatial_fields {
                    let skey = (tid_id, collection.to_string(), field.clone());
                    if let Some(rtree) = self.spatial_indexes.get_mut(&skey) {
                        rtree.delete(entry_id);
                    }
                    self.spatial_doc_map
                        .remove(&(tid_id, collection.to_string(), field, entry_id));
                }

                // Record deletion for edge referential integrity.
                self.mark_node_deleted(tid, document_id);

                // Invalidate document cache.
                self.doc_cache.invalidate(tid, collection, document_id);

                self.checkpoint_coordinator.mark_dirty("sparse", 1);

                // Emit delete event to Event Plane if the row actually
                // existed. `sparse.delete` returns the prior bytes — we
                // thread them through so CDC/trigger consumers see the
                // pre-delete state as `old_value`. A delete against a
                // non-existent key is a true no-op and emits nothing.
                if let Some(prior_bytes) = prior.as_deref() {
                    let old_converted = self.resolve_event_payload(tid, collection, prior_bytes);
                    self.emit_write_event(
                        task,
                        collection,
                        crate::event::WriteOp::Delete,
                        document_id,
                        None,
                        Some(old_converted.as_deref().unwrap_or(prior_bytes)),
                    );
                }

                self.response_ok(task)
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
