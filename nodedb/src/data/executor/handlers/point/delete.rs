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
            Ok(_) => {
                // Cascade 1: Remove from full-text inverted index (tenant-scoped).
                let scoped_coll = format!("{tid}:{collection}");
                if let Err(e) = self.inverted.remove_document(&scoped_coll, document_id) {
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
                let edges_removed = self.csr.remove_node_edges(document_id);
                if edges_removed > 0 {
                    // Also remove from persistent edge store.
                    if let Err(e) = self.edge_store.delete_edges_for_node(document_id) {
                        warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
                    }
                    tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
                }

                // Cascade 4: Remove from spatial R-tree indexes + reverse map.
                let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                let prefix = format!("{tid}:{collection}:");
                let spatial_keys: Vec<String> = self
                    .spatial_indexes
                    .keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect();
                for key in spatial_keys {
                    if let Some(rtree) = self.spatial_indexes.get_mut(&key) {
                        rtree.delete(entry_id);
                    }
                    self.spatial_doc_map.remove(&(key, entry_id));
                }

                // Record deletion for edge referential integrity.
                self.deleted_nodes.insert(document_id.to_string());

                // Invalidate document cache.
                self.doc_cache.invalidate(tid, collection, document_id);

                self.checkpoint_coordinator.mark_dirty("sparse", 1);

                // Emit delete event to Event Plane.
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    document_id,
                    None,
                    None, // old_value: would require reading before delete; future batch adds this.
                );

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
