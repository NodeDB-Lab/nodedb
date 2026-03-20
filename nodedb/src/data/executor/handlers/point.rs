//! Point operation handlers: PointGet, PointPut, PointDelete, PointUpdate.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_get(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point get");
        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(data)) => {
                // Document is stored as MessagePack (or legacy JSON).
                // Pass through directly — the Control Plane's
                // decode_payload_to_json() handles format conversion.
                self.response_with_payload(task, data)
            }
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                tracing::warn!(core = self.core_id, error = %e, "sparse get failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_point_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point put");
        let stored = super::super::doc_format::json_to_msgpack(value);
        match self.sparse.put(tid, collection, document_id, &stored) {
            Ok(()) => {
                // Auto-index text fields for full-text search.
                // Extract all string values from the document and index them.
                if let Some(doc) = super::super::doc_format::decode_document(value) {
                    if let Some(obj) = doc.as_object() {
                        let text_content: String = obj
                            .values()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(" ");
                        if !text_content.is_empty() {
                            if let Err(e) =
                                self.inverted
                                    .index_document(collection, document_id, &text_content)
                            {
                                warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed");
                            }
                        }
                    }

                    // Update column statistics for CBO.
                    if let Err(e) = self.stats_store.observe_document(tid, collection, &doc) {
                        warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
                    }
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
                // Cascade 1: Remove from full-text inverted index.
                if let Err(e) = self.inverted.remove_document(collection, document_id) {
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

                // Record deletion for edge referential integrity.
                self.deleted_nodes.insert(document_id.to_string());

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

    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, fields = updates.len(), "point update");
        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(current_bytes)) => {
                let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse document for update".into(),
                            },
                        );
                    }
                };
                if let Some(obj) = doc.as_object_mut() {
                    for (field, value_bytes) in updates {
                        let val: serde_json::Value = match serde_json::from_slice(value_bytes) {
                            Ok(v) => v,
                            Err(_) => serde_json::Value::String(
                                String::from_utf8_lossy(value_bytes).into_owned(),
                            ),
                        };
                        obj.insert(field.clone(), val);
                    }
                }
                let updated_bytes = super::super::doc_format::encode_to_msgpack(&doc);
                match self
                    .sparse
                    .put(tid, collection, document_id, &updated_bytes)
                {
                    Ok(()) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
