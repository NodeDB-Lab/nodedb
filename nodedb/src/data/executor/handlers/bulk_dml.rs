//! Bulk DML handlers: BulkUpdate, BulkDelete.
//!
//! These operate on document sets matching ScanFilter predicates,
//! unlike PointUpdate/PointDelete which require `WHERE id = 'x'`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns document IDs of all matching documents.
    pub(in crate::data::executor) fn scan_matching_documents(
        &self,
        tid: u32,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<String>> {
        let prefix = format!("{tid}:{collection}:");
        let end = format!("{tid}:{collection}:\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                if let Some(doc) = super::super::doc_format::decode_document(value_bytes)
                    && filters.iter().all(|f| f.matches(&doc))
                    && let Some(doc_id) = key.strip_prefix(&prefix)
                {
                    ids.push(doc_id.to_string());
                }
            }
        }
        Ok(ids)
    }

    /// Bulk update: scan documents matching filters, apply field updates.
    ///
    /// When `returning` is false, returns affected row count as JSON:
    /// `{"affected": N}`.
    ///
    /// When `returning` is true, returns a JSON array of the updated documents
    /// (post-update state). If 0 rows match, returns `{"affected": 0}`.
    pub(in crate::data::executor) fn execute_bulk_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        filter_bytes: &[u8],
        updates: &[(String, Vec<u8>)],
        returning: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, returning, "bulk update");

        let filters: Vec<ScanFilter> = match rmp_serde::from_slice(filter_bytes) {
            Ok(f) => f,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("deserialize filters: {e}"),
                    },
                );
            }
        };

        let matching_ids = match self.scan_matching_documents(tid, collection, &filters) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Apply updates to each matching document.
        let mut affected = 0u64;
        let mut returned_docs: Vec<serde_json::Value> = if returning {
            Vec::with_capacity(matching_ids.len())
        } else {
            Vec::new()
        };

        for doc_id in &matching_ids {
            match self.sparse.get(tid, collection, doc_id) {
                Ok(Some(current_bytes)) => {
                    let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                        Some(v) => v,
                        None => continue,
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
                    if self
                        .sparse
                        .put(tid, collection, doc_id, &updated_bytes)
                        .is_ok()
                    {
                        self.doc_cache.put(tid, collection, doc_id, &updated_bytes);
                        affected += 1;
                        if returning {
                            // Include document ID in the returned document.
                            if let Some(obj) = doc.as_object_mut() {
                                obj.insert(
                                    "id".to_string(),
                                    serde_json::Value::String(doc_id.clone()),
                                );
                            }
                            returned_docs.push(doc);
                        }
                    }
                }
                _ => continue,
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk update complete");

        if returning && affected > 0 {
            let payload =
                serde_json::to_vec(&serde_json::Value::Array(returned_docs)).unwrap_or_default();
            self.response_with_payload(task, payload)
        } else {
            let payload = serde_json::json!({ "affected": affected });
            self.response_with_payload(task, serde_json::to_vec(&payload).unwrap_or_default())
        }
    }

    /// Bulk delete: scan documents matching filters, delete all matches.
    ///
    /// Cascades to inverted index, secondary indexes, and graph edges.
    /// Returns affected row count as JSON payload: `{"affected": N}`.
    pub(in crate::data::executor) fn execute_bulk_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        filter_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "bulk delete");

        let filters: Vec<ScanFilter> = match rmp_serde::from_slice(filter_bytes) {
            Ok(f) => f,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("deserialize filters: {e}"),
                    },
                );
            }
        };

        let matching_ids = match self.scan_matching_documents(tid, collection, &filters) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Delete each matching document with full cascade.
        let mut affected = 0u64;
        for doc_id in &matching_ids {
            if self.sparse.delete(tid, collection, doc_id).unwrap_or(false) {
                // Cascade: inverted index (tenant-scoped).
                let scoped_coll = format!("{tid}:{collection}");
                if let Err(e) = self.inverted.remove_document(&scoped_coll, doc_id) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: inverted index removal failed");
                }
                // Cascade: secondary indexes.
                if let Err(e) = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, doc_id)
                {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: secondary index cascade failed");
                }
                // Cascade: graph edges.
                let edges_removed = self.csr.remove_node_edges(doc_id);
                if edges_removed > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(doc_id)
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
                }
                self.deleted_nodes.insert(doc_id.to_string());
                self.doc_cache.invalidate(tid, collection, doc_id);
                affected += 1;
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk delete complete");
        let payload = serde_json::json!({ "affected": affected });
        self.response_with_payload(task, serde_json::to_vec(&payload).unwrap_or_default())
    }
}
