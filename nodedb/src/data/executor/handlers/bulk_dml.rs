//! Bulk DML handlers: BulkUpdate, BulkDelete.
//!
//! These operate on document sets matching ScanFilter predicates,
//! unlike PointUpdate/PointDelete which require `WHERE id = 'x'`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
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

        // Check if this is a strict (Binary Tuple) collection.
        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                let matches = if let Some(ref schema) = strict_schema {
                    // Strict: Binary Tuple → Value → MessagePack → matches_binary.
                    match super::super::strict_format::binary_tuple_to_json(value_bytes, schema) {
                        Some(doc) => {
                            let msgpack = super::super::doc_format::encode_to_msgpack(&doc);
                            filters.iter().all(|f| f.matches_binary(&msgpack))
                        }
                        None => false,
                    }
                } else {
                    filters.iter().all(|f| f.matches_binary(value_bytes))
                };
                if matches && let Some(doc_id) = key.strip_prefix(&prefix) {
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
        updates: &[(String, crate::bridge::physical_plan::UpdateValue)],
        returning: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, returning, "bulk update");

        // Reject direct updates to generated columns.
        let config_key = format!("{tid}:{collection}");
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = super::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
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

        // Check if this is a strict (Binary Tuple) collection.
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

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
                    // Decode current value — format depends on storage mode.
                    let mut doc = if let Some(ref schema) = strict_schema {
                        match super::super::strict_format::binary_tuple_to_json(
                            &current_bytes,
                            schema,
                        ) {
                            Some(v) => v,
                            None => continue,
                        }
                    } else {
                        match super::super::doc_format::decode_document(&current_bytes) {
                            Some(v) => v,
                            None => continue,
                        }
                    };
                    // Snapshot the current row for expression evaluation. All
                    // expression assignments see the pre-update state — multiple
                    // assignments in the same UPDATE do not observe each other,
                    // matching PostgreSQL semantics.
                    let eval_doc: nodedb_types::Value = doc.clone().into();
                    if let Some(obj) = doc.as_object_mut() {
                        for (field, update_val) in updates {
                            let val: serde_json::Value = match update_val {
                                crate::bridge::physical_plan::UpdateValue::Literal(bytes) => {
                                    match nodedb_types::json_from_msgpack(bytes) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    }
                                }
                                crate::bridge::physical_plan::UpdateValue::Expr(expr) => {
                                    let result: nodedb_types::Value = expr.eval(&eval_doc);
                                    result.into()
                                }
                            };
                            obj.insert(field.clone(), val);
                        }
                    }
                    // Recompute generated columns if any dependency changed.
                    if let Some(config) = self.doc_configs.get(&config_key)
                        && !config.enforcement.generated_columns.is_empty()
                        && super::generated::needs_recomputation(
                            updates,
                            &config.enforcement.generated_columns,
                        )
                        && let Err(e) = super::generated::evaluate_generated_columns(
                            &mut doc,
                            &config.enforcement.generated_columns,
                        )
                    {
                        tracing::warn!(
                            %doc_id,
                            error = ?e,
                            "generated column recomputation failed, skipping document"
                        );
                        continue;
                    }
                    // Re-encode — format depends on storage mode.
                    let updated_bytes = if let Some(ref schema) = strict_schema {
                        let ndb_val: nodedb_types::Value = doc.clone().into();
                        match super::super::strict_format::value_to_binary_tuple(&ndb_val, schema) {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                tracing::warn!(
                                    %doc_id,
                                    error = %e,
                                    "strict re-encode failed, skipping document"
                                );
                                continue;
                            }
                        }
                    } else {
                        super::super::doc_format::encode_to_msgpack(&doc)
                    };
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
            let result = serde_json::Value::Array(returned_docs);
            match response_codec::encode_json(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        } else {
            let result = serde_json::json!({ "affected": affected });
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

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
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
        let result = serde_json::json!({ "affected": affected });
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
}
