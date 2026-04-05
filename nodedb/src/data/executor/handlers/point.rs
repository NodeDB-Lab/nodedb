//! Point operation handlers: PointGet, PointPut, PointDelete, PointUpdate.

use redb::WriteTransaction;
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
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point get");

        // Check if this is a strict collection — affects decode format.
        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Fetch data from cache or redb.
        let cached = self
            .doc_cache
            .get(tid, collection, document_id)
            .map(|v| v.to_vec());
        let data = if let Some(data) = cached {
            data
        } else {
            match self.sparse.get(tid, collection, document_id) {
                Ok(Some(data)) => {
                    self.doc_cache.put(tid, collection, document_id, &data);
                    data
                }
                Ok(None) => return self.response_error(task, ErrorCode::NotFound),
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "sparse get failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        // RLS post-fetch: evaluate filters. For strict collections, decode
        // Binary Tuple to JSON for RLS evaluation, then return JSON.
        // For schemaless, check directly against MessagePack bytes.
        if !rls_filters.is_empty() {
            if strict_schema.is_some() {
                // Strict: decode to JSON, check RLS against JSON.
                if let Some(ref schema) = strict_schema
                    && let Some(json) =
                        super::super::strict_format::binary_tuple_to_json(&data, schema)
                {
                    let json_bytes = serde_json::to_vec(&json).unwrap_or_default();
                    if !super::rls_eval::rls_check_msgpack_bytes(rls_filters, &json_bytes) {
                        return self.response_error(task, ErrorCode::NotFound);
                    }
                }
            } else if !super::rls_eval::rls_check_msgpack_bytes(rls_filters, &data) {
                return self.response_error(task, ErrorCode::NotFound);
            }
        }

        // For strict collections, return JSON instead of raw Binary Tuple
        // (clients expect JSON, not Binary Tuple).
        if let Some(ref schema) = strict_schema
            && let Some(json) = super::super::strict_format::binary_tuple_to_json(&data, schema)
        {
            let json_bytes = serde_json::to_vec(&json).unwrap_or_default();
            return self.response_with_payload(task, json_bytes);
        }

        self.response_with_payload(task, data)
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

        // Unified write transaction: document + inverted index + stats in one commit.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        if let Err(e) = self.apply_point_put(&txn, tid, collection, document_id, value) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Emit write event to Event Plane (after successful commit).
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            document_id,
            Some(value),
            None,
        );

        self.response_ok(task)
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

    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        updates: &[(String, Vec<u8>)],
        returning: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, fields = updates.len(), returning, "point update");

        let config_key = format!("{tid}:{collection}");
        let is_strict = self.doc_configs.get(&config_key).is_some_and(|c| {
            matches!(
                c.storage_mode,
                crate::bridge::physical_plan::StorageMode::Strict { .. }
            )
        });

        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(current_bytes)) => {
                // Decode current value — format depends on storage mode.
                let mut doc = if is_strict {
                    if let Some(config) = self.doc_configs.get(&config_key)
                        && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                            config.storage_mode
                    {
                        match super::super::strict_format::binary_tuple_to_json(
                            &current_bytes,
                            schema,
                        ) {
                            Some(v) => v,
                            None => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: "failed to decode Binary Tuple for update".into(),
                                    },
                                );
                            }
                        }
                    } else {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "strict config missing during update".into(),
                            },
                        );
                    }
                } else {
                    match super::super::doc_format::decode_document(&current_bytes) {
                        Some(v) => v,
                        None => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "failed to parse document for update".into(),
                                },
                            );
                        }
                    }
                };

                // Apply field-level updates.
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

                // Re-encode — format depends on storage mode.
                let updated_bytes = if is_strict {
                    if let Some(config) = self.doc_configs.get(&config_key)
                        && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                            config.storage_mode
                    {
                        let json_bytes = serde_json::to_vec(&doc).unwrap_or_default();
                        match super::super::strict_format::json_to_binary_tuple(&json_bytes, schema)
                        {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("strict re-encode: {e}"),
                                    },
                                );
                            }
                        }
                    } else {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "strict config missing during re-encode".into(),
                            },
                        );
                    }
                } else {
                    super::super::doc_format::encode_to_msgpack(&doc)
                };

                match self
                    .sparse
                    .put(tid, collection, document_id, &updated_bytes)
                {
                    Ok(()) => {
                        self.doc_cache
                            .put(tid, collection, document_id, &updated_bytes);

                        // Emit update event to Event Plane.
                        self.emit_write_event(
                            task,
                            collection,
                            crate::event::WriteOp::Update,
                            document_id,
                            Some(&updated_bytes),
                            Some(&current_bytes),
                        );

                        if returning {
                            // Return post-update document as JSON array (consistent with bulk).
                            if let Some(obj) = doc.as_object_mut() {
                                obj.insert(
                                    "id".to_string(),
                                    serde_json::Value::String(document_id.to_string()),
                                );
                            }
                            let payload = serde_json::to_vec(&serde_json::Value::Array(vec![doc]))
                                .unwrap_or_default();
                            self.response_with_payload(task, payload)
                        } else {
                            let payload = serde_json::json!({ "affected": 1u64 });
                            self.response_with_payload(
                                task,
                                serde_json::to_vec(&payload).unwrap_or_default(),
                            )
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
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Apply a PointPut within an externally-owned WriteTransaction.
    ///
    /// Stores the document, auto-indexes text fields, updates column stats,
    /// and populates the document cache. Does NOT commit the transaction.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        // Check if this collection uses strict (Binary Tuple) encoding.
        let config_key = format!("{tid}:{collection}");
        let stored = if let Some(config) = self.doc_configs.get(&config_key)
            && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            super::super::strict_format::json_to_binary_tuple(value, schema).map_err(|e| {
                crate::Error::Serialization {
                    format: "binary_tuple".into(),
                    detail: e,
                }
            })?
        } else {
            super::super::doc_format::json_to_msgpack(value)
        };

        self.sparse
            .put_in_txn(txn, tid, collection, document_id, &stored)?;

        // Text indexing and stats use the original JSON input, not the stored
        // bytes — Binary Tuple requires a schema to decode, and the input JSON
        // is already available here regardless of storage mode.
        if let Some(doc) = super::super::doc_format::decode_document(value) {
            if let Some(obj) = doc.as_object() {
                let text_content: String = obj
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !text_content.is_empty()
                    && let Err(e) = self.inverted.index_document_in_txn(
                        txn,
                        &config_key, // "{tid}:{collection}" — tenant-scoped
                        document_id,
                        &text_content,
                    )
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed");
                }
            }

            if let Err(e) = self
                .stats_store
                .observe_document_in_txn(txn, tid, collection, &doc)
            {
                warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
            }

            let cache_prefix = format!("{tid}:{collection}\0");
            self.aggregate_cache
                .retain(|k, _| !k.starts_with(&cache_prefix));
        }

        self.doc_cache.put(tid, collection, document_id, &stored);

        // Secondary index extraction: if this collection has registered index paths,
        // extract values from the incoming document and store them in the INDEXES
        // redb B-Tree for range-scan-based lookups.
        let config_key = format!("{tid}:{collection}");
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Some(doc) = super::super::doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            self.apply_secondary_indexes(tid, collection, &doc, document_id, &paths);
        }

        // Spatial index: detect geometry fields and insert into R-tree.
        // Tries to parse each object field as a GeoJSON Geometry.
        // If successful, computes bbox and inserts into the per-field R-tree.
        // Also writes the document to columnar_memtables so that bare table scans
        // and aggregates on spatial collections read from columnar (spatial extends columnar).
        if let Some(doc) = super::super::doc_format::decode_document(value)
            && let Some(obj) = doc.as_object()
        {
            let mut has_geometry = false;
            for (field_name, field_value) in obj {
                if let Ok(geom) =
                    serde_json::from_value::<nodedb_types::geometry::Geometry>(field_value.clone())
                {
                    has_geometry = true;
                    let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                    let index_key = format!("{tid}:{collection}:{field_name}");
                    let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                    let rtree = self.spatial_indexes.entry(index_key.clone()).or_default();
                    rtree.insert(nodedb_spatial::RTreeEntry { id: entry_id, bbox });
                    // Maintain reverse map: entry_id → document_id.
                    self.spatial_doc_map
                        .insert((index_key, entry_id), document_id.to_string());
                }
            }

            // If document has geometry, also write to columnar memtable.
            // This ensures bare scans + aggregates work via columnar path.
            if has_geometry {
                self.ingest_doc_to_columnar(collection, obj);
            }
        }

        Ok(())
    }
}
