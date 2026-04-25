//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction.
    ///
    /// Stores the document, auto-indexes text fields, updates column stats,
    /// and populates the document cache. Does NOT commit the transaction.
    ///
    /// Returns the prior stored bytes when this put replaced an existing row,
    /// or `None` when it was a fresh insert. The caller threads the prior
    /// bytes into `emit_write_event` so the Event Plane's `WriteOp` tag
    /// reflects the actual mutation.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        // Evaluate generated columns before encoding.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let value = if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            if let Some(mut doc) = super::super::super::doc_format::decode_document(value) {
                if let Err(e) = super::super::generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                ) {
                    return Err(crate::Error::Storage {
                        engine: "generated".into(),
                        detail: format!("generated column evaluation failed: {e:?}"),
                    });
                }
                super::super::super::doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            super::super::super::doc_format::canonicalize_document_for_storage(value)
        };
        let value = &value;

        let bitemporal = self.is_bitemporal(tid, collection);
        let sys_from_ms = self.bitemporal_now_ms();
        let valid_from_ms = i64::MIN;
        let valid_until_ms = i64::MAX;

        // Check if this collection uses strict (Binary Tuple) encoding.
        let stored = if let Some(config) = self.doc_configs.get(&config_key)
            && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            if bitemporal && schema.bitemporal {
                super::super::super::strict_format::bytes_to_binary_tuple_bitemporal(
                    value,
                    schema,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                )
            } else {
                super::super::super::strict_format::bytes_to_binary_tuple(value, schema)
            }
            .map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e,
            })?
        } else {
            value.to_vec()
        };

        // Bitemporal collections version every write: read the current
        // (pre-write) version for the `prior` slot, then append a new
        // version at `sys_from = now()`. Non-bitemporal collections use
        // the legacy overwrite path, returning the old bytes redb replaced.
        let prior = if bitemporal {
            let current = self
                .sparse
                .versioned_get_current(tid, collection, document_id)?;
            self.sparse.versioned_put_in_txn(
                txn,
                crate::engine::sparse::btree_versioned::VersionedPut {
                    tenant: tid,
                    coll: collection,
                    doc_id: document_id,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                    body: &stored,
                },
            )?;
            current
        } else {
            self.sparse
                .put_in_txn(txn, tid, collection, document_id, &stored)?
        };

        // Text indexing and stats use the original JSON input, not the stored
        // bytes — Binary Tuple requires a schema to decode, and the input JSON
        // is already available here regardless of storage mode.
        if let Some(doc) = super::super::super::doc_format::decode_document(value) {
            if let Some(obj) = doc.as_object() {
                let text_content: String = obj
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !text_content.is_empty()
                    && let Err(e) = self.inverted.index_document_in_txn(
                        txn,
                        crate::types::TenantId::new(tid),
                        collection,
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

            let tid_key = crate::types::TenantId::new(tid);
            let coll_prefix = format!("{collection}\0");
            self.aggregate_cache
                .retain(|(t, rest), _| !(*t == tid_key && rest.starts_with(&coll_prefix)));
        }

        self.doc_cache.put(tid, collection, document_id, &stored);

        // Secondary index extraction: if this collection has registered
        // index paths, extract values and write them into the INDEXES redb
        // B-Tree inside the CALLER'S write txn. Using the non-_in_txn
        // variant here would deadlock — `execute_point_put` already owns
        // the only writer.
        //
        // UNIQUE enforcement runs first: for every `unique: true` path we
        // check whether the incoming value already belongs to a different
        // document and reject with a typed constraint error. The check
        // uses the sparse engine's read API, which opens a separate read
        // transaction (redb MVCC) — the read view won't see our outer
        // write txn but that's precisely the semantics we want for the
        // "does another row already hold this value" question.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Some(doc) = super::super::super::doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            check_unique_constraints(&self.sparse, tid, collection, &doc, document_id, &paths)?;
            if bitemporal {
                let sys_from = self.bitemporal_now_ms();
                for path in &paths {
                    if let Some(ref pred) = path.predicate
                        && !pred.evaluate_json(&doc)
                    {
                        continue;
                    }
                    for v in crate::engine::document::store::extract_index_values(
                        &doc,
                        &path.path,
                        path.is_array,
                    ) {
                        let value = if path.case_insensitive {
                            v.to_lowercase()
                        } else {
                            v
                        };
                        self.sparse.versioned_index_put_in_txn(
                            txn,
                            tid,
                            collection,
                            &path.path,
                            &value,
                            document_id,
                            sys_from,
                        )?;
                    }
                }
            } else {
                self.apply_secondary_indexes_in_txn(
                    txn,
                    tid,
                    collection,
                    &doc,
                    document_id,
                    &paths,
                );
            }
        }

        // Spatial index: detect geometry fields and insert into R-tree.
        // Tries to parse each object field as a GeoJSON Geometry.
        // If successful, computes bbox and inserts into the per-field R-tree.
        // Also writes the document to columnar_memtables so that bare table scans
        // and aggregates on spatial collections read from columnar (spatial extends columnar).
        if let Some(doc) = super::super::super::doc_format::decode_document(value)
            && let Some(obj) = doc.as_object()
        {
            let mut has_geometry = false;
            for (field_name, field_value) in obj {
                if let Ok(geom) =
                    serde_json::from_value::<nodedb_types::geometry::Geometry>(field_value.clone())
                {
                    has_geometry = true;
                    let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                    let tid_id = crate::types::TenantId::new(tid);
                    let spatial_key = (tid_id, collection.to_string(), field_name.clone());
                    let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                    let rtree = self.spatial_indexes.entry(spatial_key.clone()).or_default();
                    rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                    // Maintain reverse map: entry_id → document_id.
                    self.spatial_doc_map.insert(
                        (tid_id, collection.to_string(), field_name.clone(), entry_id),
                        document_id.to_string(),
                    );
                }
            }

            // If document has geometry, also write to columnar memtable.
            // This ensures bare scans + aggregates work via columnar path.
            if has_geometry {
                self.ingest_doc_to_columnar(tid, collection, obj);
            }
        }

        // Vector index: if the strict schema declares Vector(dim) columns,
        // extract float arrays and insert into HNSW so KNN search works.
        // Collect vector fields from schema first (avoids borrow conflict).
        let vector_fields: Vec<(String, u32)> = self
            .doc_configs
            .get(&config_key)
            .and_then(|config| {
                if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
                {
                    let fields: Vec<_> = schema
                        .columns
                        .iter()
                        .filter_map(|col| {
                            if let nodedb_types::columnar::ColumnType::Vector(dim) = col.column_type
                            {
                                Some((col.name.clone(), dim))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if fields.is_empty() {
                        None
                    } else {
                        Some(fields)
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if !vector_fields.is_empty() {
            // Decode from MessagePack (internal format) — not JSON.
            if let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (field_name, dim) in &vector_fields {
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
                        let floats: Vec<f32> = arr
                            .iter()
                            .filter_map(|v| match v {
                                nodedb_types::Value::Float(f) => Some(*f as f32),
                                nodedb_types::Value::Integer(i) => Some(*i as f32),
                                _ => None,
                            })
                            .collect();
                        if floats.len() == *dim as usize {
                            let index_key = Self::vector_index_key(tid, collection, field_name);
                            let params = self
                                .vector_params
                                .get(&index_key)
                                .cloned()
                                .unwrap_or_default();
                            let coll =
                                self.vector_collections.entry(index_key).or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(*dim as usize, params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
                        }
                    }
                }
            }
        }

        // Schemaless vector indexing: if no strict schema but vector_params exist
        // for this collection, extract matching fields and index them.
        if vector_fields.is_empty() {
            // Named-field keys have the shape `(TenantId, "{collection}:{field}")`.
            // The bare (no-field) key is `(TenantId, "{collection}")`.
            let tid_key = crate::types::TenantId::new(tid);
            let field_prefix = format!("{collection}:");
            let bare_key = (tid_key, collection.to_string());

            // Collect all vector_params entries for this tenant+collection.
            // Each entry maps to a (params_map_key, field_name) pair.
            let mut schemaless_keys: Vec<((crate::types::TenantId, String), String)> = self
                .vector_params
                .keys()
                .filter(|(t, coll_key)| *t == bare_key.0 && coll_key.starts_with(&field_prefix))
                .map(|k| {
                    let field = k.1[field_prefix.len()..].to_string();
                    (k.clone(), field)
                })
                .collect();
            // Also check for bare key (no field name) — default to "embedding".
            if schemaless_keys.is_empty() && self.vector_params.contains_key(&bare_key) {
                schemaless_keys.push((bare_key.clone(), "embedding".to_string()));
            }

            if !schemaless_keys.is_empty()
                && let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (params_key, field_name) in &schemaless_keys {
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
                        let floats: Vec<f32> = arr
                            .iter()
                            .filter_map(|v| match v {
                                nodedb_types::Value::Float(f) => Some(*f as f32),
                                nodedb_types::Value::Integer(i) => Some(*i as f32),
                                _ => None,
                            })
                            .collect();
                        if !floats.is_empty() {
                            let params = self
                                .vector_params
                                .get(params_key)
                                .cloned()
                                .unwrap_or_default();
                            // Use field-qualified key so search can find it.
                            let store_key = Self::vector_index_key(tid, collection, field_name);
                            let coll =
                                self.vector_collections.entry(store_key).or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(floats.len(), params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
                        }
                    }
                }
            }
        }

        Ok(prior)
    }
}

/// Reject the write if any `unique: true` index already holds one of the
/// incoming document's extracted values under a *different* `document_id`.
///
/// Runs before `apply_secondary_indexes_in_txn` so the caller's write
/// transaction is still clean — rejection does not roll anything back.
/// Same-id re-puts (idempotent overwrites) are allowed through; we only
/// reject when another row owns the value.
fn check_unique_constraints(
    sparse: &crate::engine::sparse::btree::SparseEngine,
    tid: u32,
    collection: &str,
    doc: &serde_json::Value,
    document_id: &str,
    paths: &[crate::engine::document::store::IndexPath],
) -> crate::Result<()> {
    use crate::engine::document::store::extract_index_values;

    let doc_engine = crate::engine::document::store::DocumentEngine::new(sparse, tid);
    for path in paths {
        if !path.unique {
            continue;
        }
        // A partial UNIQUE index only applies to rows the predicate
        // accepts; rows outside the predicate's scope are not part of
        // the uniqueness domain. Skipping the check here mirrors the
        // skip in `apply_secondary_indexes_in_txn` so the two paths
        // agree on which rows the index governs.
        if let Some(ref p) = path.predicate
            && !p.evaluate_json(doc)
        {
            continue;
        }
        for raw in extract_index_values(doc, &path.path, path.is_array) {
            let needle = if path.case_insensitive {
                raw.to_lowercase()
            } else {
                raw
            };
            let existing = doc_engine
                .index_lookup(collection, &path.path, &needle)
                .unwrap_or_default();
            if existing.iter().any(|id| id != document_id) {
                return Err(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "unique index '{}' violation on field '{}' (value '{}')",
                        path.name, path.path, needle
                    ),
                });
            }
        }
    }
    Ok(())
}
