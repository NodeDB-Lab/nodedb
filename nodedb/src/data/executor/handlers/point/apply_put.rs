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
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        // Evaluate generated columns before encoding.
        let config_key = format!("{tid}:{collection}");
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

        // Check if this collection uses strict (Binary Tuple) encoding.
        let stored = if let Some(config) = self.doc_configs.get(&config_key)
            && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            super::super::super::strict_format::bytes_to_binary_tuple(value, schema).map_err(
                |e| crate::Error::Serialization {
                    format: "binary_tuple".into(),
                    detail: e,
                },
            )?
        } else {
            value.to_vec()
        };

        self.sparse
            .put_in_txn(txn, tid, collection, document_id, &stored)?;

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
            && let Some(doc) = super::super::super::doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            self.apply_secondary_indexes(tid, collection, &doc, document_id, &paths);
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
                    let index_key = format!("{tid}:{collection}:{field_name}");
                    let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                    let rtree = self.spatial_indexes.entry(index_key.clone()).or_default();
                    rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
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
                            coll.insert_with_doc_id(floats, document_id.to_string());
                        }
                    }
                }
            }
        }

        // Schemaless vector indexing: if no strict schema but vector_params exist
        // for this collection, extract matching fields and index them.
        if vector_fields.is_empty() {
            let prefix = format!("{tid}:{collection}:");
            let bare_key = format!("{tid}:{collection}");
            let mut schemaless_keys: Vec<(String, String)> = self
                .vector_params
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .map(|k| {
                    let field = k[prefix.len()..].to_string();
                    (k.clone(), field)
                })
                .collect();
            // Also check for bare key (no field name) — default to "embedding".
            if schemaless_keys.is_empty() && self.vector_params.contains_key(&bare_key) {
                schemaless_keys.push((bare_key, "embedding".to_string()));
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
                            coll.insert_with_doc_id(floats, document_id.to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
