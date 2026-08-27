// SPDX-License-Identifier: BUSL-1.1

//! Index a document's vectors into their HNSW collections on a point put.

use crate::data::executor::core_loop::CoreLoop;

use super::types::{VectorFieldInsert, VectorIndexDelta, VectorIndexPutParams};

/// Extract a float vector from a decoded document value.
///
/// Accepts both native msgpack arrays (the forward write path) and JSON
/// strings (schemaless bodies transcoded from a columnar TEXT/JSON column,
/// e.g. `"[0.1, 0.2, ...]"` or a plain comma/whitespace-separated list) so
/// the durable-store rebuild path indexes the same vectors a live PUT would.
fn floats_from_value(v: &nodedb_types::Value) -> Option<Vec<f32>> {
    match v {
        nodedb_types::Value::Array(arr) => {
            let floats: Vec<f32> = arr
                .iter()
                .filter_map(|v| match v {
                    nodedb_types::Value::Float(f) => Some(*f as f32),
                    nodedb_types::Value::Integer(i) => Some(*i as f32),
                    nodedb_types::Value::Decimal(d) => {
                        use rust_decimal::prelude::ToPrimitive;
                        d.to_f32()
                    }
                    nodedb_types::Value::String(s) => s.parse::<f32>().ok(),
                    _ => None,
                })
                .collect();
            if floats.is_empty() {
                None
            } else {
                Some(floats)
            }
        }
        nodedb_types::Value::String(s) => {
            // Try JSON array first, then comma/space-separated numbers.
            if let Ok(vals) = serde_json::from_str::<Vec<f64>>(s) {
                return Some(vals.into_iter().map(|f| f as f32).collect());
            }
            let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
            let mut floats: Vec<f32> = Vec::new();
            for tok in trimmed.split([',', ' ', '\t', '\n']) {
                let tok = tok.trim();
                if tok.is_empty() {
                    continue;
                }
                floats.push(tok.parse::<f32>().ok()?);
            }
            if floats.is_empty() {
                None
            } else {
                Some(floats)
            }
        }
        _ => None,
    }
}

impl CoreLoop {
    /// HNSW vector indexing side-effect: index declared strict-schema
    /// `Vector(dim)` columns, or (schemaless) fields matched by registered
    /// `vector_params`, into the corresponding `VectorCollection`.
    ///
    /// Returns the `(index_key, vector_id)` pairs inserted so a transactional
    /// caller can push `UndoEntry::InsertVector` reversals. Each inserted
    /// vector is also recorded in `vector_doc_map` keyed by the hex surrogate
    /// row key, so `apply_point_delete` can soft-delete it when the owning
    /// document is removed (closing the vector-orphan leak).
    /// `wal_lsn` is the WAL LSN of the document write driving this indexing
    /// (`0` when unassigned). It advances each touched collection's checkpoint
    /// watermark so a later vector checkpoint records that this document's
    /// embedding is already indexed; on WAL replay the same value gates a
    /// straddling-segment record — a field whose collection already absorbed
    /// this LSN is skipped rather than re-appended as a duplicate HNSW node.
    ///
    /// Fails when a vector's width disagrees with the index it would land in —
    /// either the width declared by `CREATE VECTOR INDEX ... DIM <n>` or the
    /// width an already-materialized index carries. The write is refused
    /// rather than the field skipped: a document that silently loses its
    /// embedding is indistinguishable, at query time, from one that was never
    /// similar to anything.
    pub(in crate::data::executor) fn apply_point_put_vector_indexes(
        &mut self,
        params: VectorIndexPutParams<'_>,
    ) -> crate::Result<Vec<VectorIndexDelta>> {
        let VectorIndexPutParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            wal_lsn,
        } = params;
        let mut inserts: Vec<VectorIndexDelta> = Vec::new();

        // Vector index: if the strict schema declares Vector(dim) columns,
        // extract float arrays and insert into HNSW so KNN search works.
        let vector_fields = self.strict_vector_fields(database_id, tid, collection);

        if !vector_fields.is_empty() {
            // Decode from MessagePack (internal format) — not JSON.
            if let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (field_name, dim) in &vector_fields {
                    if let Some(v) = obj.get(field_name)
                        && let Some(floats) = floats_from_value(v)
                    {
                        let index_key =
                            Self::vector_index_key(database_id, tid, collection, field_name);
                        self.check_vector_width(&index_key, field_name, floats.len())?;
                        if floats.len() != *dim as usize {
                            return Err(crate::Error::RejectedConstraint {
                                collection: collection.to_string(),
                                constraint: format!("vector dimension on '{field_name}'"),
                                detail: format!("column declares {dim}, got {}", floats.len()),
                            });
                        }
                        {
                            let params = self
                                .vector_params
                                .get(&index_key)
                                .cloned()
                                .unwrap_or_default();
                            let skip = {
                                let coll = self
                                    .vector_collections
                                    .entry(index_key.clone())
                                    .or_insert_with(|| {
                                        nodedb_vector::VectorCollection::new(*dim as usize, params)
                                    });
                                // Skip a straddling-segment record the restored
                                // checkpoint already absorbed (replay only; a
                                // live write always carries a higher, unseen
                                // LSN).
                                wal_lsn != 0 && wal_lsn <= coll.checkpoint_wal_lsn()
                            };
                            if skip {
                                continue;
                            }
                            if let Some(delta) =
                                self.remove_then_insert_vector_field(VectorFieldInsert {
                                    database_id,
                                    tid,
                                    index_key,
                                    collection,
                                    field_name,
                                    document_id,
                                    floats,
                                    surrogate,
                                    wal_lsn,
                                })
                            {
                                inserts.push(delta);
                            }
                        }
                    }
                }
            }
        }

        // Schemaless vector indexing: if no strict schema but vector_params exist
        // for this collection, extract matching fields and index them.
        if vector_fields.is_empty() {
            // Named-field keys have the shape `(DatabaseId, TenantId, "{collection}:{field}")`.
            // The bare (no-field) key is `(DatabaseId, TenantId, "{collection}")`.
            let db_key = nodedb_types::DatabaseId::new(database_id);
            let tid_key = crate::types::TenantId::new(tid);
            let field_prefix = format!("{collection}:");
            let bare_key = (db_key, tid_key, collection.to_string());
            let field_names = self.schemaless_vector_field_names(database_id, tid, collection);

            // Each field name maps back to its `vector_params` map key: either
            // the field-qualified key (if one was registered) or the bare key
            // (single default-"embedding" field, no per-field registration).
            let schemaless_keys: Vec<(
                (nodedb_types::DatabaseId, crate::types::TenantId, String),
                String,
            )> = field_names
                .into_iter()
                .map(|field| {
                    let qualified = (db_key, tid_key, format!("{field_prefix}{field}"));
                    let params_key = if self.vector_params.contains_key(&qualified) {
                        qualified
                    } else {
                        bare_key.clone()
                    };
                    (params_key, field)
                })
                .collect();

            if !schemaless_keys.is_empty()
                && let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (params_key, field_name) in &schemaless_keys {
                    if let Some(v) = obj.get(field_name)
                        && let Some(floats) = floats_from_value(v)
                        && !floats.is_empty()
                    {
                            let params = self
                                .vector_params
                                .get(params_key)
                                .cloned()
                                .unwrap_or_default();
                            // Use field-qualified key so search can find it.
                            let store_key =
                                Self::vector_index_key(database_id, tid, collection, field_name);
                            self.check_vector_width(&store_key, field_name, floats.len())?;
                            let dim = floats.len();
                            let skip = {
                                let coll = self
                                    .vector_collections
                                    .entry(store_key.clone())
                                    .or_insert_with(|| {
                                        nodedb_vector::VectorCollection::new(dim, params)
                                    });
                                // Skip a straddling-segment record the restored
                                // checkpoint already absorbed (replay only; a
                                // live write always carries a higher, unseen
                                // LSN).
                                wal_lsn != 0 && wal_lsn <= coll.checkpoint_wal_lsn()
                            };
                            if skip {
                                continue;
                            }
                            if let Some(delta) =
                                self.remove_then_insert_vector_field(VectorFieldInsert {
                                    database_id,
                                    tid,
                                    index_key: store_key,
                                    collection,
                                    field_name,
                                    document_id,
                                    floats,
                                    surrogate,
                                    wal_lsn,
                                })
                            {
                                inserts.push(delta);
                            }
                        }
                }
            }
        }

        Ok(inserts)
    }

    /// Reject a vector whose width disagrees with the index it targets.
    ///
    /// Checks the width declared at `CREATE VECTOR INDEX ... DIM <n>` before
    /// the index has materialized, then the width the materialized index
    /// actually carries. Both matter: the first write would otherwise define
    /// the width and silently supersede the declaration.
    fn check_vector_width(
        &self,
        index_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        field_name: &str,
        got: usize,
    ) -> crate::Result<()> {
        let mismatch = |expected: usize, source: &str| crate::Error::RejectedConstraint {
            collection: index_key.2.clone(),
            constraint: format!("vector dimension on '{field_name}'"),
            detail: format!("index {source} {expected}, got {got}"),
        };

        if let Some(&declared) = self.declared_dims.get(index_key)
            && declared != 0
            && declared != got
        {
            return Err(mismatch(declared, "declares"));
        }
        if let Some(existing) = self.vector_collections.get(index_key)
            && existing.dim() != got
        {
            return Err(mismatch(existing.dim(), "has"));
        }
        Ok(())
    }

    /// Shared tail of `apply_point_put_vector_indexes`'s strict and
    /// schemaless arms, once each has resolved its own `index_key` and
    /// extracted `floats` for `field_name`. Removes this field's prior node
    /// for the surrogate before inserting the new one — `insert_with_surrogate`
    /// appends a fresh node rather than replacing, so a second put for the
    /// same surrogate (a live overwrite, or a replayed duplicate) would
    /// otherwise leave the stale embedding searchable alongside the new one.
    /// Per-field (not whole-doc) so a sibling vector field's just-inserted
    /// node is never clobbered. The remove is idempotent — a no-op on a
    /// genuine first insert.
    ///
    /// Binds the vector node to the document's global surrogate so
    /// cross-engine identity holds: a search hit resolves back to this row's
    /// surrogate (and thus its user PK at the response boundary) instead of
    /// leaking a headless local node id. Returns `None` if `index_key`'s
    /// `VectorCollection` was somehow absent (defensive — it was just
    /// populated via `entry().or_insert_with()` by the caller).
    fn remove_then_insert_vector_field(
        &mut self,
        params: VectorFieldInsert<'_>,
    ) -> Option<VectorIndexDelta> {
        let VectorFieldInsert {
            database_id,
            tid,
            index_key,
            collection,
            field_name,
            document_id,
            floats,
            surrogate,
            wal_lsn,
        } = params;
        let _ = self.remove_document_vector_index_field(
            database_id,
            tid,
            collection,
            field_name,
            document_id,
        );
        let coll = self.vector_collections.get_mut(&index_key)?;
        let vector_id = coll.insert_with_surrogate(floats, surrogate);
        coll.note_checkpoint_lsn(wal_lsn);
        self.vector_doc_map.insert(
            (
                index_key.0,
                index_key.1,
                collection.to_string(),
                field_name.to_string(),
                document_id.to_string(),
            ),
            vector_id,
        );
        Some(VectorIndexDelta {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field_name.to_string(),
            doc_id: document_id.to_string(),
        })
    }
}
