// SPDX-License-Identifier: BUSL-1.1

//! HNSW vector-index side-effects for `apply_point_put`: index declared
//! strict-schema `Vector(dim)` columns and schemaless `vector_params` fields,
//! and soft-delete a document's prior vector nodes (per field, or whole-doc).
//! Split out of `index.rs` to keep that file focused on spatial side-effects.

use crate::data::executor::core_loop::CoreLoop;

/// Inputs to `apply_point_put_vector_indexes` for one document write.
///
/// `wal_lsn` is the WAL LSN of the document write driving this indexing (`0`
/// when unassigned); it advances each touched collection's checkpoint watermark
/// and, on replay, gates a record the collection's checkpoint already absorbed.
pub(in crate::data::executor) struct VectorIndexPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub value: &'a [u8],
    pub wal_lsn: u64,
}

/// Capture of a single HNSW vector index mutation (insert or soft-delete),
/// carrying everything needed to both key the `VectorCollection` (`index_key`,
/// `vector_id`) AND reverse the paired `vector_doc_map` entry on rollback
/// (`collection`, `field`, `doc_id`). Replaces a raw `(index_key, vector_id)`
/// tuple so undo can restore/remove the reverse-lookup map symmetrically with
/// the R-tree's `SpatialInsert`/`SpatialDelete` undo pattern.
pub(in crate::data::executor) struct VectorIndexDelta {
    pub index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub vector_id: u32,
    pub collection: String,
    pub field: String,
    pub doc_id: String,
}

/// Inputs to `remove_then_insert_vector_field`, the shared per-field
/// remove-before-insert tail of `apply_point_put_vector_indexes`'s strict and
/// schemaless arms, once each has resolved its own `index_key` and extracted
/// `floats` for `field_name`.
struct VectorFieldInsert<'a> {
    database_id: u64,
    tid: u64,
    index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    collection: &'a str,
    field_name: &'a str,
    document_id: &'a str,
    floats: Vec<f32>,
    surrogate: nodedb_types::Surrogate,
    wal_lsn: u64,
}

impl CoreLoop {
    /// Strict-schema `Vector(dim)` column names + dims declared on
    /// `collection`, or empty if the collection has no strict schema / no
    /// vector columns. Shared by `apply_point_put_vector_indexes` (which
    /// needs `dim` to validate extracted float arrays) and
    /// `apply_point_delete`'s vector cleanup (which only needs the field
    /// names to construct exact `vector_doc_map` keys without a full-map
    /// scan).
    pub(in crate::data::executor) fn strict_vector_fields(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<(String, u32)> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs
            .get(&config_key)
            .and_then(|config| {
                if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
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
            .unwrap_or_default()
    }

    /// Schemaless vector field names registered via `vector_params` for
    /// `collection` (named-field entries `"{collection}:{field}"`, plus the
    /// bare `"{collection}"` key defaulting to `"embedding"`). Shared by the
    /// put path's schemaless indexing branch and the delete cleanup's exact
    /// key construction.
    pub(in crate::data::executor) fn schemaless_vector_field_names(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<String> {
        let db_key = nodedb_types::DatabaseId::new(database_id);
        let tid_key = crate::types::TenantId::new(tid);
        let field_prefix = format!("{collection}:");
        let bare_key = (db_key, tid_key, collection.to_string());

        let mut names: Vec<String> = self
            .vector_params
            .keys()
            .filter(|(d, t, coll_key)| {
                *d == bare_key.0 && *t == bare_key.1 && coll_key.starts_with(&field_prefix)
            })
            .map(|k| k.2[field_prefix.len()..].to_string())
            .collect();
        if names.is_empty() && self.vector_params.contains_key(&bare_key) {
            names.push("embedding".to_string());
        }
        names
    }

    /// Whether `collection` has any vector fields — strict-schema `Vector(dim)`
    /// columns OR schemaless fields registered via `vector_params`. Combines
    /// `strict_vector_fields` + `schemaless_vector_field_names` into the single
    /// gate check callers need before deciding whether to pay for HNSW
    /// maintenance at all. Callers that loop over many rows (bulk update/
    /// delete, merge, update-from-join) must call this ONCE before the loop
    /// and thread the resulting bool through, rather than recomputing it per
    /// row — the schemaless half is an unindexed scan of `vector_params`.
    pub(in crate::data::executor) fn collection_has_vectors(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        !self
            .strict_vector_fields(database_id, tid, collection)
            .is_empty()
            || !self
                .schemaless_vector_field_names(database_id, tid, collection)
                .is_empty()
    }

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
    pub(in crate::data::executor) fn apply_point_put_vector_indexes(
        &mut self,
        params: VectorIndexPutParams<'_>,
    ) -> Vec<VectorIndexDelta> {
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
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
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
                        if floats.len() == *dim as usize {
                            let index_key =
                                Self::vector_index_key(database_id, tid, collection, field_name);
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
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
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
                        if !floats.is_empty() {
                            let params = self
                                .vector_params
                                .get(params_key)
                                .cloned()
                                .unwrap_or_default();
                            // Use field-qualified key so search can find it.
                            let store_key =
                                Self::vector_index_key(database_id, tid, collection, field_name);
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
        }

        inserts
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

    /// Soft-delete the single HNSW vector node a document produced for one
    /// `field`, keyed by its hex-surrogate storage `row_key`, and drop the
    /// paired `vector_doc_map` reverse entry. Returns the removed delta, or
    /// `None` when the `(db, tid, collection, field, row_key)` key had no prior
    /// node (a genuine first insert). This is the per-field unit the whole-doc
    /// `remove_document_vector_indexes` loops over, and the put path calls it
    /// for the current field only so a sibling field's just-inserted node is
    /// never clobbered.
    pub(in crate::data::executor) fn remove_document_vector_index_field(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        row_key: &str,
    ) -> Option<VectorIndexDelta> {
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let doc_key = (
            db_id,
            tid_id,
            collection.to_string(),
            field.to_string(),
            row_key.to_string(),
        );
        let vector_id = self.vector_doc_map.remove(&doc_key)?;
        let index_key = Self::vector_index_key(database_id, tid, collection, field);
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.delete(vector_id);
        }
        Some(VectorIndexDelta {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: row_key.to_string(),
        })
    }

    /// Soft-delete every HNSW vector entry a document produced, keyed by its
    /// hex-surrogate storage `row_key`, and drop the paired `vector_doc_map`
    /// reverse entries. Shared by the PointDelete cascade (which orphans the
    /// vectors of a removed row) and the PointUpdate re-index (which must clear
    /// the surrogate's old embedding before inserting the new one, since
    /// `insert_with_surrogate` appends rather than replaces).
    ///
    /// Candidate fields come from the same strict-schema / `vector_params`
    /// enumeration the put path uses, so each `vector_doc_map` entry is looked
    /// up by its exact key (via `remove_document_vector_index_field`) instead
    /// of scanning the whole map. Returns the removed `(index_key, vector_id)`
    /// deltas so a transactional caller can push `UndoEntry::DeleteVector`
    /// reversals.
    pub(in crate::data::executor) fn remove_document_vector_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        row_key: &str,
    ) -> Vec<VectorIndexDelta> {
        let strict_fields = self.strict_vector_fields(database_id, tid, collection);
        let candidate_fields: Vec<String> = if !strict_fields.is_empty() {
            strict_fields.into_iter().map(|(name, _dim)| name).collect()
        } else {
            self.schemaless_vector_field_names(database_id, tid, collection)
        };
        let mut vector_deletes = Vec::with_capacity(candidate_fields.len());
        for field in candidate_fields {
            if let Some(delta) = self.remove_document_vector_index_field(
                database_id,
                tid,
                collection,
                &field,
                row_key,
            ) {
                vector_deletes.push(delta);
            }
        }
        vector_deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_types::{Surrogate, Value};

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive `apply_point_put_vector_indexes` directly and never
    /// tick the event loop, so the far ends are unused — they just must not
    /// be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: Producer<BridgeRequest>,
        _resp_rx: Consumer<BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Register a bare (default-"embedding") schemaless vector field so the put
    /// path's schemaless indexing branch fires for it.
    fn register_bare_field(core: &mut CoreLoop, db_id: u64, tid: u64, collection: &str) {
        core.vector_params.insert(
            (
                nodedb_types::DatabaseId::new(db_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ),
            crate::engine::vector::hnsw::HnswParams::default(),
        );
    }

    /// Register a named schemaless vector field (`{collection}:{field}`).
    fn register_named_field(
        core: &mut CoreLoop,
        db_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
    ) {
        core.vector_params.insert(
            (
                nodedb_types::DatabaseId::new(db_id),
                crate::types::TenantId::new(tid),
                format!("{collection}:{field}"),
            ),
            crate::engine::vector::hnsw::HnswParams::default(),
        );
    }

    /// A schemaless document body carrying the named vector fields.
    fn doc_with_vectors(fields: &[(&str, &[f32])]) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        for (name, vector) in fields {
            obj.insert(
                (*name).to_string(),
                Value::Array(vector.iter().map(|f| Value::Float(*f as f64)).collect()),
            );
        }
        nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode doc")
    }

    fn live_count(core: &CoreLoop, db_id: u64, tid: u64, collection: &str, field: &str) -> usize {
        let key = CoreLoop::vector_index_key(db_id, tid, collection, field);
        core.vector_collections
            .get(&key)
            .map(|c| c.live_count())
            .unwrap_or(0)
    }

    fn physical_len(core: &CoreLoop, db_id: u64, tid: u64, collection: &str, field: &str) -> usize {
        let key = CoreLoop::vector_index_key(db_id, tid, collection, field);
        core.vector_collections
            .get(&key)
            .map(|c| c.len())
            .unwrap_or(0)
    }

    /// Regression for the latent HNSW duplicate-node bug: a second `PointPut`
    /// for the same surrogate — a live overwrite, or a replayed duplicate WAL
    /// record — must replace the surrogate's prior vector node rather than
    /// append a second one that keeps scoring in KNN forever.
    #[test]
    fn second_put_for_same_surrogate_replaces_not_duplicates_vector_node() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db_id = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let surrogate = Surrogate::new(1);
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);

        register_bare_field(core, db_id, tid, collection);

        let first = doc_with_vectors(&[("embedding", &[1.0, 0.0, 0.0])]);
        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            document_id: &row_key,
            surrogate,
            value: &first,
            wal_lsn: 0,
        });

        let second = doc_with_vectors(&[("embedding", &[0.0, 1.0, 0.0])]);
        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            document_id: &row_key,
            surrogate,
            value: &second,
            wal_lsn: 0,
        });

        assert_eq!(
            physical_len(core, db_id, tid, collection, "embedding"),
            2,
            "both puts must have physically indexed (guards against a silent no-op false pass)"
        );
        assert_eq!(
            live_count(core, db_id, tid, collection, "embedding"),
            1,
            "second put for the same surrogate must replace the prior node, not append a duplicate"
        );
    }

    /// Regression for the multi-vector-field case: a single put of a document
    /// carrying TWO vector fields must leave exactly one live node in EACH
    /// field's index. A whole-doc remove-before-insert inside the per-field
    /// loop would delete the first field's just-inserted node while processing
    /// the second, wiping every field but the last — breaking MetaEmbed /
    /// ColBERT multi-vector collections on every put.
    #[test]
    fn single_put_with_two_vector_fields_keeps_one_live_node_each() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db_id = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let surrogate = Surrogate::new(1);
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);

        register_named_field(core, db_id, tid, collection, "embedding");
        register_named_field(core, db_id, tid, collection, "title_vec");

        let doc = doc_with_vectors(&[
            ("embedding", &[1.0, 0.0, 0.0]),
            ("title_vec", &[0.0, 1.0, 0.0, 0.0]),
        ]);
        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            document_id: &row_key,
            surrogate,
            value: &doc,
            wal_lsn: 0,
        });

        assert_eq!(
            live_count(core, db_id, tid, collection, "embedding"),
            1,
            "the `embedding` field must keep its live node — not be wiped by the sibling field's put"
        );
        assert_eq!(
            live_count(core, db_id, tid, collection, "title_vec"),
            1,
            "the `title_vec` field must have exactly one live node"
        );
    }
}
