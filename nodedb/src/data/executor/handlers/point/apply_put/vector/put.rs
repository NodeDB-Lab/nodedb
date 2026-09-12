// SPDX-License-Identifier: BUSL-1.1

//! Index a document's vectors into their HNSW collections on a point put.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::vector_string::floats_from_value;

use super::types::{VectorFieldInsert, VectorIndexDelta, VectorIndexPutParams};

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
            storage_key,
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
                    let floats = match obj.get(field_name) {
                        Some(v) => match floats_from_value(collection, field_name, v)? {
                            Some(f) => f,
                            None => continue,
                        },
                        None => continue,
                    };
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
                    if let Some(delta) = self.remove_then_insert_vector_field(VectorFieldInsert {
                        database_id,
                        tid,
                        index_key,
                        collection,
                        field_name,
                        storage_key,
                        floats,
                        wal_lsn,
                    }) {
                        inserts.push(delta);
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
                    let floats = match obj.get(field_name) {
                        Some(v) => match floats_from_value(collection, field_name, v)? {
                            Some(f) => f,
                            None => continue,
                        },
                        None => continue,
                    };
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
                            .or_insert_with(|| nodedb_vector::VectorCollection::new(dim, params));
                        // Skip a straddling-segment record the restored
                        // checkpoint already absorbed (replay only; a
                        // live write always carries a higher, unseen
                        // LSN).
                        wal_lsn != 0 && wal_lsn <= coll.checkpoint_wal_lsn()
                    };
                    if skip {
                        continue;
                    }
                    if let Some(delta) = self.remove_then_insert_vector_field(VectorFieldInsert {
                        database_id,
                        tid,
                        index_key: store_key,
                        collection,
                        field_name,
                        storage_key,
                        floats,
                        wal_lsn,
                    }) {
                        inserts.push(delta);
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
            storage_key,
            floats,
            wal_lsn,
        } = params;
        let _ = self.remove_document_vector_index_field(
            database_id,
            tid,
            collection,
            field_name,
            storage_key,
        );
        let coll = self.vector_collections.get_mut(&index_key)?;
        let vector_id = coll.insert_with_surrogate(floats, storage_key.surrogate());
        coll.note_checkpoint_lsn(wal_lsn);
        self.vector_doc_map.insert(
            (
                index_key.0,
                index_key.1,
                collection.to_string(),
                field_name.to_string(),
                storage_key,
            ),
            vector_id,
        );
        Some(VectorIndexDelta {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field_name.to_string(),
            doc_id: storage_key,
        })
    }
}

/// Vector side-effect tests for the point-put path.
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
            crate::data::executor::core_loop::test_governor(),
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
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);

        register_bare_field(core, db_id, tid, collection);

        let first = doc_with_vectors(&[("embedding", &[1.0, 0.0, 0.0])]);
        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            storage_key,
            value: &first,
            wal_lsn: 0,
        })
        .expect("vector indexing must accept this fixture");

        let second = doc_with_vectors(&[("embedding", &[0.0, 1.0, 0.0])]);
        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            storage_key,
            value: &second,
            wal_lsn: 0,
        })
        .expect("vector indexing must accept this fixture");

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
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);

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
            storage_key,
            value: &doc,
            wal_lsn: 0,
        })
        .expect("vector indexing must accept this fixture");

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

    /// A schemaless vector arriving as an SQL string literal must be parsed
    /// and indexed like an `ARRAY[...]` literal.
    ///
    /// `INSERT ... VALUES ('id', '[0.1, 0.2, ...]')` carries the vector as a
    /// text literal — the planner's `SqlValue::String` — which decodes to
    /// `Value::String` in the document body. The put path parses it through
    /// the same grammar as the strict UPDATE coerce path, so the document is
    /// indexed and the durable-store rebuild finds it.
    #[test]
    fn json_string_embedding_is_indexed_not_silently_dropped() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db_id = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let surrogate = Surrogate::new(1);
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);

        register_bare_field(core, db_id, tid, collection);

        // Embedding arrives as a JSON-array string literal, the shape the
        // planner's SqlValue::String produces.
        let body =
            nodedb_types::value_to_msgpack(&Value::Object(std::collections::HashMap::from([(
                "embedding".to_string(),
                Value::String("[0.1, 0.2, 0.3]".to_string()),
            )])))
            .expect("encode doc");

        core.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: db_id,
            tid,
            collection,
            storage_key,
            value: &body,
            wal_lsn: 0,
        })
        .expect("vector indexing must accept a JSON-string embedding");

        assert_eq!(
            live_count(core, db_id, tid, collection, "embedding"),
            1,
            "a JSON-string embedding must be indexed, not silently dropped"
        );
    }

    /// A malformed JSON-string embedding must reject the put — a document
    /// that silently loses its embedding is indistinguishable from one that
    /// never had one.
    #[test]
    fn malformed_json_string_embedding_rejects_the_put() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db_id = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let surrogate = Surrogate::new(1);
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);

        register_bare_field(core, db_id, tid, collection);

        for bad in ["not-json", "7", "[0.1, \"bad\"]", "{}", "[nan]"] {
            let body =
                nodedb_types::value_to_msgpack(&Value::Object(std::collections::HashMap::from([
                    ("embedding".to_string(), Value::String(bad.to_string())),
                ])))
                .expect("encode doc");

            let res = core.apply_point_put_vector_indexes(VectorIndexPutParams {
                database_id: db_id,
                tid,
                collection,
                storage_key,
                value: &body,
                wal_lsn: 0,
            });

            assert!(
                matches!(res, Err(crate::Error::RejectedConstraint { .. })),
                "malformed embedding '{bad}' must reject the put"
            );
            assert_eq!(
                live_count(core, db_id, tid, collection, "embedding"),
                0,
                "malformed embedding '{bad}' must not be indexed"
            );
        }
    }
}
