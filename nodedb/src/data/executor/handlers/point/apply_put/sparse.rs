// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector inverted-index side-effects for `apply_point_put`: maintain
//! declared strict-schema `SparseVector` columns and drop a document's prior
//! sparse entries on delete/update. Mirrors `apply_put/vector.rs`, but sparse
//! vectors are strict-schema-only (there is no schemaless `sparse_params`
//! analog), carry no cross-engine surrogate (the string `doc_id` keys the
//! index directly), and the index `insert` is itself an upsert — so this file
//! has neither the schemaless arm nor the per-field remove-before-insert dance
//! the dense-vector path needs.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Strict-schema `SparseVector` column names declared on `collection`, or
    /// empty when the collection has no strict schema / no sparse columns.
    /// Shared by `apply_point_put_sparse_indexes` (which extracts + parses each
    /// field's literal) and `remove_document_sparse_indexes` (which drops each
    /// field's prior posting entries). Sparse vectors are dimensionless, so —
    /// unlike `strict_vector_fields` — only the field NAME is returned.
    pub(in crate::data::executor) fn strict_sparse_fields(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<String> {
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
                    let fields: Vec<String> = schema
                        .columns
                        .iter()
                        .filter(|col| {
                            matches!(
                                col.column_type,
                                nodedb_types::columnar::ColumnType::SparseVector
                            )
                        })
                        .map(|col| col.name.clone())
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

    /// Whether `collection` declares any strict-schema `SparseVector` column —
    /// the single gate callers check before paying for any sparse-index
    /// maintenance. Callers looping over many rows must call this ONCE before
    /// the loop and thread the resulting bool through, mirroring the
    /// `collection_has_vectors` contract.
    pub(in crate::data::executor) fn collection_has_sparse(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        !self
            .strict_sparse_fields(database_id, tid, collection)
            .is_empty()
    }

    /// Sparse inverted-index side-effect: for every declared `SparseVector`
    /// column, extract its string literal from the document body, parse it, and
    /// upsert it into the corresponding `SparseInvertedIndex` keyed by
    /// `document_id`.
    ///
    /// `document_id` is the hex-surrogate storage `row_key` — the SAME id the
    /// delete path (`remove_document_sparse_indexes`) and the engine handler
    /// (`execute_sparse_insert` / `execute_sparse_search`) key on, so a search
    /// reads back exactly what this write wrote. The index `insert` is an
    /// upsert (it removes the doc's prior entries first), so a second put for
    /// the same `document_id` replaces rather than duplicates. A missing field,
    /// a non-string value, or an unparseable literal is skipped — mirroring the
    /// dense-vector path's silent skip of malformed fields.
    ///
    /// No-op (byte-identical to a collection without sparse columns) when
    /// `strict_sparse_fields` is empty.
    pub(in crate::data::executor) fn apply_point_put_sparse_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) {
        let sparse_fields = self.strict_sparse_fields(database_id, tid, collection);
        if sparse_fields.is_empty() {
            return;
        }

        // Decode from MessagePack (internal format) — not JSON. Matches the
        // `value` `apply_point_put` feeds the dense-vector indexer.
        let Ok(nodedb_types::Value::Object(obj)) = nodedb_types::value_from_msgpack(value) else {
            return;
        };

        for field in &sparse_fields {
            let Some(nodedb_types::Value::String(literal)) = obj.get(field) else {
                continue;
            };
            let Ok(sv) = nodedb_types::SparseVector::parse_literal(literal) else {
                continue;
            };
            self.get_or_create_sparse_index(database_id, tid, collection, field)
                .insert(document_id, &sv);
            // Sparse indexes are in-memory with no redb store behind them; the
            // checkpoint that persists them fires only on a dirty mark, exactly
            // as the standalone `execute_sparse_insert` handler flags it.
            self.checkpoint_coordinator.mark_dirty("vector", 1);
        }
    }

    /// Drop every sparse-index posting entry a document produced, keyed by its
    /// hex-surrogate storage `row_key`. Shared by the PointDelete cascade
    /// (which orphans a removed row's sparse entries) and the PointUpdate
    /// re-index (which clears the old literal before inserting the new one).
    /// Mirrors `remove_document_vector_indexes`. No-op when the collection
    /// declares no sparse columns.
    pub(in crate::data::executor) fn remove_document_sparse_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        row_key: &str,
    ) {
        let sparse_fields = self.strict_sparse_fields(database_id, tid, collection);
        for field in &sparse_fields {
            if self
                .get_or_create_sparse_index(database_id, tid, collection, field)
                .delete(row_key)
            {
                self.checkpoint_coordinator.mark_dirty("vector", 1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::{Surrogate, Value};

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive `apply_point_put_sparse_indexes` directly and never tick the
    /// event loop, so the far ends are unused — they just must not be dropped.
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

    /// Seed a strict collection whose schema declares a `SparseVector` column
    /// named `field`, so `strict_sparse_fields` reports it.
    fn register_strict_sparse(core: &mut CoreLoop, tid: u64, collection: &str, field: &str) {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable(field, ColumnType::SparseVector),
        ])
        .expect("schema");
        let config =
            CollectionConfig::new(collection).with_storage_mode(StorageMode::Strict { schema });
        core.doc_configs.insert(
            (
                crate::types::DatabaseId::DEFAULT,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ),
            config,
        );
    }

    /// A document body carrying a sparse-vector string literal for `field`.
    fn doc_with_sparse(field: &str, literal: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(field.to_string(), Value::String(literal.into()));
        nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode doc")
    }

    fn doc_count(core: &CoreLoop, db: u64, tid: u64, collection: &str, field: &str) -> usize {
        let key = CoreLoop::sparse_index_key(db, tid, collection, field);
        core.sparse_vector_indexes
            .get(&key)
            .map(|idx| idx.doc_count())
            .unwrap_or(0)
    }

    /// A put of a strict document carrying a `SparseVector` field must upsert
    /// exactly one document into that field's sparse inverted index, under the
    /// hex-surrogate row key.
    #[test]
    fn put_indexes_sparse_field_once() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let field = "terms";
        let row_key = surrogate_to_doc_id(Surrogate::new(1));

        register_strict_sparse(core, tid, collection, field);

        let doc = doc_with_sparse(field, "{3:0.5, 7:1.5}");
        core.apply_point_put_sparse_indexes(db, tid, collection, &row_key, &doc);

        assert_eq!(
            doc_count(core, db, tid, collection, field),
            1,
            "the put must index exactly one document in the sparse field's index"
        );
    }

    /// A second put for the same row key must replace (upsert), not duplicate —
    /// the sparse index stays at one document.
    #[test]
    fn second_put_for_same_row_key_replaces_not_duplicates() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let field = "terms";
        let row_key = surrogate_to_doc_id(Surrogate::new(1));

        register_strict_sparse(core, tid, collection, field);

        core.apply_point_put_sparse_indexes(
            db,
            tid,
            collection,
            &row_key,
            &doc_with_sparse(field, "{3:0.5, 7:1.5}"),
        );
        core.apply_point_put_sparse_indexes(
            db,
            tid,
            collection,
            &row_key,
            &doc_with_sparse(field, "{1:0.9}"),
        );

        assert_eq!(
            doc_count(core, db, tid, collection, field),
            1,
            "a second put for the same row key must replace the prior entry, not append a duplicate"
        );
    }

    /// `remove_document_sparse_indexes` must drop the document's entry, taking
    /// the field's index back to zero documents.
    #[test]
    fn remove_drops_sparse_entry() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db = 0u64;
        let tid = 1u64;
        let collection = "docs";
        let field = "terms";
        let row_key = surrogate_to_doc_id(Surrogate::new(1));

        register_strict_sparse(core, tid, collection, field);

        core.apply_point_put_sparse_indexes(
            db,
            tid,
            collection,
            &row_key,
            &doc_with_sparse(field, "{3:0.5, 7:1.5}"),
        );
        assert_eq!(doc_count(core, db, tid, collection, field), 1);

        core.remove_document_sparse_indexes(db, tid, collection, &row_key);
        assert_eq!(
            doc_count(core, db, tid, collection, field),
            0,
            "remove must drop the document from the sparse field's index"
        );
    }

    /// A collection with no `SparseVector` column is untouched: no index is
    /// created and the maintenance call is a pure no-op.
    #[test]
    fn non_sparse_collection_is_unaffected() {
        let mut harness = make_core();
        let core = &mut harness.core;

        let db = 0u64;
        let tid = 1u64;
        let collection = "plain";
        let row_key = surrogate_to_doc_id(Surrogate::new(1));

        // No strict sparse schema registered.
        assert!(!core.collection_has_sparse(
            crate::types::DatabaseId::DEFAULT.as_u64(),
            tid,
            collection,
        ));

        core.apply_point_put_sparse_indexes(
            db,
            tid,
            collection,
            &row_key,
            &doc_with_sparse("terms", "{3:0.5}"),
        );

        assert!(
            core.sparse_vector_indexes.is_empty(),
            "a collection without a SparseVector column must create no sparse index"
        );
    }
}
