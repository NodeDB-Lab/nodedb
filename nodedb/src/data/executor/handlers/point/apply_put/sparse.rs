// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector inverted-index side-effects for `apply_point_put`: maintain
//! declared strict-schema `SparseVector` columns and drop a document's prior
//! sparse entries on delete/update. Mirrors `apply_put/vector.rs`, but sparse
//! vectors are strict-schema-only (there is no schemaless `sparse_params`
//! analog), carry no cross-engine surrogate (the string `doc_id` keys the
//! index directly), and the index `insert_at` ends the document's live
//! version itself — so this file has neither the schemaless arm nor the
//! per-field remove-before-insert dance the dense-vector path needs.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::document::store::StorageKey;

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
    /// `storage_key`'s rendered text.
    ///
    /// `storage_key` is the SAME id the delete path
    /// (`remove_document_sparse_indexes`) and the engine handler
    /// (`execute_sparse_insert` / `execute_sparse_search`) key on, so a search
    /// reads back exactly what this write wrote. The upsert ends the
    /// document's live version at `sys_from` and starts the new one there, so
    /// a read at an earlier cut still scores the old one. A column the body
    /// does not carry as a parseable literal ends the document's live version
    /// at `sys_from`, as a delete does: a replaced row never keeps the old
    /// literal searchable.
    ///
    /// No-op (byte-identical to a collection without sparse columns) when
    /// `strict_sparse_fields` is empty.
    ///
    /// Pushes a `SparseDoc` undo entry onto `undo` before each change. The
    /// entry holds the document's prior version in that index and the
    /// index's id counter, or marks the index as created by this write.
    pub(in crate::data::executor) fn apply_point_put_sparse_indexes(
        &mut self,
        put: SparseIndexPut<'_>,
        undo: &mut Vec<UndoEntry>,
    ) {
        let SparseIndexPut {
            database_id,
            tid,
            collection,
            storage_key,
            value,
            sys_from,
        } = put;
        let sparse_fields = self.strict_sparse_fields(database_id, tid, collection);
        if sparse_fields.is_empty() {
            return;
        }

        // Decode from MessagePack (internal format) — not JSON. Matches the
        // `value` `apply_point_put` feeds the dense-vector indexer. A body
        // that is no object carries no literal.
        let obj = match nodedb_types::value_from_msgpack(value) {
            Ok(nodedb_types::Value::Object(obj)) => obj,
            _ => std::collections::HashMap::new(),
        };

        // Rendered once here — the inverted index is keyed by text.
        let document_id = storage_key.to_string();
        for field in &sparse_fields {
            let literal = match obj.get(field) {
                Some(nodedb_types::Value::String(literal)) => {
                    nodedb_types::SparseVector::parse_literal(literal).ok()
                }
                _ => None,
            };
            let key = Self::sparse_index_key(database_id, tid, collection, field);
            let Some(sv) = literal else {
                self.end_document_sparse_field(&key, &document_id, sys_from, undo);
                continue;
            };
            let (prior, next_id) = match self.sparse_vector_indexes.get(&key) {
                Some(index) => (
                    index.doc_image(&document_id),
                    Some(index.next_internal_id()),
                ),
                None => (None, None),
            };
            undo.push(UndoEntry::SparseDoc {
                key,
                doc_id: document_id.clone(),
                prior,
                next_id,
            });
            self.get_or_create_sparse_index(database_id, tid, collection, field)
                .insert_at(&document_id, &sv, sys_from);
            // Sparse indexes are in-memory with no redb store behind them; the
            // checkpoint that persists them fires only on a dirty mark, exactly
            // as the standalone `execute_sparse_insert` handler flags it.
            self.checkpoint_coordinator.mark_dirty("vector", 1);
        }
    }

    /// End, at `ordinal`, the live version of every sparse-index entry a
    /// document produced. Shared by the PointDelete cascade, the bulk delete
    /// and truncate cascades, and the PointUpdate re-index. Mirrors
    /// `remove_document_vector_indexes`. A read at a cut below `ordinal`
    /// still scores the ended version. No-op when the collection declares no
    /// sparse columns.
    ///
    /// Pushes a `SparseDoc` undo entry onto `undo` for each version it ends,
    /// carrying the version's internal id, postings and start.
    pub(in crate::data::executor) fn remove_document_sparse_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
        ordinal: i64,
        undo: &mut Vec<UndoEntry>,
    ) {
        let sparse_fields = self.strict_sparse_fields(database_id, tid, collection);
        if sparse_fields.is_empty() {
            return;
        }
        // The sparse index keys postings by the rendered storage key.
        let row_key = storage_key.to_string();
        for field in &sparse_fields {
            let key = Self::sparse_index_key(database_id, tid, collection, field);
            self.end_document_sparse_field(&key, &row_key, ordinal, undo);
        }
    }

    /// End the live version of `row_key` in the sparse index `key` at
    /// `ordinal`, with its undo entry. A field with no index or no live
    /// version changes nothing.
    fn end_document_sparse_field(
        &mut self,
        key: &SparseIndexKey,
        row_key: &str,
        ordinal: i64,
        undo: &mut Vec<UndoEntry>,
    ) {
        let Some(index) = self.sparse_vector_indexes.get_mut(key) else {
            return;
        };
        let Some(prior) = index.doc_image(row_key) else {
            return;
        };
        let next_id = index.next_internal_id();
        index.delete_at(row_key, ordinal);
        undo.push(UndoEntry::SparseDoc {
            key: key.clone(),
            doc_id: row_key.to_string(),
            prior: Some(prior),
            next_id: Some(next_id),
        });
        self.checkpoint_coordinator.mark_dirty("vector", 1);
    }
}

/// The key of one sparse inverted index: database, tenant, collection, field.
type SparseIndexKey = (
    nodedb_types::DatabaseId,
    crate::types::TenantId,
    String,
    String,
);

/// Inputs to [`CoreLoop::apply_point_put_sparse_indexes`].
pub(in crate::data::executor) struct SparseIndexPut<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// The row's storage key. Its rendered text keys the index.
    pub storage_key: StorageKey,
    /// The row body as MessagePack.
    pub value: &'a [u8],
    /// The system-time ordinal of the row version the write stores.
    pub sys_from: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::engine::document::store::CollectionConfig;
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

    /// Put `body` as row `surrogate` of `collection` at `sys_from`.
    fn put(core: &mut CoreLoop, at: (u64, u64, &str), surrogate: u32, body: &[u8], sys_from: i64) {
        let (database_id, tid, collection) = at;
        core.apply_point_put_sparse_indexes(
            SparseIndexPut {
                database_id,
                tid,
                collection,
                storage_key: StorageKey::for_surrogate(Surrogate::new(surrogate)),
                value: body,
                sys_from,
            },
            &mut Vec::new(),
        );
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

        register_strict_sparse(core, tid, collection, field);

        let doc = doc_with_sparse(field, "{3:0.5, 7:1.5}");
        put(core, (db, tid, collection), 1, &doc, 10);

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

        register_strict_sparse(core, tid, collection, field);

        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{3:0.5, 7:1.5}"),
            10,
        );
        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{1:0.9}"),
            20,
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

        register_strict_sparse(core, tid, collection, field);

        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{3:0.5, 7:1.5}"),
            10,
        );
        assert_eq!(doc_count(core, db, tid, collection, field), 1);

        let mut undo = Vec::new();
        core.remove_document_sparse_indexes(
            db,
            tid,
            collection,
            StorageKey::for_surrogate(Surrogate::new(1)),
            20,
            &mut undo,
        );
        assert_eq!(
            doc_count(core, db, tid, collection, field),
            0,
            "remove must drop the document from the sparse field's index"
        );
        assert_eq!(undo.len(), 1, "the removal pushes one undo entry");
    }

    /// Reversing a removal's undo entries puts the document's postings back
    /// under the same internal id.
    #[test]
    fn undoing_a_remove_restores_the_sparse_entry() {
        let mut harness = make_core();
        let core = &mut harness.core;
        let (db, tid, collection, field) = (0u64, 1u64, "docs", "terms");
        register_strict_sparse(core, tid, collection, field);
        let row = StorageKey::for_surrogate(Surrogate::new(1));

        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{3:0.5, 7:1.5}"),
            10,
        );
        let key = CoreLoop::sparse_index_key(db, tid, collection, field);
        let before = core
            .sparse_vector_indexes
            .get(&key)
            .and_then(|index| index.doc_image(&row.to_string()))
            .expect("the put indexed the row");

        let mut undo = Vec::new();
        core.remove_document_sparse_indexes(db, tid, collection, row, 20, &mut undo);
        assert_eq!(doc_count(core, db, tid, collection, field), 0);

        core.undo_memory_effects(db, tid, undo)
            .expect("the removal reverses");
        let after = core
            .sparse_vector_indexes
            .get(&key)
            .and_then(|index| index.doc_image(&row.to_string()))
            .expect("the row is indexed again");
        assert_eq!(after, before, "same internal id and postings");
    }

    /// An overwrite and a delete keep the replaced versions for a read at an
    /// earlier cut, and a put without the column ends the live version.
    #[test]
    fn writes_stamp_their_ordinal_on_the_sparse_index() {
        let mut harness = make_core();
        let core = &mut harness.core;
        let (db, tid, collection, field) = (0u64, 1u64, "docs", "terms");
        register_strict_sparse(core, tid, collection, field);
        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{3:1.0}"),
            10,
        );
        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse(field, "{3:2.0}"),
            20,
        );
        put(
            core,
            (db, tid, collection),
            2,
            &doc_with_sparse(field, "{3:4.0}"),
            25,
        );
        put(
            core,
            (db, tid, collection),
            2,
            &doc_with_sparse("other", "{3:9.0}"),
            28,
        );
        core.remove_document_sparse_indexes(
            db,
            tid,
            collection,
            StorageKey::for_surrogate(Surrogate::new(1)),
            30,
            &mut Vec::new(),
        );
        let key = CoreLoop::sparse_index_key(db, tid, collection, field);
        let index = core.sparse_vector_indexes.get(&key).expect("index");
        let query = nodedb_types::SparseVector::from_entries(vec![(3, 1.0)]).expect("query");
        let scores = |at: i64| -> Vec<f32> {
            crate::engine::vector::sparse::search::dot_product_topk_at(index, &query, 10, at)
                .into_iter()
                .map(|r| r.score)
                .collect()
        };
        assert_eq!(scores(15), vec![1.0]);
        assert_eq!(scores(26), vec![4.0, 2.0]);
        assert_eq!(scores(29), vec![2.0], "the column-less put ended row 2");
        assert!(scores(30).is_empty(), "deleted at 30");
        assert!(index.is_empty());
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

        // No strict sparse schema registered.
        assert!(!core.collection_has_sparse(
            crate::types::DatabaseId::DEFAULT.as_u64(),
            tid,
            collection,
        ));

        put(
            core,
            (db, tid, collection),
            1,
            &doc_with_sparse("terms", "{3:0.5}"),
            10,
        );

        assert!(
            core.sparse_vector_indexes.is_empty(),
            "a collection without a SparseVector column must create no sparse index"
        );
    }
}
