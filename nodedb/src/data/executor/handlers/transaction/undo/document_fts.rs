// SPDX-License-Identifier: BUSL-1.1

//! Full-text re-indexing for a rolled-back document DELETE.
//!
//! The forward delete cascade removes a document's inverted-index postings
//! unconditionally (both plain and bitemporal collections). A transactional
//! rollback restores the document body into the primary store, so it must also
//! recompute and re-insert the FTS postings — otherwise the row comes back
//! restored-but-unsearchable. `nodedb_fts::analyze` is deterministic, so the
//! recomputed text (extracted via the same [`extract_fts_fields`] helper the
//! forward PUT path uses) reproduces byte-identical postings in every index.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_fields;

use super::UndoError;
use super::document::UndoDocumentContext;

impl CoreLoop {
    /// Restore a deleted document's text during DELETE rollback: the text
    /// version the delete ended at `delete_ordinal` is live again. A delete
    /// that kept no history re-indexes the restored body instead
    /// ([`Self::reindex_restored_document_fts`]).
    pub(super) fn restore_deleted_document_fts(
        &self,
        ctx: UndoDocumentContext<'_>,
        surrogate: nodedb_types::Surrogate,
        old_value: &[u8],
        delete_ordinal: i64,
    ) -> Result<(), UndoError> {
        let restored = self
            .inverted
            .undo_text_version(
                ctx.database_id,
                crate::types::TenantId::new(ctx.tid),
                ctx.collection,
                surrogate,
                delete_ordinal,
            )
            .map_err(|e| {
                let err = UndoError::failed(
                    ctx.entry_index,
                    format!(
                        "fts version restore on {}/{}",
                        ctx.collection, ctx.document_id
                    ),
                    e,
                );
                error!(
                    core = self.core_id,
                    entry_index = ctx.entry_index,
                    collection = %ctx.collection,
                    error = %err,
                    "transaction undo: FTS version restore failed; shard state unknown"
                );
                err
            })?;
        if restored {
            return Ok(());
        }
        self.reindex_restored_document_fts(ctx, surrogate, old_value)
    }

    /// Re-index a restored document's text into the inverted index during
    /// DELETE rollback. Decodes the restored body through the storage-mode-aware
    /// helper (strict → Binary Tuple, schemaless → MessagePack) so both modes
    /// recompute their real text. Returns an [`UndoError`] on failure, so a
    /// partial FTS restore escalates to `RollbackFailed`.
    pub(super) fn reindex_restored_document_fts(
        &self,
        ctx: UndoDocumentContext<'_>,
        surrogate: nodedb_types::Surrogate,
        old_value: &[u8],
    ) -> Result<(), UndoError> {
        let UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection,
            document_id,
        } = ctx;
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            // No config → cannot decode a strict tuple and no index paths to
            // reconstruct; the forward cascade could not have indexed text it
            // could not decode either, so there is nothing to restore.
            return Ok(());
        };
        // A rollback that cannot read the body it is restoring cannot rebuild
        // the row's FTS postings, so the restored row would be permanently
        // unsearchable. That is a failed rollback, not a no-op.
        let doc = self
            .decode_stored_document(config, old_value)
            .map_err(|e| {
                UndoError::failed(
                    entry_index,
                    format!("decoding the restored body of {collection}/{document_id}"),
                    e,
                )
            })?;
        let text = extract_fts_fields(&doc);
        if text.is_empty() {
            return Ok(());
        }
        self.inverted
            .index_document(
                database_id,
                crate::types::TenantId::new(tid),
                collection,
                surrogate,
                &text,
            )
            .map_err(|e| {
                let err = UndoError::failed(
                    entry_index,
                    format!("fts re-index on {collection}/{document_id}"),
                    e,
                );
                error!(
                    core = self.core_id,
                    entry_index,
                    collection = %collection,
                    document_id = %document_id,
                    error = %err,
                    "transaction undo: FTS re-index failed; shard state unknown"
                );
                err
            })
    }
}

/// FTS rollback coverage for a STRICT (Binary Tuple) collection — the
/// first-time strict FTS-undo code path.
///
/// A rolled-back transactional DELETE must re-index the restored strict body
/// (decoded via the schema-aware `decode_stored_document`) so the document is
/// searchable again; a rolled-back PUT must remove the postings it wrote.
#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::DatabaseId;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::transaction::redo_apply::test_commit::{
        doc_delete_sub_record, doc_put_sub_record,
    };
    use crate::engine::document::store::CollectionConfig;
    use crate::types::TenantId;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "strict_docs";
    const PK: &str = "row1";

    /// Register a strict collection whose first column is a non-null `_rowid`
    /// (so `apply_point_put` injects the surrogate) plus a nullable `body` text
    /// column that feeds the inverted index.
    fn register_strict(core: &mut CoreLoop) {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("body", ColumnType::String),
        ])
        .unwrap();
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema }),
        );
    }

    /// MessagePack input document (no `_rowid` — the strict path injects it).
    fn doc_bytes() -> Vec<u8> {
        use nodedb_types::Value;
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "body".to_string(),
            Value::String("searchable elephant paragraph".into()),
        );
        nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap()
    }

    fn fts_searchable(core: &CoreLoop) -> bool {
        !core
            .inverted
            .search(
                DB,
                TenantId::new(TID),
                COLL,
                nodedb_fts::FtsSearchParams {
                    query: "elephant",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: nodedb_fts::posting::QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap()
            .is_empty()
    }

    /// Commit an insert through the committed-redo install and discard its
    /// undo log.
    fn commit_put(core: &mut CoreLoop) {
        core.install_with_undo_for_test(
            TID,
            10,
            vec![doc_put_sub_record(COLL, PK, &doc_bytes(), 1)],
        );
    }

    #[test]
    fn strict_tx_delete_rollback_restores_fts_postings() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        register_strict(&mut core);

        commit_put(&mut core);
        assert!(
            fts_searchable(&core),
            "strict body must be searchable after insert"
        );

        let undo_log =
            core.install_with_undo_for_test(TID, 20, vec![doc_delete_sub_record(COLL, PK, 1)]);
        assert!(
            !fts_searchable(&core),
            "delete cascade must remove strict FTS postings"
        );

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");
        assert!(
            fts_searchable(&core),
            "strict FTS postings must be restored (searchable again) after delete-rollback"
        );
    }

    #[test]
    fn strict_tx_put_rollback_removes_fts_postings() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _t, _r) = make_core_with_dir(dir.path());
        register_strict(&mut core);

        assert!(!fts_searchable(&core));

        let undo_log = core.install_with_undo_for_test(
            TID,
            20,
            vec![doc_put_sub_record(COLL, PK, &doc_bytes(), 1)],
        );
        assert!(fts_searchable(&core), "strict body searchable mid-tx");

        core.rollback_undo_log(DB, TID, undo_log)
            .expect("rollback must succeed");
        assert!(
            !fts_searchable(&core),
            "strict FTS postings must be gone after put-rollback"
        );
    }
}
