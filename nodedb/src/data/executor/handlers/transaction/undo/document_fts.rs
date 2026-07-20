// SPDX-License-Identifier: BUSL-1.1

//! Full-text re-indexing for a rolled-back document DELETE.
//!
//! The forward delete cascade removes a document's inverted-index postings
//! unconditionally (both plain and bitemporal collections). A transactional
//! rollback restores the document body into the primary store, so it must also
//! recompute and re-insert the FTS postings — otherwise the row comes back
//! restored-but-unsearchable. `nodedb_fts::analyze` is deterministic, so the
//! recomputed text (extracted via the same [`extract_fts_text`] helper the
//! forward PUT path uses) reproduces byte-identical postings.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_text;

use super::document::UndoDocumentContext;

impl CoreLoop {
    /// Re-index a restored document's text into the inverted index during
    /// DELETE rollback. Decodes the restored body through the storage-mode-aware
    /// helper (strict → Binary Tuple, schemaless → MessagePack) so both modes
    /// recompute their real text. Returns `Err((entry_index, detail))` on
    /// failure so a partial FTS restore escalates to `RollbackFailed`.
    pub(super) fn reindex_restored_document_fts(
        &self,
        ctx: UndoDocumentContext<'_>,
        surrogate: nodedb_types::Surrogate,
        old_value: &[u8],
    ) -> Result<(), (usize, String)> {
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
        let Some(doc) = self.decode_stored_document(config, old_value) else {
            return Ok(());
        };
        let text = extract_fts_text(&doc);
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
                error!(
                    core = self.core_id,
                    entry_index,
                    collection = %collection,
                    document_id = %document_id,
                    error = %e,
                    "transaction undo: FTS re-index failed; shard state unknown"
                );
                (
                    entry_index,
                    format!("fts re-index on {collection}/{document_id}: {e}"),
                )
            })
    }
}
