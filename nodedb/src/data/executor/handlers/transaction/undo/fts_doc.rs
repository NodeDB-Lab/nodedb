// SPDX-License-Identifier: BUSL-1.1

//! Undo of one sync-ingested full-text write (`TextOp::FtsIndexDoc` /
//! `FtsDeleteDoc`).
//!
//! The write replaces the document's whole index footprint. Its pre-image is
//! the footprint before the write (`InvertedIndex::document_image`), and the
//! undo re-indexes it, or removes the document when it was not indexed.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::inverted::FtsDocImage;
use crate::types::{DatabaseId, TenantId};

use super::UndoEntry;

/// The pre-image of one document's index footprint.
pub(in crate::data::executor) struct FtsDocUndo {
    pub database_id: u64,
    pub tid: u64,
    pub collection: String,
    pub surrogate: Surrogate,
    /// `None` when the document was not indexed before the write.
    pub prior: Option<FtsDocImage>,
}

impl CoreLoop {
    /// The undo entries of one full-text write: the document's footprint and
    /// the sync high-water mark its provenance advances.
    pub(in crate::data::executor) fn capture_fts_doc_undo(
        &self,
        database_id: DatabaseId,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        provenance: Option<&SyncProvenance>,
    ) -> crate::Result<Vec<UndoEntry>> {
        let prior = self.inverted.document_image(
            database_id.as_u64(),
            TenantId::new(tid),
            collection,
            surrogate,
        )?;
        let mut undo = vec![UndoEntry::FtsDocument(Box::new(FtsDocUndo {
            database_id: database_id.as_u64(),
            tid,
            collection: collection.to_string(),
            surrogate,
            prior,
        }))];
        undo.extend(self.capture_sync_hwm_undo(provenance));
        Ok(undo)
    }

    /// Put the document's index footprint back.
    pub(super) fn apply_undo_fts_doc(
        &mut self,
        entry_index: usize,
        undo: FtsDocUndo,
    ) -> Result<(), (usize, String)> {
        self.inverted
            .restore_document_image(
                undo.database_id,
                TenantId::new(undo.tid),
                &undo.collection,
                undo.surrogate,
                undo.prior.as_ref(),
            )
            .map_err(|e| {
                (
                    entry_index,
                    format!(
                        "restoring the full-text footprint of {} in '{}': {e}",
                        undo.surrogate.as_u32(),
                        undo.collection
                    ),
                )
            })
    }
}
