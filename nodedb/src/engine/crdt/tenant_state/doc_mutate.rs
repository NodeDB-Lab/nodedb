// SPDX-License-Identifier: BUSL-1.1

//! Server-built document-row mutations for `CrdtOp::DocUpsert` / `DocDelete`.

use loro::LoroValue;
use nodedb_crdt::state::RowImage;

use super::TenantCrdtEngine;
use super::core::loro_root;

impl TenantCrdtEngine {
    /// Insert-or-replace a document row's scalar fields (full-projection LWW
    /// replace — scalar keys absent from `fields` are pruned).
    pub fn doc_upsert(
        &mut self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .upsert(loro_root(collection), row_id, fields)
            .map_err(crate::Error::Crdt)
    }

    /// Partial-merge a document row's scalar fields (UPDATE SET — only the
    /// provided fields are written, untouched keys survive).
    pub fn doc_set_fields(
        &mut self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .set_fields(loro_root(collection), row_id, fields)
            .map_err(crate::Error::Crdt)
    }

    /// Delete a document row (tombstone in the collection's Loro doc).
    pub fn doc_delete(&mut self, collection: &str, row_id: &str) -> crate::Result<()> {
        self.state_mut(collection)?
            .delete(loro_root(collection), row_id)
            .map_err(crate::Error::Crdt)
    }

    /// Capture the state of a document row that `doc_upsert` and
    /// `doc_set_fields` can change. `None`: the collection has no local
    /// state.
    pub fn doc_row_image(&self, collection: &str, row_id: &str) -> crate::Result<Option<RowImage>> {
        match self.collections.get(collection) {
            Some(state) => state
                .row_image(loro_root(collection), row_id)
                .map(Some)
                .map_err(crate::Error::Crdt),
            None => Ok(None),
        }
    }

    /// Put a document row back to the image `doc_row_image` captured. `None`
    /// removes the collection's local state: it had none before the write.
    pub fn restore_doc_row(
        &mut self,
        collection: &str,
        row_id: &str,
        image: Option<&RowImage>,
    ) -> crate::Result<()> {
        let Some(image) = image else {
            self.collections.remove(collection);
            return Ok(());
        };
        self.state_mut(collection)?
            .restore_row_image(loro_root(collection), row_id, image)
            .map_err(crate::Error::Crdt)
    }
}
