// SPDX-License-Identifier: BUSL-1.1

//! Server-built document-row mutations for `CrdtOp::DocUpsert` / `DocDelete`.

use loro::LoroValue;

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
}
