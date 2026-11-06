// SPDX-License-Identifier: BUSL-1.1

//! Non-mutating CRDT delta preview and frontier fencing.

use nodedb_types::DatabaseId;

use super::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Preview a delta without creating or changing a collection state entry.
    pub fn preview_delta(
        &self,
        collection: &str,
        document_id: &str,
        delta: &[u8],
    ) -> crate::Result<nodedb_crdt::CrdtDeltaPreview> {
        let empty;
        let state = match self.collections.get(collection) {
            Some(state) => state,
            None => {
                empty = nodedb_crdt::CrdtState::new(self.peer_id).map_err(crate::Error::Crdt)?;
                &empty
            }
        };
        state
            .preview_delta(
                delta,
                super::core::loro_root(collection),
                document_id,
                nodedb_crdt::CrdtDeltaPreviewLimits::default(),
            )
            .map_err(crate::Error::Crdt)
    }

    /// Compute the current domain-bound frontier without creating state.
    pub fn frontier_digest(&self, database_id: DatabaseId, collection: &str) -> [u8; 32] {
        nodedb_crdt::state::frontier_digest::domain_frontier_digest(
            self.tenant_id.as_u64(),
            database_id.as_u64(),
            collection,
            self.collections.get(collection),
        )
    }
}
