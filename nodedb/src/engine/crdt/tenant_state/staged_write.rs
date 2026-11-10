// SPDX-License-Identifier: BUSL-1.1

//! Writes the authoritative CRDT state takes only once their sparse
//! projection has landed.
//!
//! A local one-row write (`DocUpsert`, a block-list op) is predicted first
//! ([`TenantCrdtEngine::predict_row`]): the deep row it leaves, with the
//! error it raises, computed from the row alone and changing nothing. The
//! caller projects the predicted row into the sparse store, and only then
//! runs the write ([`TenantCrdtEngine::apply_row_write`]). A projection that
//! fails leaves the document, its version vector, its nested containers and
//! its exported bytes unchanged. The cost is the row's size, never the
//! collection's.
//!
//! A sync delta is staged instead
//! ([`TenantCrdtEngine::apply_committed_delta_staged`]): the clean apply
//! keeps the state it displaced in a [`StagedInstall`], and a projection that
//! fails puts that state back.
//!
//! The Data Plane core owns the engine alone, so nothing writes the
//! collection between predicting and applying.

use loro::LoroValue;
use nodedb_crdt::state::RowWrite;

use super::apply_validated::StagedInstall;
use super::core::{TenantCrdtEngine, loro_root};

impl TenantCrdtEngine {
    /// The deep row `row_id` of `collection` after `write`, or the error
    /// `write` raises. Changes no document.
    pub fn predict_row(
        &mut self,
        collection: &str,
        row_id: &str,
        write: &RowWrite<'_>,
    ) -> crate::Result<LoroValue> {
        self.state_mut(collection)?
            .predict_row(loro_root(collection), row_id, write)
            .map_err(crate::Error::Crdt)
    }

    /// Run `write` on row `row_id` of `collection`.
    pub fn apply_row_write(
        &mut self,
        collection: &str,
        row_id: &str,
        write: &RowWrite<'_>,
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .apply_row_write(loro_root(collection), row_id, write)
            .map_err(crate::Error::Crdt)
    }

    /// Keep a staged sync apply. The displaced state takes the same delta
    /// and becomes the next apply's candidate, so the next delta pays its
    /// own size, not a copy of the collection.
    pub fn confirm_install(&mut self, staged: StagedInstall) {
        let StagedInstall {
            collection,
            previous,
            delta,
            local,
        } = staged;
        let Some(previous) = previous else {
            return;
        };
        let imported = if local {
            previous.import_local(&delta)
        } else {
            previous.import(&delta)
        };
        // Both documents held the same state and take the same delta, so
        // they stay identical. One that did not land is dropped and the
        // next apply rebuilds a candidate: slower, never wrong.
        if imported.is_ok() {
            self.apply_candidates.insert(collection, previous);
        }
    }

    /// Undo a staged sync apply: the state it displaced is authoritative
    /// again, exactly as it was.
    pub fn rollback_install(&mut self, staged: StagedInstall) {
        let StagedInstall {
            collection,
            previous,
            ..
        } = staged;
        self.apply_candidates.remove(&collection);
        match previous {
            Some(previous) => {
                self.collections.insert(collection, previous);
            }
            None => {
                self.collections.remove(&collection);
            }
        }
    }
}
