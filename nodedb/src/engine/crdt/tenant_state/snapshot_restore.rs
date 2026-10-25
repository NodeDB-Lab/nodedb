// SPDX-License-Identifier: BUSL-1.1

//! Exact collection-state replacement for CRDT transaction rollback.

use nodedb_crdt::state::CrdtState;

use super::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Replace one collection's state with an exact pre-image.
    ///
    /// Normal snapshot import is a monotonic Loro merge and therefore cannot
    /// undo a delta already imported into the same `LoroDoc`. Transaction
    /// rollback needs replacement semantics instead: construct and validate a
    /// fresh document first, then atomically replace this collection's entry.
    /// `None` restores the prior absence of the collection.
    pub(crate) fn restore_collection_snapshot(
        &mut self,
        collection: &str,
        snapshot: Option<&[u8]>,
    ) -> crate::Result<()> {
        let Some(snapshot) = snapshot else {
            self.collections.remove(collection);
            return Ok(());
        };

        // Do every fallible step before mutating `collections`, so an invalid
        // rollback token cannot discard the current state while reporting an
        // error to the transaction driver.
        // Same per-collection derivation as `state_mut`: a rollback must not
        // hand the collection back a document whose operation identities
        // collide with a sibling collection's.
        //
        // The pre-image is a snapshot this process exported when the
        // transaction opened, so it is admitted as local: under the peer
        // ceilings a collection that grew past them could be written but never
        // rolled back, and the transaction driver would be handed a failure it
        // has no way to act on.
        let replacement = CrdtState::from_local_snapshot(self.peer_id_of(collection), snapshot)
            .map_err(crate::Error::Crdt)?;
        self.collections.insert(collection.to_owned(), replacement);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::constraint::ConstraintSet;

    use crate::types::TenantId;

    use super::*;

    /// Build a peer document spanning two collections and export one incremental
    /// delta per write, mirroring how an embedded client that keeps a single Loro
    /// document for the whole database produces its deltas.
    ///
    /// Returns `(first_delta_for_target, later_delta_for_target)` where the later
    /// delta causally depends on an intervening write to the *other* collection.
    fn interleaved_collection_deltas(peer: u64, target: &str, other: &str) -> (Vec<u8>, Vec<u8>) {
        let state = CrdtState::new(peer).unwrap();

        let v0 = state.oplog_version_vector();
        state
            .upsert(target, "first", &[("v", LoroValue::I64(1))])
            .unwrap();
        let first = state.export_updates_since(&v0).unwrap();

        state
            .upsert(other, "aside", &[("v", LoroValue::I64(2))])
            .unwrap();

        let v2 = state.oplog_version_vector();
        state
            .upsert(target, "later", &[("v", LoroValue::I64(3))])
            .unwrap();
        let later = state.export_updates_since(&v2).unwrap();

        (first, later)
    }

    /// Transaction rollback replaces a collection with an exact pre-image. If the
    /// pre-image import leaves operations pending, rollback installs an incomplete
    /// state and tells the transaction driver it succeeded.
    #[test]
    fn rollback_pre_image_does_not_report_success_without_applying() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let (first, later) = interleaved_collection_deltas(33, "users", "signals");

        engine
            .restore_collection_snapshot("users", Some(&first))
            .unwrap();
        assert!(engine.row_exists("users", "first"));

        let result = engine.restore_collection_snapshot("users", Some(&later));

        assert!(
            result.is_err() || engine.row_exists("users", "later"),
            "rollback reported success while the pre-image's operations stayed \
             causally pending — the transaction driver believes state was restored"
        );
    }
}
