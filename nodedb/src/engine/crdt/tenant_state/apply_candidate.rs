// SPDX-License-Identifier: BUSL-1.1

//! The detached candidate document a validated apply imports a delta into.
//!
//! A clean apply returns the displaced document to a pool as the next
//! delta's candidate. A run of deltas then pays one full copy per
//! collection, not one per delta.

use nodedb_crdt::state::CrdtState;

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// A document identical to `collection`'s authoritative state, to import a
    /// delta into before deciding whether to keep it.
    ///
    /// Reuses the candidate left behind by the previous apply when there is
    /// one. Building it costs a full encode and decode of the collection —
    /// Loro's own `fork` is implemented the same way, so there is no cheaper
    /// copy available — and a run of deltas (WAL replay, a committed batch, a
    /// sync burst) would otherwise pay that per delta rather than once.
    ///
    /// The candidate is only ever returned to the pool after a clean apply. A
    /// delta that was refused for any reason leaves its operations in the
    /// candidate — Loro buffers even causally-pending ones — so a poisoned
    /// candidate is dropped rather than reused.
    pub(super) fn take_apply_candidate(
        &mut self,
        collection: &str,
    ) -> Result<CrdtState, nodedb_crdt::CrdtError> {
        if let Some(candidate) = self.apply_candidates.remove(collection) {
            // A candidate is only usable while it still matches the state it
            // was cloned from. Anything else that touches this collection — a
            // local write through `state_mut`, a transaction rollback, a
            // snapshot import, a purge — leaves it behind, and installing a
            // behind candidate as authoritative would silently discard those
            // writes. Checking the frontier here means no other mutation site
            // has to remember this one exists; a stale candidate is simply
            // rebuilt.
            let current_frontier = self.collections.get(collection).map(|s| s.frontier());
            if current_frontier.is_some_and(|frontier| frontier == candidate.frontier()) {
                return Ok(candidate);
            }
        }
        // The candidate becomes authoritative on a clean apply, so it must be
        // born with the same derived peer id `state_mut` would have given the
        // collection. The node's base peer id would mint operation ids that
        // collide across collections — the silent row-drop `collection_peer_id`
        // exists to prevent.
        let peer_id = self.peer_id_of(collection);
        match self.collections.get(collection) {
            // Its own snapshot, exported on the line above: admitted as local,
            // because under the peer ceilings a collection that outgrew them
            // would fail to seed and every subsequent delta would report
            // `Malformed` — silently unwritable, with the sender blamed for it.
            Some(current) => {
                let snapshot = current.export_snapshot()?;
                CrdtState::from_local_snapshot(peer_id, &snapshot)
            }
            None => CrdtState::new(peer_id),
        }
    }

    /// Drop every retained apply candidate.
    ///
    /// Callers that apply a run of deltas — WAL replay, a committed batch —
    /// call this when the run ends, so the second document per collection does
    /// not outlive the work it accelerates.
    pub fn clear_apply_candidates(&mut self) {
        self.apply_candidates.clear();
    }

    /// How many collections are currently holding a retained candidate.
    #[cfg(test)]
    pub(crate) fn apply_candidate_count(&self) -> usize {
        self.apply_candidates.len()
    }

    /// The Loro peer id of a collection's authoritative document.
    #[cfg(test)]
    pub(crate) fn collection_peer_id_for_test(&self, collection: &str) -> Option<u64> {
        self.collections.get(collection).map(|s| s.peer_id())
    }
}
