// SPDX-License-Identifier: BUSL-1.1

//! The applier's epoch, and which cuts every mirror group applied.

use std::collections::VecDeque;

use nodedb_types::DatabaseId;

use crate::control::security::auth_fence::cluster::hosts_group;
use crate::control::state::SharedState;

use super::propose::{MirrorRaft, SealAt};

/// The bits of an epoch that hold the per-term counter.
const EPOCH_COUNTER_BITS: u32 = 32;

/// Mints each applier life's epoch: `(term << 32) | per-term counter`.
///
/// One leader can fence many times in one term: each failed attempt fences. A
/// bare term gives them all one epoch, and the old attempt's copies apply.
#[derive(Debug, Default)]
pub(super) struct EpochMint {
    term: u64,
    counter: u64,
}

impl EpochMint {
    /// The next epoch at `term`, above `fence`. `None` when `fence` belongs to a
    /// later term: a newer leader owns the mirror.
    pub(super) fn next(&mut self, term: u64, fence: u64) -> Option<u64> {
        let fence_term = fence >> EPOCH_COUNTER_BITS;
        if fence_term > term {
            return None;
        }
        if term != self.term {
            self.term = term;
            self.counter = 0;
        }
        if fence_term == term {
            self.counter = self.counter.max(fence & u64::from(u32::MAX));
        }
        self.counter += 1;
        Some((term << EPOCH_COUNTER_BITS) | (self.counter & u64::from(u32::MAX)))
    }
}

/// A cut the mirror raised, waiting for every group to apply it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingCut {
    pub cut: u64,
    pub hlc_ns: u64,
    pub cap_ordinal: i64,
    pub seals: Vec<SealAt>,
}

/// Whether a group applied `cut`, from this node's in-memory seal. A missing
/// value means not sealed: the seals live in memory, and a restart loses them.
fn sealed(sealed_cut: Option<u64>, cut: u64) -> bool {
    sealed_cut.is_some_and(|sealed| sealed >= cut)
}

/// The cuts the mirror raised and no group set can vouch for yet, oldest first.
#[derive(Debug, Default)]
pub(super) struct AppliedTracker {
    pending: VecDeque<PendingCut>,
}

impl AppliedTracker {
    pub(super) fn push(&mut self, cut: PendingCut) {
        self.pending.push_back(cut);
    }

    /// The newest raised cut every group applied. A group this node hosts
    /// answers from its seal. Any other group answers from a member's durable
    /// apply, so a leader with no replica of it still moves the state.
    pub(super) async fn take_newest_applied(
        &mut self,
        state: &SharedState,
        raft: &dyn MirrorRaft,
        database: DatabaseId,
    ) -> Option<PendingCut> {
        let mut newest = None;
        while let Some(front) = self.pending.front() {
            if !every_group_applied(state, raft, database, front).await {
                break;
            }
            newest = self.pending.pop_front();
        }
        newest
    }
}

pub(super) async fn every_group_applied(
    state: &SharedState,
    raft: &dyn MirrorRaft,
    database: DatabaseId,
    pending: &PendingCut,
) -> bool {
    for seal in &pending.seals {
        let applied = if hosts_group(state, seal.group) {
            sealed(
                state.mirror.cuts.sealed_cut(database, seal.group),
                pending.cut,
            )
        } else {
            raft.probe(seal.group, Vec::new())
                .await
                .is_ok_and(|answer| answer.durable_applied >= seal.index)
        };
        if !applied {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_mint_of_a_term_rises() {
        let mut mint = EpochMint::default();
        assert_eq!(mint.next(3, 0), Some((3 << 32) | 1));
        assert_eq!(mint.next(3, (3 << 32) | 1), Some((3 << 32) | 2));
        assert_eq!(mint.next(4, (3 << 32) | 2), Some((4 << 32) | 1));
    }

    /// A restart in one term loses the counter. The fence holds it.
    #[test]
    fn a_mint_lands_above_the_fence_of_its_own_term() {
        let mut mint = EpochMint::default();
        assert_eq!(mint.next(3, (3 << 32) | 9), Some((3 << 32) | 10));
    }

    #[test]
    fn a_later_terms_fence_stops_the_mint() {
        let mut mint = EpochMint::default();
        assert_eq!(mint.next(3, 4 << 32), None);
    }

    #[test]
    fn a_missing_seal_is_not_sealed() {
        assert!(!sealed(None, 1));
        assert!(!sealed(Some(1), 2));
        assert!(sealed(Some(2), 2));
        assert!(sealed(Some(3), 2));
    }
}
