// SPDX-License-Identifier: BUSL-1.1

//! Which Calvin positions this node's replica of each vShard applied.
//!
//! One [`CalvinAppliedLedger`] per vShard answers whether this replica
//! applied `(epoch, position)`. Boot recovery fills it before the data-group
//! apply loop starts. A snapshot install replaces it. A scheduler seeds its
//! applied gate from it and marks each position it finishes. A checkpoint
//! saves it before WAL truncation deletes the records it came from.
//!
//! The ledger keeps the shape of the scheduler's gate: a fully-applied
//! watermark and the applied positions above it. The tail is pruned as the
//! watermark advances.
//!
//! A claim reserves a position while one stamped redo installs it. A second
//! copy of the same position finds the claim and installs nothing. A claim
//! is not an applied position: [`CalvinAppliedLedger::snapshot`] leaves it
//! out.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use super::recovery::NOT_YET_APPLIED_EPOCH;
use crate::control::security::catalog::calvin_applied::StoredCalvinApplied;

/// Why [`CalvinAppliedLedger::claim`] refused a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRefusal {
    /// The position is applied on this replica.
    Applied,
    /// An install of the position holds its claim.
    Claimed,
}

#[derive(Debug)]
struct LedgerState {
    /// Every position of every epoch at or below this is applied.
    /// [`NOT_YET_APPLIED_EPOCH`] means none is.
    fully_applied_epoch: u64,
    /// Applied positions of epochs above the watermark.
    tail: BTreeSet<(u64, u32)>,
    /// Positions an install holds and has not finished.
    claims: BTreeSet<(u64, u32)>,
}

impl LedgerState {
    fn below_watermark(&self, epoch: u64) -> bool {
        self.fully_applied_epoch != NOT_YET_APPLIED_EPOCH && epoch <= self.fully_applied_epoch
    }

    fn is_applied(&self, epoch: u64, position: u32) -> bool {
        self.below_watermark(epoch) || self.tail.contains(&(epoch, position))
    }

    fn insert_applied(&mut self, epoch: u64, position: u32) {
        if !self.below_watermark(epoch) {
            self.tail.insert((epoch, position));
        }
    }
}

/// Applied positions of one vShard on this node.
#[derive(Debug)]
pub struct CalvinAppliedLedger {
    state: Mutex<LedgerState>,
}

impl CalvinAppliedLedger {
    /// A ledger holding the watermark `fully_applied_epoch` and the applied
    /// positions `tail` above it, with no claim.
    pub fn new(fully_applied_epoch: u64, tail: BTreeSet<(u64, u32)>) -> Self {
        Self {
            state: Mutex::new(LedgerState {
                fully_applied_epoch,
                tail,
                claims: BTreeSet::new(),
            }),
        }
    }

    /// Reserve `(epoch, position)` for one install. Refuses an applied
    /// position and a position another install holds.
    pub fn claim(&self, epoch: u64, position: u32) -> Result<(), ClaimRefusal> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.is_applied(epoch, position) {
            return Err(ClaimRefusal::Applied);
        }
        if !state.claims.insert((epoch, position)) {
            return Err(ClaimRefusal::Claimed);
        }
        Ok(())
    }

    /// Drop the claim on `(epoch, position)`: its install did not apply, so a
    /// later copy can install it.
    pub fn release_claim(&self, epoch: u64, position: u32) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.claims.remove(&(epoch, position));
    }

    /// Record that the install of `(epoch, position)` is durable. Drops its
    /// claim.
    pub fn mark_applied(&self, epoch: u64, position: u32) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.claims.remove(&(epoch, position));
        state.insert_applied(epoch, position);
    }

    /// Record that `(epoch, position)` finished with no install on this
    /// vShard: an abort, or a slice that writes nothing. A claim on it stays
    /// until its holder finishes.
    pub fn mark_terminal(&self, epoch: u64, position: u32) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.insert_applied(epoch, position);
    }

    /// Record that every position of every epoch at or below `watermark`
    /// applied. A lower watermark changes nothing.
    pub fn fold(&self, watermark: u64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.below_watermark(watermark) {
            return;
        }
        state.fully_applied_epoch = watermark;
        let above = (watermark.saturating_add(1), 0);
        state.tail = state.tail.split_off(&above);
        state.claims = state.claims.split_off(&above);
    }

    /// The fully-applied watermark and the applied positions above it.
    /// Claims are not applied positions, so they are absent.
    pub fn snapshot(&self) -> (u64, BTreeSet<(u64, u32)>) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (state.fully_applied_epoch, state.tail.clone())
    }

    /// Whether this replica applied `(epoch, position)`.
    pub fn is_applied(&self, epoch: u64, position: u32) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.is_applied(epoch, position)
    }

    /// The highest epoch with an applied position, or
    /// [`NOT_YET_APPLIED_EPOCH`] when none is applied.
    pub fn max_applied_epoch(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.tail.last() {
            Some((epoch, _)) => *epoch,
            None => state.fully_applied_epoch,
        }
    }
}

/// The applied ledger of every vShard this node holds Calvin state for.
#[derive(Debug, Default)]
pub struct CalvinAppliedLedgers {
    by_vshard: Mutex<HashMap<u32, Arc<CalvinAppliedLedger>>>,
}

impl CalvinAppliedLedgers {
    /// Replace the ledger of `vshard_id` with one holding
    /// `fully_applied_epoch` and `tail`. A holder of the old ledger keeps the
    /// old one.
    pub fn install(
        &self,
        vshard_id: u32,
        fully_applied_epoch: u64,
        tail: BTreeSet<(u64, u32)>,
    ) -> Arc<CalvinAppliedLedger> {
        let ledger = Arc::new(CalvinAppliedLedger::new(fully_applied_epoch, tail));
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&ledger));
        ledger
    }

    /// The ledger of `vshard_id`. A vShard with no ledger has no Calvin
    /// history on this node, so it gets an empty one.
    pub fn get_or_create(&self, vshard_id: u32) -> Arc<CalvinAppliedLedger> {
        let mut by_vshard = self.by_vshard.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(by_vshard.entry(vshard_id).or_insert_with(|| {
            Arc::new(CalvinAppliedLedger::new(
                NOT_YET_APPLIED_EPOCH,
                BTreeSet::new(),
            ))
        }))
    }

    /// The ledger of `vshard_id`, when this node holds one.
    pub fn get(&self, vshard_id: u32) -> Option<Arc<CalvinAppliedLedger>> {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&vshard_id)
            .cloned()
    }

    /// Drop the ledger of `vshard_id`: this node left the vShard's group and
    /// holds none of its applied positions.
    pub fn remove(&self, vshard_id: u32) {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard_id);
    }

    /// Every ledger's applied state, in the shape the catalog stores.
    pub fn snapshot_all(&self) -> Vec<StoredCalvinApplied> {
        let ledgers: Vec<(u32, Arc<CalvinAppliedLedger>)> = self
            .by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(vshard_id, ledger)| (*vshard_id, Arc::clone(ledger)))
            .collect();
        ledgers
            .into_iter()
            .map(|(vshard_id, ledger)| {
                let (fully_applied_epoch, tail) = ledger.snapshot();
                StoredCalvinApplied {
                    vshard_id,
                    fully_applied_epoch,
                    tail,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> CalvinAppliedLedger {
        CalvinAppliedLedger::new(NOT_YET_APPLIED_EPOCH, BTreeSet::new())
    }

    #[test]
    fn a_ledger_answers_like_the_gate() {
        let ledger = empty();
        assert!(!ledger.is_applied(0, 0));
        ledger.mark_applied(3, 1);
        assert!(ledger.is_applied(3, 1));
        assert!(!ledger.is_applied(3, 0));
        ledger.fold(3);
        assert!(ledger.is_applied(3, 0));
        assert!(ledger.is_applied(2, 9));
        assert!(!ledger.is_applied(4, 0));
        // A mark at or below the watermark changes nothing.
        ledger.mark_terminal(1, 0);
        assert!(ledger.is_applied(1, 0));
        // A lower watermark never moves it back.
        ledger.fold(1);
        assert!(ledger.is_applied(3, 0));
        assert_eq!(ledger.max_applied_epoch(), 3);
    }

    #[test]
    fn a_claim_refuses_a_claimed_or_applied_position() {
        let ledger = empty();
        assert_eq!(ledger.claim(2, 0), Ok(()));
        assert_eq!(ledger.claim(2, 0), Err(ClaimRefusal::Claimed));
        ledger.mark_applied(2, 0);
        assert_eq!(ledger.claim(2, 0), Err(ClaimRefusal::Applied));
        ledger.mark_terminal(2, 1);
        assert_eq!(ledger.claim(2, 1), Err(ClaimRefusal::Applied));
        ledger.fold(5);
        assert_eq!(ledger.claim(4, 7), Err(ClaimRefusal::Applied));
        assert_eq!(ledger.claim(6, 0), Ok(()));
    }

    #[test]
    fn a_released_claim_can_be_claimed_again() {
        let ledger = empty();
        assert_eq!(ledger.claim(1, 3), Ok(()));
        ledger.release_claim(1, 3);
        assert!(!ledger.is_applied(1, 3));
        assert_eq!(ledger.claim(1, 3), Ok(()));
    }

    #[test]
    fn snapshot_names_only_applied_positions() {
        let ledger = empty();
        ledger.claim(4, 0).expect("claim");
        ledger.claim(4, 1).expect("claim");
        ledger.mark_applied(4, 1);
        ledger.mark_terminal(5, 2);
        let (watermark, tail) = ledger.snapshot();
        assert_eq!(watermark, NOT_YET_APPLIED_EPOCH);
        assert_eq!(tail, [(4, 1), (5, 2)].into_iter().collect());
    }

    #[test]
    fn an_empty_ledger_has_no_applied_epoch() {
        assert_eq!(empty().max_applied_epoch(), NOT_YET_APPLIED_EPOCH);
        let folded = CalvinAppliedLedger::new(6, BTreeSet::new());
        assert_eq!(folded.max_applied_epoch(), 6);
    }

    #[test]
    fn an_installed_ledger_replaces_the_previous_one() {
        let ledgers = CalvinAppliedLedgers::default();
        let first = ledgers.install(7, NOT_YET_APPLIED_EPOCH, BTreeSet::new());
        first.mark_applied(1, 0);
        let second = ledgers.install(7, 4, BTreeSet::new());
        let current = ledgers.get(7).expect("ledger");
        assert!(Arc::ptr_eq(&current, &second));
        assert!(current.is_applied(4, 0));
        assert!(ledgers.get(8).is_none());
    }

    #[test]
    fn get_or_create_keeps_an_existing_ledger() {
        let ledgers = CalvinAppliedLedgers::default();
        let recovered = ledgers.install(3, NOT_YET_APPLIED_EPOCH, [(2, 0)].into_iter().collect());
        let used = ledgers.get_or_create(3);
        assert!(Arc::ptr_eq(&recovered, &used));
        assert!(used.is_applied(2, 0));
        let fresh = ledgers.get_or_create(9);
        assert!(!fresh.is_applied(0, 0));
        assert_eq!(ledgers.snapshot_all().len(), 2);
        ledgers.remove(9);
        assert!(ledgers.get(9).is_none());
    }
}
