// SPDX-License-Identifier: BUSL-1.1

//! The ordered cuts whose barrier one vShard's scheduler waits for.
//!
//! A cut marker that carries a barrier arrives between two epoch batches, as
//! every marker does. Every transaction of an epoch at or below the highest
//! epoch delivered before the marker came before it. Its slice installed
//! before the scheduler passed the marker, so its redo sits before the
//! group's barrier. Every transaction of a later epoch came after the
//! marker: its redo waits until this node applied the group's barrier. A
//! transaction before the marker never waits on it, so the hold never keeps
//! the marker from passing, nor the barrier from being placed.
//!
//! Every replica tracks the same cuts, so a new leader holds the same
//! transactions. A copy of a tracked cut, which the cut proposes again when
//! a leader change can drop its marker, changes nothing: the first copy
//! placed the cut.

use crate::control::backup::cut_order::OrderedCut;

/// A cut whose barrier the scheduler waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldCut {
    cut: OrderedCut,
    /// The highest epoch delivered before the cut's marker, `None` when none
    /// was: then every transaction came after it.
    through: Option<u64>,
}

/// The cuts one scheduler holds later redo for, in arrival order.
#[derive(Debug, Default)]
pub struct CutHolds {
    held: Vec<HeldCut>,
}

impl CutHolds {
    /// Hold for `cut`, whose marker arrived after epochs up to `through`.
    /// Returns `false` for a copy of a cut already held.
    pub fn receive(&mut self, cut: OrderedCut, through: Option<u64>) -> bool {
        let key = cut.key();
        if self.held.iter().any(|held| held.cut.key() == key) {
            return false;
        }
        self.held.push(HeldCut { cut, through });
        true
    }

    /// Whether a transaction of `epoch` waits for a held cut's barrier.
    pub fn holds(&self, epoch: u64) -> bool {
        self.held
            .iter()
            .any(|held| held.through.is_none_or(|through| through < epoch))
    }

    /// Stop holding for every cut `ended` names. Returns whether any ended.
    pub fn end(&mut self, mut ended: impl FnMut(&OrderedCut) -> bool) -> bool {
        let before = self.held.len();
        self.held.retain(|held| !ended(&held.cut));
        self.held.len() != before
    }

    /// Every held cut, in arrival order.
    pub fn cuts(&self) -> impl Iterator<Item = &OrderedCut> {
        self.held.iter().map(|held| &held.cut)
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cut(hlc: u64) -> OrderedCut {
        OrderedCut {
            hlc,
            restore_point: 0,
            capture: None,
        }
    }

    #[test]
    fn only_a_transaction_after_the_marker_waits() {
        let mut holds = CutHolds::default();
        assert!(!holds.holds(9), "no cut, no hold");
        assert!(holds.receive(cut(100), Some(7)));
        assert!(!holds.holds(7), "epoch 7 came before the marker");
        assert!(!holds.holds(3));
        assert!(holds.holds(8), "epoch 8 came after it");
    }

    #[test]
    fn a_marker_before_any_epoch_holds_every_transaction() {
        let mut holds = CutHolds::default();
        holds.receive(cut(100), None);
        assert!(holds.holds(0));
        assert!(holds.holds(1));
    }

    /// A second copy of a held cut arrives later and keeps the first copy's
    /// place: the transactions between the two copies still wait.
    #[test]
    fn a_copy_of_a_held_cut_keeps_the_first_place() {
        let mut holds = CutHolds::default();
        assert!(holds.receive(cut(100), Some(7)));
        assert!(
            !holds.receive(cut(100), Some(12)),
            "a copy is not a new cut"
        );
        assert!(holds.holds(9), "epoch 9 came after the first copy");
        assert_eq!(holds.cuts().count(), 1);
    }

    #[test]
    fn an_ended_cut_releases_its_transactions() {
        let mut holds = CutHolds::default();
        holds.receive(cut(100), Some(7));
        holds.receive(cut(200), Some(10));
        assert!(!holds.end(|_| false));
        assert!(holds.end(|ended| ended.hlc == 100));
        assert!(!holds.holds(9), "only the later cut holds, from epoch 11");
        assert!(holds.holds(11));
        assert!(holds.end(|_| true));
        assert!(holds.is_empty());
        assert!(!holds.holds(11));
    }
}
