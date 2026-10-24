// SPDX-License-Identifier: BUSL-1.1

//! Per-key lock state for the Calvin lock table.
//!
//! A [`LockEntry`] holds one group of compatible holders, plus a FIFO waiter
//! queue that carries each waiter's requested [`LockMode`].

use std::collections::VecDeque;

use smallvec::SmallVec;

use super::lock_key::TxnId;

// ── LockMode ──────────────────────────────────────────────────────────────────

/// The mode under which a lock is held or requested.
///
/// Compatibility:
///
/// | held \ requested | IntentShared | Intent | Shared | Exclusive |
/// |------------------|--------------|--------|--------|-----------|
/// | IntentShared     | yes          | yes    | yes    | no        |
/// | Intent           | yes          | yes    | no     | no        |
/// | Shared           | yes          | no     | yes    | no        |
/// | Exclusive        | no           | no     | no     | no        |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// A row reader's hold on its collection key. Excludes only a
    /// collection-wide writer, so a TRUNCATE orders against the row reads of
    /// its collection while row readers, row writers and predicate readers
    /// share the key.
    IntentShared,
    /// A row writer's hold on its collection key. Compatible with `Intent`
    /// and `IntentShared` holders, so row writers run together and exclude a
    /// predicate reader or a collection-wide writer.
    Intent,
    /// Compatible with other shared holders on the same key.
    Shared,
    /// Held by exactly one transaction; excludes all others.
    Exclusive,
}

impl LockMode {
    /// Whether a holder in `self` and a holder in `other` can hold one key
    /// together.
    pub fn compatible(self, other: LockMode) -> bool {
        match (self, other) {
            (
                LockMode::IntentShared,
                LockMode::IntentShared | LockMode::Intent | LockMode::Shared,
            )
            | (LockMode::Intent | LockMode::Shared, LockMode::IntentShared)
            | (LockMode::Intent, LockMode::Intent)
            | (LockMode::Shared, LockMode::Shared) => true,
            (LockMode::IntentShared | LockMode::Intent | LockMode::Shared, LockMode::Exclusive)
            | (
                LockMode::Exclusive,
                LockMode::IntentShared | LockMode::Intent | LockMode::Shared | LockMode::Exclusive,
            )
            | (LockMode::Intent, LockMode::Shared)
            | (LockMode::Shared, LockMode::Intent) => false,
        }
    }

    /// The one mode that covers both `self` and `other` for one transaction.
    ///
    /// Equal modes merge to themselves. `IntentShared` merges to the other
    /// mode, which covers it. Any other pair merges to `Exclusive`: `Shared`
    /// plus `Intent` means the transaction both reads the whole collection
    /// and writes rows of it.
    pub fn merge(self, other: LockMode) -> LockMode {
        if self == other {
            return self;
        }
        match (self, other) {
            (LockMode::IntentShared, stronger) | (stronger, LockMode::IntentShared) => stronger,
            (
                LockMode::Intent | LockMode::Shared | LockMode::Exclusive,
                LockMode::Intent | LockMode::Shared | LockMode::Exclusive,
            ) => LockMode::Exclusive,
        }
    }
}

// ── AcquireOutcome ────────────────────────────────────────────────────────────

/// Result of a lock-acquire call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    /// All requested locks were granted; the transaction is ready to dispatch.
    Ready,
    /// At least one key conflicted with a current holder or an earlier
    /// waiter. The transaction holds every key it could take and waits in
    /// FIFO order on every other key.
    Blocked,
}

/// Result of a moded acquire: the requester's outcome, plus the other txns
/// the call granted.
///
/// A holder that raises its mode and waits drops its hold. Waiters at the
/// queue front can then take the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acquisition {
    pub outcome: AcquireOutcome,
    /// Other txns that now hold their whole request, in ascending order. The
    /// caller dispatches them like the ids a release returns.
    pub promoted: Vec<TxnId>,
}

// ── LockEntry ─────────────────────────────────────────────────────────────────

/// Per-key lock state.
///
/// Invariants maintained by [`LockManager`](super::manager::LockManager):
/// - Every pair of holder modes is compatible. `IntentShared` holders mix
///   with `Intent` or with `Shared` holders, never with both.
/// - `mode` is the merge of every holder's mode: the strongest one held.
/// - `Exclusive` entries have exactly one holder.
/// - `waiters` is FIFO; each waiter carries the mode it requested so that
///   promotion on release is mode-aware.
pub(super) struct LockEntry {
    /// The strongest mode a current holder holds this lock under. A request
    /// is compatible with every holder iff it is compatible with this mode.
    pub(super) mode: LockMode,
    /// The transactions currently holding this lock, each with its own mode.
    /// Exclusive: exactly one; otherwise one or more.
    pub(super) holders: SmallVec<[(TxnId, LockMode); 2]>,
    /// Transactions waiting for this lock, in FIFO order, each tagged with the
    /// mode it requested.
    pub(super) waiters: VecDeque<(TxnId, LockMode)>,
}

impl LockEntry {
    /// Whether `txn` holds this lock.
    pub(super) fn holds(&self, txn: TxnId) -> bool {
        self.holders.iter().any(|(holder, _)| *holder == txn)
    }

    /// The mode `txn` holds this lock in, `None` when it holds none.
    pub(super) fn mode_of(&self, txn: TxnId) -> Option<LockMode> {
        self.holders
            .iter()
            .find(|(holder, _)| *holder == txn)
            .map(|(_, mode)| *mode)
    }

    /// The holders' transactions, in grant order.
    pub(super) fn holder_ids(&self) -> Vec<TxnId> {
        self.holders.iter().map(|(holder, _)| *holder).collect()
    }

    /// Whether every holder other than `txn` is compatible with `mode`.
    pub(super) fn others_compatible(&self, txn: TxnId, mode: LockMode) -> bool {
        self.holders
            .iter()
            .filter(|(holder, _)| *holder != txn)
            .all(|(_, held)| held.compatible(mode))
    }

    /// Add `txn` in `mode`, or raise the mode it already holds to cover
    /// `mode`, then recompute the group mode.
    pub(super) fn hold(&mut self, txn: TxnId, mode: LockMode) {
        match self.holders.iter_mut().find(|(holder, _)| *holder == txn) {
            Some((_, held)) => *held = held.merge(mode),
            None => self.holders.push((txn, mode)),
        }
        self.regroup();
    }

    /// Drop `txn` from the holders, then recompute the group mode.
    pub(super) fn drop_holder(&mut self, txn: TxnId) {
        self.holders.retain(|(holder, _)| *holder != txn);
        self.regroup();
    }

    /// Set `mode` to the merge of every holder's mode. An entry with no
    /// holder keeps its last mode until a waiter's grant replaces it.
    fn regroup(&mut self) {
        let mut held = self.holders.iter().map(|(_, mode)| *mode);
        if let Some(first) = held.next() {
            self.mode = held.fold(first, LockMode::merge);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [LockMode; 4] = [
        LockMode::IntentShared,
        LockMode::Intent,
        LockMode::Shared,
        LockMode::Exclusive,
    ];

    #[test]
    fn compatibility_matrix() {
        assert!(LockMode::Intent.compatible(LockMode::Intent));
        assert!(LockMode::Shared.compatible(LockMode::Shared));
        assert!(!LockMode::Intent.compatible(LockMode::Shared));
        assert!(!LockMode::Shared.compatible(LockMode::Intent));
        for mode in ALL {
            assert!(!mode.compatible(LockMode::Exclusive));
            assert!(!LockMode::Exclusive.compatible(mode));
            assert_eq!(
                mode.compatible(LockMode::IntentShared),
                mode != LockMode::Exclusive
            );
            assert_eq!(
                LockMode::IntentShared.compatible(mode),
                mode != LockMode::Exclusive
            );
        }
    }

    #[test]
    fn intent_shared_merges_to_the_mode_that_covers_it() {
        for mode in ALL {
            assert_eq!(LockMode::IntentShared.merge(mode), mode);
            assert_eq!(mode.merge(LockMode::IntentShared), mode);
        }
    }

    #[test]
    fn the_group_mode_drops_when_the_strongest_holder_leaves() {
        let (reader, writer) = (TxnId::new(1, 0), TxnId::new(1, 1));
        let mut entry = LockEntry {
            mode: LockMode::IntentShared,
            holders: SmallVec::from_slice(&[(reader, LockMode::IntentShared)]),
            waiters: VecDeque::new(),
        };
        entry.hold(writer, LockMode::Intent);
        assert_eq!(entry.mode, LockMode::Intent);
        assert!(!entry.mode.compatible(LockMode::Shared));
        entry.drop_holder(writer);
        assert_eq!(entry.mode, LockMode::IntentShared);
        assert!(entry.mode.compatible(LockMode::Shared));
        assert_eq!(entry.holder_ids(), vec![reader]);
    }

    #[test]
    fn merge_keeps_the_strongest_mode() {
        for mode in ALL {
            assert_eq!(mode.merge(mode), mode);
            assert_eq!(mode.merge(LockMode::Exclusive), LockMode::Exclusive);
            assert_eq!(LockMode::Exclusive.merge(mode), LockMode::Exclusive);
        }
        assert_eq!(
            LockMode::Shared.merge(LockMode::Intent),
            LockMode::Exclusive
        );
        assert_eq!(
            LockMode::Intent.merge(LockMode::Shared),
            LockMode::Exclusive
        );
    }
}
