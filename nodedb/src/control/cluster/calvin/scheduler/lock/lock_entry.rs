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
/// | held \ requested | Intent | Shared | Exclusive |
/// |------------------|--------|--------|-----------|
/// | Intent           | yes    | no     | no        |
/// | Shared           | no     | yes    | no        |
/// | Exclusive        | no     | no     | no        |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// A row writer's hold on its collection key. Compatible only with other
    /// `Intent` holders, so row writers run together and exclude a predicate
    /// reader or a collection-wide writer.
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
            (LockMode::Intent, LockMode::Intent) | (LockMode::Shared, LockMode::Shared) => true,
            (LockMode::Intent, LockMode::Shared | LockMode::Exclusive)
            | (LockMode::Shared, LockMode::Intent | LockMode::Exclusive)
            | (LockMode::Exclusive, LockMode::Intent | LockMode::Shared | LockMode::Exclusive) => {
                false
            }
        }
    }

    /// The one mode that covers both `self` and `other` for one transaction.
    ///
    /// Equal modes merge to themselves. Any other pair merges to `Exclusive`:
    /// `Shared` plus `Intent` means the transaction both reads the whole
    /// collection and writes rows of it.
    pub fn merge(self, other: LockMode) -> LockMode {
        if self == other {
            self
        } else {
            LockMode::Exclusive
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

// ── LockEntry ─────────────────────────────────────────────────────────────────

/// Per-key lock state.
///
/// Invariants maintained by [`LockManager`](super::manager::LockManager):
/// - Every holder holds the key in `mode`. Two modes never mix, because no
///   two distinct modes are compatible.
/// - `Exclusive` entries have exactly one holder.
/// - `waiters` is FIFO; each waiter carries the mode it requested so that
///   promotion on release is mode-aware.
pub(super) struct LockEntry {
    /// The mode the current holders hold this lock under.
    pub(super) mode: LockMode,
    /// The transactions currently holding this lock. Exclusive: exactly one;
    /// Shared or Intent: one or more.
    pub(super) holders: SmallVec<[TxnId; 2]>,
    /// Transactions waiting for this lock, in FIFO order, each tagged with the
    /// mode it requested.
    pub(super) waiters: VecDeque<(TxnId, LockMode)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [LockMode; 3] = [LockMode::Intent, LockMode::Shared, LockMode::Exclusive];

    #[test]
    fn compatibility_matrix() {
        assert!(LockMode::Intent.compatible(LockMode::Intent));
        assert!(LockMode::Shared.compatible(LockMode::Shared));
        assert!(!LockMode::Intent.compatible(LockMode::Shared));
        assert!(!LockMode::Shared.compatible(LockMode::Intent));
        for mode in ALL {
            assert!(!mode.compatible(LockMode::Exclusive));
            assert!(!LockMode::Exclusive.compatible(mode));
        }
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
