// SPDX-License-Identifier: BUSL-1.1

//! The lock table struct, its internal decision enums, and the small
//! per-entry predicates the acquire/release paths share.

use std::collections::{BTreeMap, BTreeSet};

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{LockEntry, LockMode};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

/// Deterministic Calvin lock manager for one vshard.
///
/// Manages an in-memory lock table keyed by [`LockKey`].  The table is held in
/// a `BTreeMap` so iteration is always deterministic.
///
/// # Key sets tracked per transaction
///
/// - `held_locks`: key sets for transactions that are a current holder on ALL
///   their keys and are actively executing (i.e. dispatched to the Data Plane).
/// - `pending_keys`: key sets for transactions that are blocked waiting for at
///   least one key.  When `release` promotes a blocked txn to holder on every
///   one of its keys, the entry moves from `pending_keys` to `held_locks`.
pub struct LockManager {
    /// Per-key lock entries.  Uses `BTreeMap` for deterministic iteration.
    /// Visible to the whole `lock` module so the sibling `reap` module can
    /// scan entries for lease-expired reservations without a public accessor.
    pub(in crate::control::cluster::calvin::scheduler::lock) table: BTreeMap<LockKey, LockEntry>,
    /// Per-transaction set of currently held keys for **dispatched** txns.
    /// Used by `release` to iterate the key set without a full table scan.
    /// Visible to the whole `lock` module — see `table`.
    pub(in crate::control::cluster::calvin::scheduler::lock) held_locks:
        BTreeMap<TxnId, BTreeSet<LockKey>>,
    /// Key sets for **blocked** (not-yet-dispatched) txns.  Populated when
    /// `acquire` returns `Blocked`; cleared (moved to `held_locks`) when all
    /// keys have been acquired on the promotion path inside `release`.
    pub(super) pending_keys: BTreeMap<TxnId, BTreeSet<LockKey>>,
}

/// Outcome of inspecting a single key during [`LockManager::acquire_shared`].
pub(super) enum SharedGrant {
    /// The shared lock was granted (key was free or already held shared).
    Granted,
    /// The key is held exclusively by another txn; the request was enqueued.
    Blocked,
}

/// The wound-wait decision for an exclusive requester that meets a conflict.
pub(super) enum ExclusiveWait {
    /// Every conflicting holder is a shared reservation and the requester is
    /// older than all of them: wound (revoke) those shared holders and proceed.
    Wound,
    /// The requester must block: a conflicting holder is exclusive, or the
    /// requester is younger than some conflicting shared holder.
    Block,
}

/// The waiters promoted off one key when its holders drained, together with the
/// action to take on the now-empty entry.
pub(super) enum Promotion {
    /// No waiters remained; the entry should be removed entirely.
    Freed,
    /// These waiters were installed as the new holders.
    Promoted(Vec<TxnId>),
}

impl LockManager {
    /// Create an empty lock manager.
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
            held_locks: BTreeMap::new(),
            pending_keys: BTreeMap::new(),
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockEntry {
    /// Whether this entry is held exclusively by exactly `txn` (the self
    /// re-acquire case on the exclusive path).
    pub(super) fn held_exclusively_by(&self, txn: TxnId) -> bool {
        self.mode == LockMode::Exclusive && self.holders.len() == 1 && self.holders[0] == txn
    }

    /// Whether this entry is held **shared** by exactly `txn` and no one else —
    /// the self-upgrade case: `txn` may take the key exclusively because it is
    /// the sole current holder.
    pub(super) fn held_shared_solely_by(&self, txn: TxnId) -> bool {
        self.mode == LockMode::Shared && self.holders.len() == 1 && self.holders[0] == txn
    }

    /// Whether `txn` is already enqueued as a waiter on this entry.
    pub(super) fn has_waiter(&self, txn: TxnId) -> bool {
        self.waiters.iter().any(|(w, _)| *w == txn)
    }
}
