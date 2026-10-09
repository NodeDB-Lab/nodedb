// SPDX-License-Identifier: BUSL-1.1

//! The lock table struct and the small per-entry predicates the
//! acquire/release paths share.

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
/// - `held_locks`: keys of transactions that hold ALL their requested keys and
///   are actively executing (i.e. dispatched to the Data Plane).
/// - `pending_keys`: the full request of a transaction that waits for at least
///   one key. It already holds every other key of the request. When `release`
///   grants its last waited key, the entry moves from `pending_keys` to
///   `held_locks`.
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
    /// Full requests of **blocked** (not-yet-dispatched) txns.  Populated when
    /// `acquire` returns `Blocked`; moved to `held_locks` when the last waited
    /// key is granted on the promotion path inside `release`.
    pub(in crate::control::cluster::calvin::scheduler::lock) pending_keys:
        BTreeMap<TxnId, BTreeMap<LockKey, LockMode>>,
}

/// How a requester resolves a request that conflicts on at least one key.
pub(super) enum ConflictResolution {
    /// Every conflicting holder is a younger read reservation and no other
    /// transaction waits on a conflicting key: revoke those reservations and
    /// take every key.
    Wound,
    /// The requester waits: a conflicting holder is a transaction, an older
    /// reservation, or an earlier waiter queues ahead of it.
    Block,
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
    /// Whether `txn` is the only holder of this entry.
    pub(super) fn held_solely_by(&self, txn: TxnId) -> bool {
        self.holders.len() == 1 && self.holders[0] == txn
    }

    /// Whether `txn` is already enqueued as a waiter on this entry.
    pub(super) fn has_waiter(&self, txn: TxnId) -> bool {
        self.waiters.iter().any(|(w, _)| *w == txn)
    }
}
