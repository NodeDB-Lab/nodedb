// SPDX-License-Identifier: BUSL-1.1

//! Read-only inspection of lock manager state — readiness checks and the
//! test-only counters used to assert on table/holder sizes.

use std::collections::BTreeSet;

use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::LockManager;

impl LockManager {
    /// Check whether a previously-blocked transaction is now ready.
    ///
    /// A transaction is ready when for every key in its key set, the key is
    /// either:
    /// - Not present in the lock table (free), or
    /// - Present in the lock table with `txn` among the current holders
    ///   (shared or exclusive).
    ///
    /// This is called after `release` returns `txn_id` in the unblocked set.
    /// If `is_ready` returns `true`, the caller calls `acquire` again which
    /// will succeed on the all-available path (because the waiter was promoted).
    pub fn is_ready(&self, txn: TxnId, keys: &BTreeSet<LockKey>) -> bool {
        keys.iter().all(|key| {
            match self.table.get(key) {
                None => true,                                // key is free
                Some(entry) => entry.holders.contains(&txn), // txn is a current holder
            }
        })
    }

    /// Number of holders of `key` that hold it as a Calvin read reservation
    /// (a `TxnId` in the reservation position band), or 0 when the key is
    /// unlocked or held only by non-reservation transactions. Used to observe
    /// reservation install/release from outside the scheduler.
    pub fn reservation_holder_count(&self, key: &LockKey) -> usize {
        self.table
            .get(key)
            .map(|e| e.holders.iter().filter(|h| h.is_reservation()).count())
            .unwrap_or(0)
    }

    /// Number of currently-held locks (entries in the lock table).
    #[cfg(test)]
    pub fn lock_count(&self) -> usize {
        self.table.len()
    }

    /// Number of transactions currently holding at least one lock.
    #[cfg(test)]
    pub fn holder_count(&self) -> usize {
        self.held_locks.len()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn key(name: &str) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from(name),
            surrogate: 1,
        }
    }

    fn keyset(names: &[&str]) -> BTreeSet<LockKey> {
        names.iter().map(|n| key(n)).collect()
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    #[test]
    fn is_ready_returns_true_when_all_keys_free_or_self_at_front() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x", "y"]));
        lm.acquire(t2, keyset(&["x", "y"]));

        // t2 is not ready while t1 holds.
        assert!(!lm.is_ready(t2, &keyset(&["x", "y"])));

        // Release t1 — t2 becomes holder on both keys.
        lm.release(t1);
        // After release, t2 is promoted to holder on both keys.
        assert!(lm.is_ready(t2, &keyset(&["x", "y"])));
    }
}
