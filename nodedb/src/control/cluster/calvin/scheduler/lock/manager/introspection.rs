// SPDX-License-Identifier: BUSL-1.1

//! Read-only inspection of lock manager state: readiness checks, the keys
//! that block a request, and the test-only counters used to assert on
//! table and holder sizes.

use std::collections::BTreeMap;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::LockMode;
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::classify::KeyState;
use super::types::LockManager;

/// One key of a lock request that another transaction blocks: the mode it
/// asks, the mode its holders hold, the holders, and the waiters ahead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyContention {
    pub key: LockKey,
    pub requested: LockMode,
    pub held: LockMode,
    pub holders: Vec<TxnId>,
    pub waiters: Vec<(TxnId, LockMode)>,
}

impl LockManager {
    /// Every key of `keys` that `txn` cannot take now, with what blocks it.
    /// A request [`Self::try_acquire`] refuses names at least one.
    pub fn contention(&self, txn: TxnId, keys: &BTreeMap<LockKey, LockMode>) -> Vec<KeyContention> {
        keys.iter()
            .filter(|(key, mode)| self.classify(txn, key, **mode) == KeyState::Conflict)
            .filter_map(|(key, mode)| {
                let entry = self.table.get(key)?;
                Some(KeyContention {
                    key: key.clone(),
                    requested: *mode,
                    held: entry.mode,
                    holders: entry.holder_ids(),
                    waiters: entry.waiters.iter().copied().collect(),
                })
            })
            .collect()
    }

    /// Check whether a previously-blocked transaction is now ready.
    ///
    /// A transaction is ready when for every key in its key set, the key is
    /// either:
    /// - Not present in the lock table (free), or
    /// - Present in the lock table with `txn` among the current holders, in
    ///   any mode.
    ///
    /// This is called after `release` returns `txn_id` in the unblocked set.
    /// If `is_ready` returns `true`, the caller calls `acquire` again, which
    /// grants nothing new because the waiter was promoted.
    pub fn is_ready(&self, txn: TxnId, keys: &BTreeMap<LockKey, LockMode>) -> bool {
        keys.keys().all(|key| {
            match self.table.get(key) {
                None => true,                    // key is free
                Some(entry) => entry.holds(txn), // txn is a current holder
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
            .map(|e| e.holders.iter().filter(|(h, _)| h.is_reservation()).count())
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

    /// Watches filed by `try_acquire_or_watch` since creation.
    #[cfg(test)]
    pub fn watch_registrations(&self) -> u64 {
        self.watch_registrations
    }

    /// Watches still filed, over every key.
    #[cfg(test)]
    pub fn watcher_count(&self) -> usize {
        self.watchers.values().map(Vec::len).sum()
    }

    /// Panic unless the waiter bookkeeping matches the lock table.
    ///
    /// - Every waiter of an entry has the key in its `waiting` set, once.
    /// - Every key of a `waiting` set names the txn among its waiters.
    /// - A txn is in `waiting` iff it is in `pending_keys`.
    /// - A blocked txn holds or waits on every key of its request.
    /// - No entry waits without a holder, and no txn holds and waits on one key.
    #[cfg(test)]
    pub fn assert_consistent(&self) {
        for (key, entry) in &self.table {
            assert!(
                !entry.holders.is_empty(),
                "{key:?} has no holder but stays in the table"
            );
            for (waiter, _) in &entry.waiters {
                assert!(
                    self.is_waiting(*waiter, key),
                    "{waiter:?} queues on {key:?} unrecorded"
                );
                assert!(
                    !entry.holds(*waiter),
                    "{waiter:?} holds and waits on {key:?}"
                );
                let queued = entry.waiters.iter().filter(|(w, _)| w == waiter).count();
                assert_eq!(queued, 1, "{waiter:?} queues {queued} times on {key:?}");
            }
        }
        for (txn, keys) in &self.waiting {
            assert!(!keys.is_empty(), "{txn:?} has an empty waiting set");
            assert!(
                self.pending_keys.contains_key(txn),
                "{txn:?} waits without a request"
            );
            for key in keys {
                let queued = self
                    .table
                    .get(key)
                    .is_some_and(|entry| entry.waiters.iter().any(|(w, _)| w == txn));
                assert!(
                    queued,
                    "{txn:?} records a wait on {key:?} it does not queue"
                );
            }
        }
        for (txn, request) in &self.pending_keys {
            assert!(
                self.waiting.contains_key(txn),
                "{txn:?} is pending but waits on nothing"
            );
            for key in request.keys() {
                let held = self.table.get(key).is_some_and(|entry| entry.holds(*txn));
                assert!(
                    held || self.is_waiting(*txn, key),
                    "{txn:?} neither holds nor waits on {key:?} of its request"
                );
            }
        }
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

    fn keyset(names: &[&str]) -> BTreeMap<LockKey, LockMode> {
        names
            .iter()
            .map(|n| (key(n), LockMode::Exclusive))
            .collect()
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    /// A refused request names each key that blocks it, with the holders
    /// and the waiters ahead. A free key names nothing.
    #[test]
    fn contention_names_the_keys_that_block_a_request() {
        let mut lm = LockManager::new();
        let (holder, waiter, probe) = (txn(1, 0), txn(1, 1), txn(1, 2));
        lm.acquire(holder, keyset(&["x"]));
        lm.acquire(waiter, keyset(&["x"]));
        let request = keyset(&["x", "free"]);
        assert!(!lm.try_acquire(probe, request.clone()));
        assert_eq!(
            lm.contention(probe, &request),
            vec![KeyContention {
                key: key("x"),
                requested: LockMode::Exclusive,
                held: LockMode::Exclusive,
                holders: vec![holder],
                waiters: vec![(waiter, LockMode::Exclusive)],
            }]
        );
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
