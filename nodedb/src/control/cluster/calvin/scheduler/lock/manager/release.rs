// SPDX-License-Identifier: BUSL-1.1

//! Lock release and FIFO, mode-aware waiter promotion.

use std::collections::BTreeSet;

use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::LockManager;

impl LockManager {
    /// Release every lock `txn` holds and every wait it queues.
    ///
    /// `txn` leaves the holders and the waiters of every key it held or
    /// requested. Each such key then promotes its waiters (see
    /// [`Self::promote_waiters`]). A waiter that now holds ALL its requested
    /// keys moves from `pending_keys` to `held_locks`.
    ///
    /// Returns the fully-promoted `TxnId`s in ascending order. The caller
    /// dispatches them.
    pub fn release(&mut self, txn: TxnId) -> Vec<TxnId> {
        let mut keys: BTreeSet<LockKey> = self.held_locks.remove(&txn).unwrap_or_default();
        if let Some(pending) = self.pending_keys.remove(&txn) {
            keys.extend(pending.into_keys());
        }
        let waited = self.waiting.remove(&txn).unwrap_or_default();

        let mut newly_promoted: BTreeSet<TxnId> = BTreeSet::new();
        for key in &keys {
            if let Some(entry) = self.table.get_mut(key) {
                entry.drop_holder(txn);
                // A holder never waits on its own key, so only waited keys
                // scan the queue.
                if waited.contains(key) {
                    entry.waiters.retain(|(w, _)| *w != txn);
                }
                self.promote_waiters(key, &mut newly_promoted);
            }
            self.wake_watchers(key);
        }
        newly_promoted.into_iter().collect()
    }

    /// Wake and drop every write-admission watcher filed under `key`.
    /// `notify_one` stores a permit, so a watcher not yet asleep still wakes.
    pub(super) fn wake_watchers(&mut self, key: &LockKey) {
        if let Some(signals) = self.watchers.remove(key) {
            for signal in signals {
                signal.notify_one();
            }
        }
    }

    /// Grant the front of `key`'s waiter queue while it is compatible with the
    /// current holders, in FIFO order.
    ///
    /// A key with no holders grants its first waiter in that waiter's mode,
    /// then every following waiter compatible with it. A key with neither
    /// holders nor waiters is removed. Each grant drops `key` from the
    /// waiter's `waiting` set. A waiter whose set empties holds its whole
    /// request: it moves into `held_locks` and joins `newly_promoted`.
    pub(super) fn promote_waiters(&mut self, key: &LockKey, newly_promoted: &mut BTreeSet<TxnId>) {
        let mut granted: Vec<TxnId> = Vec::new();
        let Some(entry) = self.table.get_mut(key) else {
            return;
        };
        while let Some(&(next, mode)) = entry.waiters.front() {
            if !entry.holders.is_empty() && !entry.mode.compatible(mode) {
                break;
            }
            entry.waiters.pop_front();
            entry.hold(next, mode);
            granted.push(next);
        }
        if entry.holders.is_empty() && entry.waiters.is_empty() {
            self.table.remove(key);
        }

        for next in granted {
            #[cfg(test)]
            {
                self.promotion_steps += 1;
            }
            let Some(waited) = self.waiting.get_mut(&next) else {
                continue;
            };
            waited.remove(key);
            if !waited.is_empty() {
                continue;
            }
            self.waiting.remove(&next);
            if let Some(request) = self.pending_keys.remove(&next) {
                self.finish_ready(next, request);
                newly_promoted.insert(next);
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::collections::BTreeMap;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock::lock_entry::{AcquireOutcome, LockMode};

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

    #[test]
    fn release_returns_unblocked_waiter_ids() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x"]));
        lm.acquire(t2, keyset(&["x"]));

        let unblocked = lm.release(t1);
        assert!(unblocked.contains(&t2));
    }

    #[test]
    fn autocommit_holder_release_promotes_and_returns_scheduler_waiter() {
        // Mirrors the write-admission fast path: an autocommit-band holder takes
        // an uncontended key, a normal-band scheduler txn then blocks behind it,
        // and the holder's release promotes that scheduler txn AND returns its id
        // — the value the fast-path guard forwards to the scheduler on drop
        // (previously discarded, stranding the promoted txn as a zombie holder).
        let mut lm = LockManager::new();
        let autocommit = txn(TxnId::AUTOCOMMIT_EPOCH, 0);
        let scheduler_txn = txn(9, 0);

        assert!(
            lm.try_acquire(autocommit, keyset(&["k"])),
            "the fast-path holder takes the uncontended key"
        );
        assert_eq!(
            lm.acquire(scheduler_txn, keyset(&["k"])).outcome,
            AcquireOutcome::Blocked,
            "the scheduler txn queues behind the fast-path holder"
        );

        let promoted = lm.release(autocommit);
        assert_eq!(
            promoted,
            vec![scheduler_txn],
            "release must return the promoted scheduler waiter"
        );
        assert!(
            lm.is_ready(scheduler_txn, &keyset(&["k"])),
            "the promoted scheduler txn is now holder of the freed key"
        );
    }

    /// Keys `k0` through `k{count - 1}`, each requested `Exclusive`.
    fn numbered(count: u32) -> BTreeMap<LockKey, LockMode> {
        (0..count)
            .map(|n| (key(&format!("k{n}")), LockMode::Exclusive))
            .collect()
    }

    const WIDE: u32 = 512;

    /// One waiter on WIDE keys, each freed by its own release: each grant
    /// runs once, whatever the request width.
    #[test]
    fn promotion_work_is_linear_when_keys_free_one_release_at_a_time() {
        let mut lm = LockManager::new();
        for n in 0..WIDE {
            let holder = txn(1, n);
            let request = BTreeMap::from([(key(&format!("k{n}")), LockMode::Exclusive)]);
            assert_eq!(lm.acquire(holder, request).outcome, AcquireOutcome::Ready);
        }
        let waiter = txn(2, 0);
        assert_eq!(
            lm.acquire(waiter, numbered(WIDE)).outcome,
            AcquireOutcome::Blocked
        );
        lm.assert_consistent();
        for n in 0..WIDE {
            let promoted = lm.release(txn(1, n));
            lm.assert_consistent();
            if n + 1 < WIDE {
                assert!(
                    promoted.is_empty(),
                    "the waiter still waits after release {n}"
                );
            } else {
                assert_eq!(promoted, vec![waiter]);
            }
        }
        assert_eq!(lm.promotion_steps, u64::from(WIDE));
        assert!(lm.is_ready(waiter, &numbered(WIDE)));
    }

    /// One release that frees all WIDE keys of a waiter runs one grant per key.
    #[test]
    fn promotion_work_is_linear_when_one_release_frees_every_key() {
        let mut lm = LockManager::new();
        let (holder, waiter) = (txn(1, 0), txn(2, 0));
        assert_eq!(
            lm.acquire(holder, numbered(WIDE)).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(waiter, numbered(WIDE)).outcome,
            AcquireOutcome::Blocked
        );
        lm.assert_consistent();
        assert_eq!(lm.release(holder), vec![waiter]);
        lm.assert_consistent();
        assert_eq!(lm.promotion_steps, u64::from(WIDE));
        assert!(lm.is_ready(waiter, &numbered(WIDE)));
    }

    #[test]
    fn release_preserves_fifo_waiter_order() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        let t3 = txn(1, 2);
        lm.acquire(t1, keyset(&["x"]));
        lm.acquire(t2, keyset(&["x"]));
        lm.acquire(t3, keyset(&["x"]));

        // Release t1 — t2 should become holder (FIFO).
        lm.release(t1);
        let holder = lm.table.get(&key("x")).unwrap().holder_ids()[0];
        assert_eq!(holder, t2);

        // Release t2 — t3 should become holder.
        lm.release(t2);
        let holder = lm.table.get(&key("x")).unwrap().holder_ids()[0];
        assert_eq!(holder, t3);
    }

    #[test]
    fn multi_key_txn_releases_all_atomically() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        lm.acquire(t1, keyset(&["a", "b", "c"]));
        assert_eq!(lm.lock_count(), 3);

        lm.release(t1);
        assert_eq!(lm.lock_count(), 0);
        assert_eq!(lm.holder_count(), 0);
    }

    #[test]
    fn release_promotes_shared_run_together() {
        let mut lm = LockManager::new();
        let holder = txn(1, 0);
        let s1 = txn(2, 0);
        let s2 = txn(2, 1);

        // Exclusive holder, two shared waiters queued behind it.
        assert_eq!(
            lm.acquire(holder, keyset(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(s1, key("k")).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire_shared(s2, key("k")).outcome,
            AcquireOutcome::Blocked
        );

        // Releasing the exclusive holder promotes the whole run of shared
        // waiters together.
        let promoted = lm.release(holder);
        assert!(promoted.contains(&s1));
        assert!(promoted.contains(&s2));

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holds(s1));
        assert!(entry.holds(s2));
    }

    #[test]
    fn release_promotes_single_exclusive_waiter() {
        let mut lm = LockManager::new();
        let holder = txn(1, 0);
        let x1 = txn(2, 0);
        let x2 = txn(2, 1);

        assert_eq!(
            lm.acquire(holder, keyset(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(x1, keyset(&["k"])).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(x2, keyset(&["k"])).outcome,
            AcquireOutcome::Blocked
        );

        // Only the single leading exclusive waiter is promoted.
        let promoted = lm.release(holder);
        assert_eq!(promoted, vec![x1]);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holder_ids()[0], x1);
        // x2 is still waiting behind x1.
        assert!(entry.has_waiter(x2));
    }

    #[test]
    fn multi_holder_release() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(
            lm.acquire_shared(t1, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(t2, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(lm.lock_count(), 1);

        // Releasing one shared holder leaves the other holding the key.
        lm.release(t1);
        let entry = lm.table.get(&key("k")).unwrap();
        assert!(!entry.holds(t1));
        assert!(entry.holds(t2));
        assert_eq!(lm.lock_count(), 1);

        // Releasing the last shared holder frees the key.
        lm.release(t2);
        assert_eq!(lm.lock_count(), 0);
    }

    #[test]
    fn two_conflicting_second_dispatches_after_first_completes() {
        let mut lm = LockManager::new();

        let txn1 = TxnId::new(1, 0);
        let txn2 = TxnId::new(1, 1);
        let shared_key: BTreeMap<LockKey, LockMode> = [(
            LockKey::Surrogate {
                collection: Arc::from("coll"),
                surrogate: 42,
            },
            LockMode::Exclusive,
        )]
        .into();

        let o1 = lm.acquire(txn1, shared_key.clone()).outcome;
        assert_eq!(o1, AcquireOutcome::Ready);

        let o2 = lm.acquire(txn2, shared_key.clone()).outcome;
        assert_eq!(o2, AcquireOutcome::Blocked);

        let unblocked = lm.release(txn1);
        assert!(unblocked.contains(&txn2));

        assert!(lm.is_ready(txn2, &shared_key));
    }

    #[test]
    fn cross_epoch_raw_blocks_correctly() {
        let mut lm = LockManager::new();

        let txn_n = TxnId::new(1, 0);
        let txn_n1 = TxnId::new(2, 0);

        let key_k: BTreeMap<LockKey, LockMode> = [(
            LockKey::Surrogate {
                collection: Arc::from("orders"),
                surrogate: 100,
            },
            LockMode::Exclusive,
        )]
        .into();

        let o1 = lm.acquire(txn_n, key_k.clone()).outcome;
        assert_eq!(o1, AcquireOutcome::Ready);

        let o2 = lm.acquire(txn_n1, key_k.clone()).outcome;
        assert_eq!(o2, AcquireOutcome::Blocked);

        let unblocked = lm.release(txn_n);
        assert!(unblocked.contains(&txn_n1));
        assert!(lm.is_ready(txn_n1, &key_k));
    }
}
