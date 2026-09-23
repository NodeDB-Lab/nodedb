// SPDX-License-Identifier: BUSL-1.1

//! Exclusive lock acquisition and waiter queueing.

use std::collections::{BTreeSet, VecDeque};

use smallvec::smallvec;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{
    AcquireOutcome, LockEntry, LockMode,
};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::{ExclusiveWait, LockManager};

impl LockManager {
    /// Attempt to acquire **exclusive** locks on all keys for `txn`.
    ///
    /// If every key is free, already held exclusively by `txn` (promoted from
    /// waiter), or held **shared solely by `txn`** (a reservation this txn placed
    /// earlier, now upgraded in place to exclusive), records `txn` as sole holder
    /// of each key and returns [`AcquireOutcome::Ready`].
    ///
    /// Otherwise the whole key set is classified under the WOUND-WAIT discipline
    /// (see [`Self::wound_or_block`]).  `TxnId` order (`(epoch, position)`) is
    /// the replicated total order, so "older" means a smaller id and the
    /// decision is a pure function of the lock table plus the replicated ids —
    /// every replica computes it identically:
    /// - Any conflicting holder is **exclusive** → `txn` waits (an exclusive
    ///   holder is executing/applied work and is never wounded).
    /// - All conflicting holders are **shared** reservations and `txn` is older
    ///   than every one of them → **wound** them all (revoke to plain OCC, no
    ///   notification) and take every key. Wounding is silent.
    /// - Otherwise (`txn` younger than some conflicting shared holder) → wait.
    ///
    /// On the wait path `txn` is enqueued as an exclusive waiter on every
    /// conflicting key and holds none; its key set is stored in `pending_keys`
    /// so `release` can promote it atomically when all keys become available.
    /// The whole key set is evaluated before any mutation, so acquisition stays
    /// all-keys-or-none — the manager never partially wounds and then blocks.
    pub fn acquire(&mut self, txn: TxnId, keys: BTreeSet<LockKey>) -> AcquireOutcome {
        // First pass: determine whether any key is held by a *different* txn.
        // A key already held exclusively by `txn` (promoted via release) or held
        // shared solely by `txn` (an earlier reservation) counts as available —
        // the former is a no-op re-acquire, the latter a self-upgrade to exclusive.
        let all_available = keys.iter().all(|k| {
            self.table.get(k).is_none_or(|entry| {
                entry.held_exclusively_by(txn) || entry.held_shared_solely_by(txn)
            })
        });

        if all_available {
            // Acquire all keys.  For keys not yet in the table (free), insert a
            // new exclusive entry.  For keys already held by this txn (promoted
            // waiter), leave the entry unchanged — the waiter queue is intact.
            for key in &keys {
                match self.table.get_mut(key) {
                    None => {
                        self.table.insert(
                            key.clone(),
                            LockEntry {
                                mode: LockMode::Exclusive,
                                holders: smallvec![txn],
                                waiters: VecDeque::new(),
                            },
                        );
                    }
                    Some(entry) => {
                        // A key this txn already holds shared-solely is upgraded
                        // to exclusive in place (holders is exactly `[txn]`, so no
                        // holder change and any waiter queue stays intact). A key
                        // already held exclusively by `txn` is left unchanged.
                        if entry.held_shared_solely_by(txn) {
                            entry.mode = LockMode::Exclusive;
                        }
                    }
                }
            }
            // Move out of pending (if the txn was previously blocked on this
            // same key set) and into held_locks.
            self.pending_keys.remove(&txn);
            self.held_locks.insert(txn, keys);
            return AcquireOutcome::Ready;
        }

        // A conflict exists. Classify the WHOLE key set before mutating so the
        // wound / block decision is atomic (never partially wound then block).
        match self.wound_or_block(txn, &keys) {
            ExclusiveWait::Wound => {
                // Take every key exclusively. A conflicting shared entry has its
                // holders revoked (the wounded readers, all younger than `txn`,
                // degrade to plain OCC); `txn` becomes the sole holder while any
                // existing waiters remain queued behind it.
                for key in &keys {
                    match self.table.get_mut(key) {
                        Some(entry) => {
                            entry.mode = LockMode::Exclusive;
                            entry.holders.clear();
                            entry.holders.push(txn);
                        }
                        None => {
                            self.table.insert(
                                key.clone(),
                                LockEntry {
                                    mode: LockMode::Exclusive,
                                    holders: smallvec![txn],
                                    waiters: VecDeque::new(),
                                },
                            );
                        }
                    }
                }
                self.pending_keys.remove(&txn);
                self.held_locks.insert(txn, keys);
                AcquireOutcome::Ready
            }
            ExclusiveWait::Block => {
                // Enqueue as an exclusive waiter on every key held by a
                // different txn.
                for key in &keys {
                    if let Some(entry) = self.table.get_mut(key) {
                        // No conflict on this key means it is held solely by `txn`
                        // (a shared reservation to be upgraded, or an exclusive
                        // re-acquire) — leave it untouched; `txn` keeps the key and
                        // upgrades it once its conflicting keys are free. Same
                        // predicate as the `all_available` check above.
                        if entry.held_exclusively_by(txn) || entry.held_shared_solely_by(txn) {
                            continue;
                        }
                        // Real conflict on this key. If `txn` also holds it shared
                        // (an upgrade that must wait behind an OLDER shared holder),
                        // drop its own shared hold — degrading that read to plain
                        // OCC, never worse than today — so the key can drain to
                        // empty and normal promotion can grant `txn` the exclusive
                        // lock later. Without this, `txn` would occupy the key
                        // forever and its own exclusive request could never fire.
                        entry.holders.retain(|h| *h != txn);
                        if !entry.has_waiter(txn) {
                            entry.waiters.push_back((txn, LockMode::Exclusive));
                        }
                    }
                    // Free keys: no entry exists; the txn acquires them on the
                    // re-acquire path after all conflicting keys are released.
                }
                // Store the full key set so that release can promote this txn
                // atomically once all its keys become available.
                self.pending_keys.insert(txn, keys);
                AcquireOutcome::Blocked
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

    fn keyset(names: &[&str]) -> BTreeSet<LockKey> {
        names.iter().map(|n| key(n)).collect()
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    #[test]
    fn acquire_free_keys_returns_ready() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let outcome = lm.acquire(t, keyset(&["a", "b"]));
        assert_eq!(outcome, AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 2);
    }

    #[test]
    fn acquire_held_key_returns_blocked_and_enqueues_waiter() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);
        lm.acquire(t1, keyset(&["x"]));

        let outcome = lm.acquire(t2, keyset(&["x"]));
        assert_eq!(outcome, AcquireOutcome::Blocked);

        // t2 should be in the waiter queue for "x".
        assert!(lm.table.get(&key("x")).unwrap().has_waiter(t2));
    }

    #[test]
    fn exclusive_waits_on_exclusive() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Blocked);
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn shared_reservation_self_upgrades_to_exclusive() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);

        assert_eq!(lm.acquire_shared(t, key("k")), AcquireOutcome::Ready);
        // The txn re-acquires its own shared reservation exclusively — this must
        // NOT self-deadlock by blocking on its own held key.
        assert_eq!(
            lm.acquire(t, keyset(&["k"])),
            AcquireOutcome::Ready,
            "self-upgrade from shared to exclusive must not block"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t);
    }

    #[test]
    fn self_upgrade_with_other_shared_holder_blocks_or_wounds() {
        // T_old is older than T_young: T_old's self-upgrade must wound T_young.
        let mut lm = LockManager::new();
        let t_old = txn(1, 0);
        let t_young = txn(1, 1);

        assert_eq!(lm.acquire_shared(t_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t_young, key("k")), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(t_old, keyset(&["k"])),
            AcquireOutcome::Ready,
            "the older self-upgrader wounds the younger shared holder"
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t_old);

        // Symmetric case: the YOUNGER of the two self-upgrades and must block.
        let mut lm = LockManager::new();
        let t_old = txn(1, 0);
        let t_young = txn(1, 1);

        assert_eq!(lm.acquire_shared(t_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t_young, key("k")), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(t_young, keyset(&["k"])),
            AcquireOutcome::Blocked,
            "the younger self-upgrader must wait behind the older shared holder"
        );
        // t_young drops its own shared hold (degrading to plain OCC) so the key
        // can drain to empty and its exclusive request can later be promoted;
        // t_old remains the sole shared holder, and t_young is enqueued as an
        // exclusive waiter rather than left stuck as a non-waiting holder.
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&t_old));
        assert!(!entry.holders.contains(&t_young));
        assert!(entry.has_waiter(t_young));

        // Once t_old releases, t_young is promoted to sole exclusive holder.
        let unblocked = lm.release(t_old);
        assert!(unblocked.contains(&t_young));
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], t_young);
    }

    #[test]
    fn self_upgrade_mixed_with_conflict_on_other_key() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let u = txn(1, 1);

        // T reserves K1 shared; U holds K2 exclusively.
        assert_eq!(lm.acquire_shared(t, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(u, keyset(&["k2"])), AcquireOutcome::Ready);

        // T tries to take both keys exclusively: K2 conflicts with U, so T must
        // block on the whole set — and critically must NOT self-deadlock on K1.
        assert_eq!(
            lm.acquire(t, keyset(&["k1", "k2"])),
            AcquireOutcome::Blocked,
            "conflict on k2 blocks the whole set"
        );

        // After U releases K2, T's re-acquire succeeds and upgrades K1 in place.
        lm.release(u);
        assert_eq!(
            lm.acquire(t, keyset(&["k1", "k2"])),
            AcquireOutcome::Ready,
            "once k2 frees up, t acquires both keys exclusively"
        );
        let k1 = lm.table.get(&key("k1")).unwrap();
        assert_eq!(k1.mode, LockMode::Exclusive);
        assert_eq!(k1.holders.len(), 1);
        assert_eq!(k1.holders[0], t);
        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert_eq!(k2.holders.len(), 1);
        assert_eq!(k2.holders[0], t);
    }

    #[test]
    fn two_non_conflicting_both_dispatch_immediately() {
        let mut lm = LockManager::new();

        let txn1 = TxnId::new(1, 0);
        let txn2 = TxnId::new(1, 1);

        let keys1: BTreeSet<LockKey> = [LockKey::Surrogate {
            collection: Arc::from("coll"),
            surrogate: 1,
        }]
        .into();
        let keys2: BTreeSet<LockKey> = [LockKey::Surrogate {
            collection: Arc::from("coll"),
            surrogate: 2,
        }]
        .into();

        let o1 = lm.acquire(txn1, keys1);
        let o2 = lm.acquire(txn2, keys2);

        assert_eq!(o1, AcquireOutcome::Ready, "txn1 should be ready");
        assert_eq!(
            o2,
            AcquireOutcome::Ready,
            "txn2 should be ready (disjoint keys)"
        );
    }

    #[test]
    fn many_mixed_deterministic_dispatch_order() {
        let mut lm = LockManager::new();
        let mut dispatched: Vec<TxnId> = Vec::new();

        let pairs = [(2, 0), (1, 1), (3, 0), (1, 0), (2, 1)];
        for (epoch, pos) in pairs {
            let tid = TxnId::new(epoch, pos);
            let keys: BTreeSet<LockKey> = [LockKey::Surrogate {
                collection: Arc::from(format!("c_{epoch}_{pos}")),
                surrogate: epoch as u32 * 10 + pos,
            }]
            .into();
            let outcome = lm.acquire(tid, keys);
            if outcome == AcquireOutcome::Ready {
                dispatched.push(tid);
            }
        }

        assert_eq!(
            dispatched.len(),
            5,
            "all non-conflicting txns should be ready"
        );

        let mut expected = pairs.map(|(e, p)| TxnId::new(e, p)).to_vec();
        expected.sort();
        let mut sorted_dispatched = dispatched.clone();
        sorted_dispatched.sort();
        assert_eq!(sorted_dispatched, expected);
    }
}
