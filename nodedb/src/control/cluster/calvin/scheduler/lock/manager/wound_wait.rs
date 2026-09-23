// SPDX-License-Identifier: BUSL-1.1

//! Shared-lock reservations and the wound-wait conflict resolution used by
//! exclusive acquisition.

use std::collections::btree_map::Entry;
use std::collections::{BTreeSet, VecDeque};

use smallvec::smallvec;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{
    AcquireOutcome, LockEntry, LockMode,
};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::{ExclusiveWait, LockManager, SharedGrant};

impl LockManager {
    /// Classify the wound-wait decision for an exclusive requester `txn` over
    /// `keys`, given that at least one key already conflicts.
    ///
    /// Pure read over the lock table: any exclusive conflict forces
    /// [`ExclusiveWait::Block`] (an exclusive holder is never wounded, so a mix
    /// of exclusive and shared conflicts blocks too). Otherwise all conflicting
    /// holders are shared reservations, and `txn` wounds them only when it is
    /// older than every one (`txn < h` for each conflicting shared holder `h`);
    /// if it is younger than any, it blocks. A key held only by `txn` itself is
    /// not a conflict.
    pub(super) fn wound_or_block(&self, txn: TxnId, keys: &BTreeSet<LockKey>) -> ExclusiveWait {
        let mut shared_conflicts: Vec<TxnId> = Vec::new();
        for key in keys {
            if let Some(entry) = self.table.get(key) {
                match entry.mode {
                    LockMode::Exclusive => {
                        // Exclusive entries have exactly one holder; a holder
                        // other than `txn` is an exclusive conflict.
                        if !entry.holders.contains(&txn) {
                            return ExclusiveWait::Block;
                        }
                    }
                    LockMode::Shared => {
                        for holder in &entry.holders {
                            if *holder != txn {
                                shared_conflicts.push(*holder);
                            }
                        }
                    }
                }
            }
        }
        // Wound only when there is a shared conflict AND `txn` is older than
        // every conflicting shared holder; otherwise block. `shared_conflicts`
        // only ever holds *other* txns' shared holders (a key held shared solely
        // by `txn` never reaches here — it takes the self-upgrade path in
        // `acquire`), so an empty set here means every conflict was exclusive.
        if !shared_conflicts.is_empty() && shared_conflicts.iter().all(|holder| txn < *holder) {
            ExclusiveWait::Wound
        } else {
            ExclusiveWait::Block
        }
    }

    /// Attempt to acquire a **shared** lock on a single `key` for `txn`.
    ///
    /// - Key free → create a shared entry holding `txn`, return
    ///   [`AcquireOutcome::Ready`].
    /// - Key held shared → add `txn` to the holders, return
    ///   [`AcquireOutcome::Ready`].
    /// - Key held exclusively by another txn → enqueue `txn` as a shared waiter
    ///   (FIFO) and return [`AcquireOutcome::Blocked`].
    ///
    /// A shared request that meets an exclusive holder blocks FIFO for now;
    /// wound-wait priority resolution lands in a following change.
    pub fn acquire_shared(&mut self, txn: TxnId, key: LockKey) -> AcquireOutcome {
        // Inspect / mutate the entry via the `Entry` API (which takes the key by
        // value, sidestepping a get-then-insert borrow conflict) inside a scoped
        // borrow so the map-level bookkeeping below can re-borrow `self`.
        let grant = match self.table.entry(key.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(LockEntry {
                    mode: LockMode::Shared,
                    holders: smallvec![txn],
                    waiters: VecDeque::new(),
                });
                SharedGrant::Granted
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                if entry.mode == LockMode::Shared {
                    if !entry.holders.contains(&txn) {
                        entry.holders.push(txn);
                    }
                    SharedGrant::Granted
                } else {
                    // Held exclusively by another txn: block FIFO.
                    if !entry.has_waiter(txn) {
                        entry.waiters.push_back((txn, LockMode::Shared));
                    }
                    SharedGrant::Blocked
                }
            }
        };

        match grant {
            SharedGrant::Granted => {
                self.pending_keys.remove(&txn);
                self.held_locks.entry(txn).or_default().insert(key);
                AcquireOutcome::Ready
            }
            SharedGrant::Blocked => {
                let mut pending = BTreeSet::new();
                pending.insert(key);
                self.pending_keys.insert(txn, pending);
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
    fn shared_shared_compatible() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire_shared(t1, key("s")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t2, key("s")), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("s")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&t1));
        assert!(entry.holders.contains(&t2));
    }

    #[test]
    fn shared_blocks_exclusive() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire_shared(t1, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(t2, keyset(&["k"])),
            AcquireOutcome::Blocked,
            "an exclusive request must block behind a shared holder"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn exclusive_blocks_shared() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let t2 = txn(1, 1);

        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire_shared(t2, key("k")),
            AcquireOutcome::Blocked,
            "a shared request must block behind an exclusive holder"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(t2));
    }

    #[test]
    fn older_writer_wounds_shared() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2); // shared holder
        let t1 = txn(1, 1); // exclusive requester, older than t2

        assert_eq!(lm.acquire_shared(t2, key("k")), AcquireOutcome::Ready);
        // The older writer wounds the younger shared holder and proceeds.
        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert!(entry.holders.contains(&t1), "R is now the exclusive holder");
        assert!(
            !entry.holders.contains(&t2),
            "the wounded shared holder is gone"
        );
    }

    #[test]
    fn younger_writer_waits() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 1); // shared holder
        let t2 = txn(1, 2); // exclusive requester, younger than t1

        assert_eq!(lm.acquire_shared(t1, key("k")), AcquireOutcome::Ready);
        // The younger writer must not wound; it waits behind the shared holder.
        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Blocked);

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(entry.holders.contains(&t1), "the shared holder still holds");
        assert!(!entry.holders.contains(&t2), "R holds nothing");
        assert!(entry.has_waiter(t2), "R is enqueued as an exclusive waiter");
    }

    #[test]
    fn exclusive_waits_on_exclusive_regardless_of_age() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2); // exclusive holder (younger)
        let t1 = txn(1, 1); // exclusive requester (older)

        assert_eq!(lm.acquire(t2, keyset(&["k"])), AcquireOutcome::Ready);
        // An exclusive holder is NEVER wounded, even by an older writer.
        assert_eq!(lm.acquire(t1, keyset(&["k"])), AcquireOutcome::Blocked);

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(
            entry.holders.contains(&t2),
            "the exclusive holder is intact"
        );
        assert!(!entry.holders.contains(&t1));
        assert!(entry.has_waiter(t1));
    }

    #[test]
    fn multi_key_atomic_wound_takes_both() {
        let mut lm = LockManager::new();
        let s1 = txn(1, 5); // shared holder on k1, younger than R
        let s2 = txn(1, 6); // shared holder on k2, younger than R
        let r = txn(1, 1); // exclusive requester, older than both

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        assert_eq!(lm.acquire(r, keyset(&["k1", "k2"])), AcquireOutcome::Ready);

        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert!(entry.holders.contains(&r), "R holds {k}");
        }
        assert!(!lm.table.get(&key("k1")).unwrap().holders.contains(&s1));
        assert!(!lm.table.get(&key("k2")).unwrap().holders.contains(&s2));
    }

    #[test]
    fn multi_key_atomic_wait_holds_none() {
        let mut lm = LockManager::new();
        let s1 = txn(1, 5); // shared holder on k1, younger than R
        let s2 = txn(1, 0); // shared holder on k2, OLDER than R
        let r = txn(1, 1); // exclusive requester

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        // R is younger than the holder on k2, so it must wait on BOTH keys and
        // hold neither (all-or-nothing).
        assert_eq!(
            lm.acquire(r, keyset(&["k1", "k2"])),
            AcquireOutcome::Blocked
        );

        assert!(
            !lm.table.get(&key("k1")).unwrap().holders.contains(&r),
            "R holds no key"
        );
        assert!(!lm.table.get(&key("k2")).unwrap().holders.contains(&r));
        // The older shared holder on k2 is untouched.
        assert!(lm.table.get(&key("k2")).unwrap().holders.contains(&s2));
    }

    #[test]
    fn crossed_reservations_are_acyclic() {
        // T1 holds shared K1 and wants exclusive K2; T2 holds shared K2 and
        // wants exclusive K1. The older writer's exclusive acquire wounds the
        // younger's shared holding, breaking the cycle — no deadlock.
        let mut lm = LockManager::new();
        let t1 = txn(1, 1); // older
        let t2 = txn(1, 2); // younger

        assert_eq!(lm.acquire_shared(t1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(t2, key("k2")), AcquireOutcome::Ready);

        // T1 (older) acquires exclusive K2: wounds T2's shared holding and
        // proceeds.
        assert_eq!(lm.acquire(t1, keyset(&["k2"])), AcquireOutcome::Ready);

        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert!(k2.holders.contains(&t1), "the older writer proceeds");
        assert!(
            !k2.holders.contains(&t2),
            "the younger's reservation is wounded away"
        );
    }
}
