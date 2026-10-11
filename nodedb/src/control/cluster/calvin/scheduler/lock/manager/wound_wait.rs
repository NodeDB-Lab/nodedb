// SPDX-License-Identifier: BUSL-1.1

//! Shared-lock reservations and the wound-wait conflict resolution used by
//! acquisition.

use std::collections::BTreeMap;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{AcquireOutcome, LockMode};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::classify::KeyState;
use super::types::{ConflictResolution, LockManager};

impl LockManager {
    /// Decide how `txn` resolves a request with at least one conflicting key.
    ///
    /// Pure read over the lock table. `txn` wounds only when every conflicting
    /// key meets all of these:
    /// - nobody waits on the key, so no earlier request is passed;
    /// - every other holder holds the key `Shared`;
    /// - every other holder is a read reservation (`TxnId::is_reservation`)
    ///   younger than `txn`.
    ///
    /// A transaction holder is executing work and is never wounded, whatever
    /// its mode. Otherwise `txn` blocks.
    pub(super) fn resolve_conflict(
        &self,
        txn: TxnId,
        request: &BTreeMap<LockKey, LockMode>,
        states: &[KeyState],
    ) -> ConflictResolution {
        for (key, state) in request.keys().zip(states) {
            if *state != KeyState::Conflict {
                continue;
            }
            let Some(entry) = self.table.get(key) else {
                return ConflictResolution::Block;
            };
            if !entry.waiters.is_empty() || entry.mode != LockMode::Shared {
                return ConflictResolution::Block;
            }
            let mut others = entry.holders.iter().filter(|h| **h != txn).peekable();
            if others.peek().is_none() {
                return ConflictResolution::Block;
            }
            if others.any(|holder| !holder.is_reservation() || txn >= *holder) {
                return ConflictResolution::Block;
            }
        }
        ConflictResolution::Wound
    }

    /// Revoke every other holder of `key` and make `txn` its sole holder in
    /// `mode`, merged with any mode `txn` already held it in. The revoked
    /// reservations degrade to plain OCC, with no notification. Waiters stay
    /// queued.
    pub(super) fn wound(&mut self, txn: TxnId, key: &LockKey, mode: LockMode) {
        let Some(entry) = self.table.get_mut(key) else {
            return;
        };
        let wanted = if entry.holders.contains(&txn) {
            entry.mode.merge(mode)
        } else {
            mode
        };
        entry.holders.clear();
        entry.holders.push(txn);
        entry.mode = wanted;
    }

    /// Acquire a **shared** lock on a single `key` for `txn`: a read
    /// reservation. Same rules as [`Self::acquire`]. A reservation that
    /// meets an incompatible holder or an earlier waiter waits FIFO.
    pub fn acquire_shared(&mut self, txn: TxnId, key: LockKey) -> AcquireOutcome {
        self.acquire(txn, BTreeMap::from([(key, LockMode::Shared)]))
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

    fn exclusive(names: &[&str]) -> BTreeMap<LockKey, LockMode> {
        names
            .iter()
            .map(|n| (key(n), LockMode::Exclusive))
            .collect()
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    /// A read-reservation owner: position `n` of the reservation band.
    fn reservation(epoch: u64, n: u32) -> TxnId {
        TxnId::new(epoch, TxnId::RESERVATION_POSITION_BAND + n)
    }

    #[test]
    fn shared_shared_compatible() {
        let mut lm = LockManager::new();
        let (r1, r2) = (reservation(1, 0), reservation(1, 1));

        assert_eq!(lm.acquire_shared(r1, key("s")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(r2, key("s")), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("s")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holders.contains(&r1));
        assert!(entry.holders.contains(&r2));
    }

    #[test]
    fn a_shared_transaction_holder_is_never_wounded() {
        let mut lm = LockManager::new();
        let reader = txn(1, 2);
        let writer = txn(1, 1); // older than the reader

        assert_eq!(lm.acquire_shared(reader, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(writer, exclusive(&["k"])),
            AcquireOutcome::Blocked,
            "a transaction's read lock is executing work, not a reservation"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(writer));
    }

    #[test]
    fn exclusive_blocks_shared() {
        let mut lm = LockManager::new();
        let t1 = txn(1, 0);
        let r = reservation(1, 0);

        assert_eq!(lm.acquire(t1, exclusive(&["k"])), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire_shared(r, key("k")),
            AcquireOutcome::Blocked,
            "a shared request must block behind an exclusive holder"
        );
        assert!(lm.table.get(&key("k")).unwrap().has_waiter(r));
    }

    #[test]
    fn older_writer_wounds_shared() {
        let mut lm = LockManager::new();
        let r = reservation(1, 0);
        let writer = txn(1, 1); // older than every reservation of epoch 1

        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(writer, exclusive(&["k"])), AcquireOutcome::Ready);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.as_slice(), &[writer]);
    }

    #[test]
    fn older_intent_writer_wounds_a_collection_reservation() {
        let mut lm = LockManager::new();
        let r = reservation(1, 0);
        let writer = txn(1, 1);
        let coll = LockKey::Collection {
            collection: Arc::from("c"),
        };

        assert_eq!(lm.acquire_shared(r, coll.clone()), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(writer, BTreeMap::from([(coll.clone(), LockMode::Intent)])),
            AcquireOutcome::Ready
        );
        let entry = lm.table.get(&coll).unwrap();
        assert_eq!(entry.mode, LockMode::Intent);
        assert_eq!(entry.holders.as_slice(), &[writer]);
    }

    #[test]
    fn younger_writer_waits() {
        let mut lm = LockManager::new();
        let r = reservation(1, 1);
        let writer = txn(2, 0); // younger than the reservation

        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(writer, exclusive(&["k"])),
            AcquireOutcome::Blocked
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(entry.holders.contains(&r), "the shared holder still holds");
        assert!(!entry.holders.contains(&writer));
        assert!(entry.has_waiter(writer));
    }

    #[test]
    fn exclusive_waits_on_exclusive_regardless_of_age() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2);
        let t1 = txn(1, 1);

        assert_eq!(lm.acquire(t2, exclusive(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(t1, exclusive(&["k"])), AcquireOutcome::Blocked);

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.holders.as_slice(), &[t2]);
        assert!(entry.has_waiter(t1));
    }

    #[test]
    fn multi_key_atomic_wound_takes_both() {
        let mut lm = LockManager::new();
        let s1 = reservation(1, 5);
        let s2 = reservation(1, 6);
        let r = txn(1, 1);

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(r, exclusive(&["k1", "k2"])),
            AcquireOutcome::Ready
        );

        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert_eq!(entry.holders.as_slice(), &[r]);
        }
    }

    #[test]
    fn multi_key_wait_takes_no_conflicting_key() {
        let mut lm = LockManager::new();
        let s1 = reservation(1, 5); // younger than R
        let s2 = reservation(0, 0); // older than R
        let r = txn(1, 1);

        assert_eq!(lm.acquire_shared(s1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(s2, key("k2")), AcquireOutcome::Ready);

        // R is younger than the holder on k2, so it wounds nobody and waits on
        // both keys behind their shared holders.
        assert_eq!(
            lm.acquire(r, exclusive(&["k1", "k2"])),
            AcquireOutcome::Blocked
        );
        for (k, holder) in [("k1", s1), ("k2", s2)] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.holders.as_slice(), &[holder]);
            assert!(entry.has_waiter(r));
        }
    }

    #[test]
    fn self_upgrade_with_other_shared_holder_wounds_or_blocks() {
        // The older reservation's self-upgrade wounds the younger one.
        let mut lm = LockManager::new();
        let (r_old, r_young) = (reservation(1, 0), reservation(1, 1));

        assert_eq!(lm.acquire_shared(r_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(r_young, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(r_old, exclusive(&["k"])), AcquireOutcome::Ready);
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.as_slice(), &[r_old]);

        // The younger one's self-upgrade drops its own hold and waits.
        let mut lm = LockManager::new();
        assert_eq!(lm.acquire_shared(r_old, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(r_young, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(r_young, exclusive(&["k"])),
            AcquireOutcome::Blocked
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert_eq!(entry.holders.as_slice(), &[r_old]);
        assert!(entry.has_waiter(r_young));

        assert_eq!(lm.release(r_old), vec![r_young]);
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.as_slice(), &[r_young]);
    }

    #[test]
    fn crossed_reservations_are_acyclic() {
        // R1 reserves K1, R2 reserves K2. The older writer's exclusive acquire
        // of K2 wounds the younger reservation, breaking any cycle.
        let mut lm = LockManager::new();
        let r1 = reservation(1, 1);
        let r2 = reservation(1, 2);
        let writer = txn(1, 0);

        assert_eq!(lm.acquire_shared(r1, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(r2, key("k2")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(writer, exclusive(&["k2"])),
            AcquireOutcome::Ready
        );

        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert_eq!(k2.holders.as_slice(), &[writer]);
    }
}
