// SPDX-License-Identifier: BUSL-1.1

//! Shared-lock reservations and the wound-wait conflict resolution used by
//! acquisition.

use std::collections::BTreeMap;

use smallvec::SmallVec;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{Acquisition, LockMode};
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
            if !entry.waiters.is_empty() {
                return ConflictResolution::Block;
            }
            let mut others = entry.holders.iter().filter(|(h, _)| *h != txn).peekable();
            if others.peek().is_none() {
                return ConflictResolution::Block;
            }
            if others.any(|(holder, held)| {
                *held != LockMode::Shared || !holder.is_reservation() || txn >= *holder
            }) {
                return ConflictResolution::Block;
            }
        }
        ConflictResolution::Wound
    }

    /// Revoke every other holder of `key` and make `txn` its sole holder in
    /// `mode`, merged with any mode `txn` already held it in.
    ///
    /// A revoked holder that is blocked and whose request names `key` waits
    /// on it again, in its held mode merged with the requested one. A blocked
    /// txn thus holds or waits on every key of its request. Any other revoked
    /// reservation degrades to plain OCC, with no notification.
    pub(super) fn wound(&mut self, txn: TxnId, key: &LockKey, mode: LockMode) {
        let Some(entry) = self.table.get_mut(key) else {
            return;
        };
        let wanted = entry.mode_of(txn).map_or(mode, |own| own.merge(mode));
        let revoked: SmallVec<[(TxnId, LockMode); 2]> = entry
            .holders
            .drain(..)
            .filter(|(holder, _)| *holder != txn)
            .collect();
        entry.holders.push((txn, wanted));
        entry.mode = wanted;
        // The conflict resolution admits a wound only on a key nobody waits
        // on, so these waiters lead the queue, in grant order.
        for (holder, held) in revoked {
            let Some(requested) = self
                .pending_keys
                .get(&holder)
                .and_then(|request| request.get(key))
            else {
                continue;
            };
            entry.waiters.push_back((holder, held.merge(*requested)));
            self.waiting.entry(holder).or_default().insert(key.clone());
        }
        self.wake_watchers(key);
    }

    /// Acquire a **shared** lock on a single `key` for `txn`: a read
    /// reservation. Same rules as [`Self::acquire`]. A reservation that
    /// meets an incompatible holder or an earlier waiter waits FIFO.
    pub fn acquire_shared(&mut self, txn: TxnId, key: LockKey) -> Acquisition {
        self.acquire(txn, BTreeMap::from([(key, LockMode::Shared)]))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock::lock_entry::AcquireOutcome;

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

        assert_eq!(
            lm.acquire_shared(r1, key("s")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r2, key("s")).outcome,
            AcquireOutcome::Ready
        );

        let entry = lm.table.get(&key("s")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert!(entry.holds(r1));
        assert!(entry.holds(r2));
    }

    #[test]
    fn a_shared_transaction_holder_is_never_wounded() {
        let mut lm = LockManager::new();
        let reader = txn(1, 2);
        let writer = txn(1, 1); // older than the reader

        assert_eq!(
            lm.acquire_shared(reader, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(writer, exclusive(&["k"])).outcome,
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

        assert_eq!(
            lm.acquire(t1, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r, key("k")).outcome,
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

        assert_eq!(
            lm.acquire_shared(r, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(writer, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holder_ids(), vec![writer]);
    }

    #[test]
    fn older_intent_writer_wounds_a_collection_reservation() {
        let mut lm = LockManager::new();
        let r = reservation(1, 0);
        let writer = txn(1, 1);
        let coll = LockKey::Collection {
            collection: Arc::from("c"),
        };

        assert_eq!(
            lm.acquire_shared(r, coll.clone()).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(writer, BTreeMap::from([(coll.clone(), LockMode::Intent)]))
                .outcome,
            AcquireOutcome::Ready
        );
        let entry = lm.table.get(&coll).unwrap();
        assert_eq!(entry.mode, LockMode::Intent);
        assert_eq!(entry.holder_ids(), vec![writer]);
    }

    #[test]
    fn younger_writer_waits() {
        let mut lm = LockManager::new();
        let r = reservation(1, 1);
        let writer = txn(2, 0); // younger than the reservation

        assert_eq!(
            lm.acquire_shared(r, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(writer, exclusive(&["k"])).outcome,
            AcquireOutcome::Blocked
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert!(entry.holds(r), "the shared holder still holds");
        assert!(!entry.holds(writer));
        assert!(entry.has_waiter(writer));
    }

    #[test]
    fn exclusive_waits_on_exclusive_regardless_of_age() {
        let mut lm = LockManager::new();
        let t2 = txn(1, 2);
        let t1 = txn(1, 1);

        assert_eq!(
            lm.acquire(t2, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(t1, exclusive(&["k"])).outcome,
            AcquireOutcome::Blocked
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.holder_ids(), vec![t2]);
        assert!(entry.has_waiter(t1));
    }

    #[test]
    fn multi_key_atomic_wound_takes_both() {
        let mut lm = LockManager::new();
        let s1 = reservation(1, 5);
        let s2 = reservation(1, 6);
        let r = txn(1, 1);

        assert_eq!(
            lm.acquire_shared(s1, key("k1")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(s2, key("k2")).outcome,
            AcquireOutcome::Ready
        );

        assert_eq!(
            lm.acquire(r, exclusive(&["k1", "k2"])).outcome,
            AcquireOutcome::Ready
        );

        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert_eq!(entry.holder_ids(), vec![r]);
        }
    }

    #[test]
    fn multi_key_wait_takes_no_conflicting_key() {
        let mut lm = LockManager::new();
        let s1 = reservation(1, 5); // younger than R
        let s2 = reservation(0, 0); // older than R
        let r = txn(1, 1);

        assert_eq!(
            lm.acquire_shared(s1, key("k1")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(s2, key("k2")).outcome,
            AcquireOutcome::Ready
        );

        // R is younger than the holder on k2, so it wounds nobody and waits on
        // both keys behind their shared holders.
        assert_eq!(
            lm.acquire(r, exclusive(&["k1", "k2"])).outcome,
            AcquireOutcome::Blocked
        );
        for (k, holder) in [("k1", s1), ("k2", s2)] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.holder_ids(), vec![holder]);
            assert!(entry.has_waiter(r));
        }
    }

    #[test]
    fn self_upgrade_with_other_shared_holder_wounds_or_blocks() {
        // The older reservation's self-upgrade wounds the younger one.
        let mut lm = LockManager::new();
        let (r_old, r_young) = (reservation(1, 0), reservation(1, 1));

        assert_eq!(
            lm.acquire_shared(r_old, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r_young, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(r_old, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holder_ids(), vec![r_old]);

        // The younger one's self-upgrade drops its own hold and waits.
        let mut lm = LockManager::new();
        assert_eq!(
            lm.acquire_shared(r_old, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r_young, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(r_young, exclusive(&["k"])).outcome,
            AcquireOutcome::Blocked
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert_eq!(entry.holder_ids(), vec![r_old]);
        assert!(entry.has_waiter(r_young));

        assert_eq!(lm.release(r_old), vec![r_young]);
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holder_ids(), vec![r_young]);
    }

    #[test]
    fn crossed_reservations_are_acyclic() {
        // R1 reserves K1, R2 reserves K2. The older writer's exclusive acquire
        // of K2 wounds the younger reservation, breaking any cycle.
        let mut lm = LockManager::new();
        let r1 = reservation(1, 1);
        let r2 = reservation(1, 2);
        let writer = txn(1, 0);

        assert_eq!(
            lm.acquire_shared(r1, key("k1")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r2, key("k2")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(writer, exclusive(&["k2"])).outcome,
            AcquireOutcome::Ready
        );

        let k2 = lm.table.get(&key("k2")).unwrap();
        assert_eq!(k2.mode, LockMode::Exclusive);
        assert_eq!(k2.holder_ids(), vec![writer]);
    }

    /// A blocked reservation that loses a key of its request to a wound
    /// waits on it again, first in line, and runs once both keys free up.
    #[test]
    fn a_wounded_blocked_holder_waits_behind_the_wounder() {
        let mut lm = LockManager::new();
        let (h, w, later) = (txn(1, 0), txn(1, 1), txn(2, 0));
        let r = reservation(1, 0);
        let commit = BTreeMap::from([
            (key("a"), LockMode::Exclusive),
            (key("b"), LockMode::Shared),
        ]);

        assert_eq!(
            lm.acquire(h, exclusive(&["a"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r, key("b")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(r, commit.clone()).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(w, exclusive(&["b"])).outcome,
            AcquireOutcome::Ready,
            "w wounds r on b"
        );
        let b = lm.table.get(&key("b")).unwrap();
        assert_eq!(b.holder_ids(), vec![w]);
        assert_eq!(
            b.waiters.iter().copied().collect::<Vec<_>>(),
            vec![(r, LockMode::Shared)]
        );
        lm.assert_consistent();

        assert!(lm.release(h).is_empty(), "r takes a but still waits on b");
        assert_eq!(
            lm.acquire(later, exclusive(&["a"])).outcome,
            AcquireOutcome::Blocked
        );
        lm.assert_consistent();

        assert_eq!(lm.release(w), vec![r]);
        lm.assert_consistent();
        assert!(lm.is_ready(r, &commit));
        assert_eq!(lm.acquire(r, commit).outcome, AcquireOutcome::Ready);

        assert_eq!(lm.release(r), vec![later]);
        lm.assert_consistent();
    }

    /// A granted reservation that loses its key degrades to OCC: it waits
    /// for nothing.
    #[test]
    fn a_wounded_granted_reservation_degrades_without_a_wait() {
        let mut lm = LockManager::new();
        let (r, w) = (reservation(1, 0), txn(1, 1));
        assert_eq!(
            lm.acquire_shared(r, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(w, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.holder_ids(), vec![w]);
        assert!(entry.waiters.is_empty());
        lm.assert_consistent();
        assert!(lm.release(w).is_empty());
        assert_eq!(lm.lock_count(), 0);
        assert!(lm.release(r).is_empty());
        lm.assert_consistent();
    }

    /// A blocked reservation that loses a key outside its request queues no
    /// wait for it.
    #[test]
    fn a_wound_outside_the_blocked_request_queues_nothing() {
        let mut lm = LockManager::new();
        let (h, w) = (txn(1, 0), txn(1, 1));
        let r = reservation(1, 0);
        assert_eq!(
            lm.acquire(h, exclusive(&["a"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(r, key("b")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(r, exclusive(&["a"])).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(w, exclusive(&["b"])).outcome,
            AcquireOutcome::Ready
        );
        let b = lm.table.get(&key("b")).unwrap();
        assert_eq!(b.holder_ids(), vec![w]);
        assert!(b.waiters.is_empty(), "b is outside r's blocked request");
        lm.assert_consistent();
        assert_eq!(lm.release(h), vec![r]);
        lm.assert_consistent();
    }
}
