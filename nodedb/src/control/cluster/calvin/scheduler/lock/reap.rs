// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lease-based reaping of orphaned Calvin read reservations.
//!
//! A read reservation installs a SHARED lock owned by a reservation-band
//! [`TxnId`] (see [`TxnId::is_reservation`]). If the coordinator that owns it
//! crashes during think-time before committing or aborting, the shared lock
//! leaks and can block younger writers forever. [`LockManager::reap_expired_shared`]
//! releases any such reservation whose owner epoch has fallen behind a
//! replicated logical threshold — no wall clock, so every replica reaps
//! identically given the same input order.

use std::collections::BTreeSet;

use super::lock_entry::LockMode;
use super::lock_key::TxnId;
use super::manager::LockManager;

impl LockManager {
    /// Reap every SHARED reservation whose owner mint-epoch is older than
    /// `epoch_threshold`, releasing it through the normal `release` path (so any
    /// waiter queued behind the freed key is promoted exactly like a live
    /// release). Returns the promoted waiter ids, ready for `dispatch_promoted`.
    ///
    /// Restricted to owners in the reservation band (`is_reservation()`) — a real
    /// transaction's lock owner is never in this band, so a real txn's locks can
    /// never be reaped. A reservation that still waits for a key is reaped too,
    /// so it never blocks the waiters queued behind it. An owner that holds or
    /// requests any key in a mode other than `Shared` (a commit in flight under
    /// that reservation) is left alone.
    pub fn reap_expired_shared(&mut self, epoch_threshold: u64) -> Vec<TxnId> {
        let owners: BTreeSet<TxnId> = self
            .held_locks
            .keys()
            .chain(self.pending_keys.keys())
            .copied()
            .filter(|owner| owner.is_reservation() && owner.epoch < epoch_threshold)
            .collect();
        let expired: Vec<TxnId> = owners
            .into_iter()
            .filter(|owner| self.holds_only_shared(*owner))
            .collect();

        let mut promoted = Vec::new();
        for owner in expired {
            tracing::debug!(
                epoch = owner.epoch,
                position = owner.position,
                "calvin: reaping lease-expired shared reservation"
            );
            promoted.extend(self.release(owner));
        }
        promoted.sort();
        promoted.dedup();
        promoted
    }

    /// Whether every key `owner` holds, and every key it waits for, is
    /// `Shared`. A key whose entry a wound revoked from `owner` does not count.
    fn holds_only_shared(&self, owner: TxnId) -> bool {
        let held_shared = self.held_locks.get(&owner).is_none_or(|keys| {
            keys.iter().all(|key| {
                self.table
                    .get(key)
                    .is_none_or(|e| !e.holders.contains(&owner) || e.mode == LockMode::Shared)
            })
        });
        let waits_shared = self
            .pending_keys
            .get(&owner)
            .is_none_or(|keys| keys.values().all(|mode| *mode == LockMode::Shared));
        held_shared && waits_shared
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock::lock_entry::AcquireOutcome;
    use crate::control::cluster::calvin::scheduler::lock::lock_key::LockKey;

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

    fn resv(epoch: u64, pos_offset: u32) -> TxnId {
        TxnId::new(epoch, TxnId::RESERVATION_POSITION_BAND + pos_offset)
    }

    #[test]
    fn reap_releases_expired_shared_reservation() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 1);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty(), "no waiter queued behind the key");
        assert_eq!(lm.lock_count(), 0, "the reservation's key is freed");
        assert_eq!(lm.holder_count(), 0, "the reservation is released");
    }

    #[test]
    fn reap_ignores_within_lease() {
        let mut lm = LockManager::new();
        let r = resv(90, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(50);
        assert!(promoted.is_empty());
        assert_eq!(lm.lock_count(), 1, "still within the lease, not reaped");
        assert!(lm.table.get(&key("k")).unwrap().holders.contains(&r));
    }

    #[test]
    fn reap_ignores_non_reservation_owner() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        assert_eq!(lm.acquire(t, keyset(&["k"])), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty());
        assert_eq!(
            lm.lock_count(),
            1,
            "a real txn's exclusive lock is never reaped"
        );
        assert!(lm.table.get(&key("k")).unwrap().holders.contains(&t));
    }

    #[test]
    fn reap_skips_self_upgraded_reservation() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        // Self-upgrade to exclusive: a commit in flight under this reservation.
        assert_eq!(lm.acquire(r, keyset(&["k"])), AcquireOutcome::Ready);

        let promoted = lm.reap_expired_shared(100);
        assert!(promoted.is_empty());
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(
            entry.mode,
            LockMode::Exclusive,
            "mid-commit reservation is left alone"
        );
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], r);
    }

    #[test]
    fn reap_promotes_waiter() {
        let mut lm = LockManager::new();
        let r = resv(1, 0);
        let writer = txn(5, 0);

        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Ready);
        // A younger writer waits behind the shared reservation (wound-wait
        // younger-waits, since the writer's epoch is greater than r's).
        assert_eq!(lm.acquire(writer, keyset(&["k"])), AcquireOutcome::Blocked);
        assert!(
            lm.table
                .get(&key("k"))
                .unwrap()
                .waiters
                .iter()
                .any(|(w, _)| *w == writer)
        );

        let promoted = lm.reap_expired_shared(100);
        assert_eq!(
            promoted,
            vec![writer],
            "reaping the stuck reservation unblocks the younger writer"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.len(), 1);
        assert_eq!(entry.holders[0], writer);
    }

    #[test]
    fn reap_releases_a_waiting_reservation() {
        let mut lm = LockManager::new();
        let writer = txn(1, 0);
        let r = resv(1, 0);
        let later = txn(2, 0);

        assert_eq!(lm.acquire(writer, keyset(&["k"])), AcquireOutcome::Ready);
        assert_eq!(lm.acquire_shared(r, key("k")), AcquireOutcome::Blocked);
        assert_eq!(lm.acquire(later, keyset(&["k"])), AcquireOutcome::Blocked);

        assert!(lm.reap_expired_shared(100).is_empty());
        // The reaped reservation no longer queues ahead of `later`.
        assert_eq!(lm.release(writer), vec![later]);
    }
}
