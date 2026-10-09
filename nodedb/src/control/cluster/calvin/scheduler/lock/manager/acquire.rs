// SPDX-License-Identifier: BUSL-1.1

//! Moded lock acquisition and FIFO waiter queueing.

use std::collections::BTreeMap;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{AcquireOutcome, LockMode};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::classify::KeyState;
use super::types::{ConflictResolution, LockManager};

impl LockManager {
    /// Acquire every key of `keys` for `txn`, each in its own mode.
    ///
    /// The request merges with any request `txn` already waits on. A key the
    /// txn names twice takes the merged mode (see [`LockMode::merge`]).
    ///
    /// When no key conflicts, `txn` takes every key and the call returns
    /// [`AcquireOutcome::Ready`]. A key conflicts when an incompatible holder
    /// holds it, or when an earlier waiter queues on it.
    ///
    /// A conflicting request is resolved under WOUND-WAIT (see
    /// [`Self::resolve_conflict`]). `TxnId` order is the replicated total
    /// order, so the decision is identical on every replica:
    /// - Every conflicting holder is a younger read reservation in `Shared`
    ///   mode, and nobody waits on a conflicting key: `txn` revokes those
    ///   reservations and takes every key.
    /// - Otherwise `txn` takes every key it can take now and waits FIFO on the
    ///   rest. Calls arrive in sequencer order, so every key grants its
    ///   requesters in sequencer order, whatever order earlier holders
    ///   release in.
    pub fn acquire(&mut self, txn: TxnId, keys: BTreeMap<LockKey, LockMode>) -> AcquireOutcome {
        let mut request = self.pending_keys.remove(&txn).unwrap_or_default();
        for (key, mode) in keys {
            request
                .entry(key)
                .and_modify(|held| *held = held.merge(mode))
                .or_insert(mode);
        }
        let states: Vec<KeyState> = request
            .iter()
            .map(|(key, mode)| self.classify(txn, key, *mode))
            .collect();

        let resolution = if states.contains(&KeyState::Conflict) {
            Some(self.resolve_conflict(txn, &request, &states))
        } else {
            None
        };

        match resolution {
            None => {
                for (key, mode) in &request {
                    self.grant(txn, key, *mode);
                }
                self.finish_ready(txn, request);
                AcquireOutcome::Ready
            }
            Some(ConflictResolution::Wound) => {
                for ((key, mode), state) in request.iter().zip(&states) {
                    match state {
                        KeyState::Conflict => self.wound(txn, key, *mode),
                        KeyState::Free | KeyState::Held | KeyState::Join => {
                            self.grant(txn, key, *mode)
                        }
                    }
                }
                self.finish_ready(txn, request);
                AcquireOutcome::Ready
            }
            Some(ConflictResolution::Block) => {
                for ((key, mode), state) in request.iter().zip(&states) {
                    match state {
                        KeyState::Conflict => self.enqueue(txn, key, *mode),
                        KeyState::Free | KeyState::Held | KeyState::Join => {
                            self.grant(txn, key, *mode)
                        }
                    }
                }
                self.pending_keys.insert(txn, request);
                AcquireOutcome::Blocked
            }
        }
    }

    /// Record `txn` as holder of every key of `request`.
    pub(super) fn finish_ready(&mut self, txn: TxnId, request: BTreeMap<LockKey, LockMode>) {
        self.held_locks
            .entry(txn)
            .or_default()
            .extend(request.into_keys());
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;

    fn key(name: &str) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from(name),
            surrogate: 1,
        }
    }

    fn coll(name: &str) -> LockKey {
        LockKey::Collection {
            collection: Arc::from(name),
        }
    }

    fn exclusive(names: &[&str]) -> BTreeMap<LockKey, LockMode> {
        names
            .iter()
            .map(|n| (key(n), LockMode::Exclusive))
            .collect()
    }

    fn one(key: LockKey, mode: LockMode) -> BTreeMap<LockKey, LockMode> {
        BTreeMap::from([(key, mode)])
    }

    fn txn(epoch: u64, pos: u32) -> TxnId {
        TxnId::new(epoch, pos)
    }

    #[test]
    fn acquire_free_keys_returns_ready() {
        let mut lm = LockManager::new();
        let outcome = lm.acquire(txn(1, 0), exclusive(&["a", "b"]));
        assert_eq!(outcome, AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 2);
    }

    #[test]
    fn acquire_held_key_returns_blocked_and_enqueues_waiter() {
        let mut lm = LockManager::new();
        let (t1, t2) = (txn(1, 0), txn(1, 1));
        lm.acquire(t1, exclusive(&["x"]));

        assert_eq!(lm.acquire(t2, exclusive(&["x"])), AcquireOutcome::Blocked);
        assert!(lm.table.get(&key("x")).unwrap().has_waiter(t2));
    }

    #[test]
    fn intent_intent_compatible() {
        let mut lm = LockManager::new();
        let (t1, t2) = (txn(1, 0), txn(1, 1));
        assert_eq!(
            lm.acquire(t1, one(coll("c"), LockMode::Intent)),
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(t2, one(coll("c"), LockMode::Intent)),
            AcquireOutcome::Ready,
            "two row writers of one collection hold its key together"
        );
        let entry = lm.table.get(&coll("c")).unwrap();
        assert_eq!(entry.mode, LockMode::Intent);
        assert_eq!(entry.holders.as_slice(), &[t1, t2]);
    }

    #[test]
    fn intent_blocks_against_exclusive_and_shared() {
        for other in [LockMode::Exclusive, LockMode::Shared] {
            // Intent held, the other mode requested.
            let mut lm = LockManager::new();
            let (t1, t2) = (txn(1, 0), txn(1, 1));
            assert_eq!(
                lm.acquire(t1, one(coll("c"), LockMode::Intent)),
                AcquireOutcome::Ready
            );
            assert_eq!(
                lm.acquire(t2, one(coll("c"), other)),
                AcquireOutcome::Blocked,
                "{other:?} waits behind an Intent holder"
            );
            assert_eq!(lm.release(t1), vec![t2]);

            // The other mode held, Intent requested.
            let mut lm = LockManager::new();
            assert_eq!(lm.acquire(t1, one(coll("c"), other)), AcquireOutcome::Ready);
            assert_eq!(
                lm.acquire(t2, one(coll("c"), LockMode::Intent)),
                AcquireOutcome::Blocked,
                "Intent waits behind a {other:?} holder"
            );
            assert_eq!(lm.release(t1), vec![t2]);
        }
    }

    #[test]
    fn a_compatible_request_queues_behind_an_earlier_waiter() {
        let mut lm = LockManager::new();
        let (reader, truncate, writer) = (txn(1, 0), txn(1, 1), txn(1, 2));
        assert_eq!(
            lm.acquire(reader, one(coll("c"), LockMode::Shared)),
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(truncate, one(coll("c"), LockMode::Exclusive)),
            AcquireOutcome::Blocked
        );
        // A later reader is compatible with the holder, yet it must not pass
        // the truncate sequenced before it.
        let late_reader = txn(1, 3);
        assert_eq!(
            lm.acquire(late_reader, one(coll("c"), LockMode::Shared)),
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(writer, one(coll("c"), LockMode::Intent)),
            AcquireOutcome::Blocked
        );
        assert_eq!(lm.release(reader), vec![truncate]);
        assert_eq!(lm.release(truncate), vec![late_reader]);
        assert_eq!(lm.release(late_reader), vec![writer]);
    }

    #[test]
    fn a_blocked_txn_takes_its_free_keys_so_a_later_txn_queues_behind_it() {
        let mut lm = LockManager::new();
        let (first, second, third) = (txn(1, 0), txn(1, 1), txn(1, 2));
        assert_eq!(lm.acquire(first, exclusive(&["a"])), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(second, exclusive(&["a", "b"])),
            AcquireOutcome::Blocked
        );
        // `b` is free at `second`'s request, so `second` holds it now. A
        // later txn on `b` waits for `second`, whatever order releases run in.
        assert_eq!(
            lm.acquire(third, exclusive(&["b"])),
            AcquireOutcome::Blocked
        );
        assert_eq!(lm.release(first), vec![second]);
        assert_eq!(lm.release(second), vec![third]);
    }

    #[test]
    fn the_strongest_mode_wins_within_one_request() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        assert_eq!(
            lm.acquire(t, one(coll("c"), LockMode::Shared)),
            AcquireOutcome::Ready
        );
        // The same txn now writes a row: Shared plus Intent merges to
        // Exclusive, held in place by the sole holder.
        assert_eq!(
            lm.acquire(t, one(coll("c"), LockMode::Intent)),
            AcquireOutcome::Ready
        );
        assert_eq!(lm.table.get(&coll("c")).unwrap().mode, LockMode::Exclusive);
        let other = txn(1, 1);
        assert_eq!(
            lm.acquire(other, one(coll("c"), LockMode::Shared)),
            AcquireOutcome::Blocked
        );
    }

    #[test]
    fn grant_order_is_identical_for_the_same_input_sequence() {
        // Every input of the sequence, applied in order: an acquire or a release.
        enum Step {
            Acquire(TxnId, BTreeMap<LockKey, LockMode>),
            Release(TxnId),
        }
        let steps = || {
            let row = |n: &str| (key(n), LockMode::Exclusive);
            let intent = (coll("c"), LockMode::Intent);
            vec![
                Step::Acquire(txn(1, 0), BTreeMap::from([intent.clone(), row("a")])),
                Step::Acquire(txn(1, 1), one(coll("c"), LockMode::Exclusive)),
                Step::Acquire(txn(1, 2), BTreeMap::from([intent.clone(), row("b")])),
                Step::Acquire(txn(1, 3), one(coll("c"), LockMode::Shared)),
                Step::Acquire(txn(2, 0), BTreeMap::from([intent, row("a")])),
                Step::Release(txn(1, 0)),
                Step::Release(txn(1, 1)),
                Step::Release(txn(1, 2)),
                Step::Release(txn(1, 3)),
                Step::Release(txn(2, 0)),
            ]
        };
        let run = || {
            let mut lm = LockManager::new();
            let mut granted = Vec::new();
            for step in steps() {
                match step {
                    Step::Acquire(t, keys) => {
                        if lm.acquire(t, keys) == AcquireOutcome::Ready {
                            granted.push(t);
                        }
                    }
                    Step::Release(t) => granted.extend(lm.release(t)),
                }
            }
            granted
        };
        let first = run();
        assert_eq!(first, run());
        assert_eq!(
            first,
            vec![txn(1, 0), txn(1, 1), txn(1, 2), txn(1, 3), txn(2, 0),],
            "every txn is granted in sequencer order"
        );
    }

    #[test]
    fn shared_reservation_self_upgrades_to_exclusive() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);

        assert_eq!(lm.acquire_shared(t, key("k")), AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(t, exclusive(&["k"])),
            AcquireOutcome::Ready,
            "self-upgrade from shared to exclusive must not block"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holders.as_slice(), &[t]);
    }

    #[test]
    fn self_upgrade_mixed_with_conflict_on_other_key() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let u = txn(1, 1);

        assert_eq!(lm.acquire_shared(t, key("k1")), AcquireOutcome::Ready);
        assert_eq!(lm.acquire(u, exclusive(&["k2"])), AcquireOutcome::Ready);

        assert_eq!(
            lm.acquire(t, exclusive(&["k1", "k2"])),
            AcquireOutcome::Blocked,
            "conflict on k2 blocks the whole set"
        );

        assert_eq!(lm.release(u), vec![t]);
        assert_eq!(
            lm.acquire(t, exclusive(&["k1", "k2"])),
            AcquireOutcome::Ready,
            "the promoted txn re-acquires its keys as a no-op"
        );
        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert_eq!(entry.holders.as_slice(), &[t]);
        }
    }

    #[test]
    fn many_mixed_deterministic_dispatch_order() {
        let mut lm = LockManager::new();
        let mut dispatched: Vec<TxnId> = Vec::new();

        let pairs = [(2, 0), (1, 1), (3, 0), (1, 0), (2, 1)];
        for (epoch, pos) in pairs {
            let tid = TxnId::new(epoch, pos);
            let keys = one(
                LockKey::Surrogate {
                    collection: Arc::from(format!("c_{epoch}_{pos}")),
                    surrogate: epoch as u32 * 10 + pos,
                },
                LockMode::Exclusive,
            );
            if lm.acquire(tid, keys) == AcquireOutcome::Ready {
                dispatched.push(tid);
            }
        }

        let expected: BTreeSet<TxnId> = pairs.map(|(e, p)| TxnId::new(e, p)).into();
        let got: BTreeSet<TxnId> = dispatched.into_iter().collect();
        assert_eq!(got, expected, "all non-conflicting txns are ready");
    }
}
