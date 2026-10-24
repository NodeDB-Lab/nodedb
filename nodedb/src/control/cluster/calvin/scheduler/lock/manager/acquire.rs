// SPDX-License-Identifier: BUSL-1.1

//! Moded lock acquisition and FIFO waiter queueing.

use std::collections::{BTreeMap, BTreeSet};

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{
    AcquireOutcome, Acquisition, LockMode,
};
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
    ///
    /// A held key the request raises past its other holders drops the hold
    /// (see [`Self::enqueue`]). Waiters that this completes return in
    /// [`Acquisition::promoted`]. The caller dispatches them.
    pub fn acquire(&mut self, txn: TxnId, keys: BTreeMap<LockKey, LockMode>) -> Acquisition {
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

        let mut promoted: BTreeSet<TxnId> = BTreeSet::new();
        let outcome = match resolution {
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
                        KeyState::Conflict => self.enqueue(txn, key, *mode, &mut promoted),
                        KeyState::Free | KeyState::Held | KeyState::Join => {
                            self.grant(txn, key, *mode)
                        }
                    }
                }
                self.pending_keys.insert(txn, request);
                AcquireOutcome::Blocked
            }
        };
        Acquisition {
            outcome,
            promoted: promoted.into_iter().collect(),
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
        let outcome = lm.acquire(txn(1, 0), exclusive(&["a", "b"])).outcome;
        assert_eq!(outcome, AcquireOutcome::Ready);
        assert_eq!(lm.lock_count(), 2);
    }

    #[test]
    fn acquire_held_key_returns_blocked_and_enqueues_waiter() {
        let mut lm = LockManager::new();
        let (t1, t2) = (txn(1, 0), txn(1, 1));
        lm.acquire(t1, exclusive(&["x"]));

        assert_eq!(
            lm.acquire(t2, exclusive(&["x"])).outcome,
            AcquireOutcome::Blocked
        );
        assert!(lm.table.get(&key("x")).unwrap().has_waiter(t2));
    }

    /// A waiter that asks again for a stronger mode raises its queued mode
    /// and keeps its place in the queue.
    #[test]
    fn a_waiter_raises_its_queued_mode_in_place() {
        let mut lm = LockManager::new();
        let (holder, t, u) = (txn(1, 0), txn(1, 1), txn(1, 2));
        assert_eq!(
            lm.acquire(holder, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire_shared(t, key("k")).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire_shared(u, key("k")).outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(t, exclusive(&["k"])).outcome,
            AcquireOutcome::Blocked
        );
        let queue: Vec<(TxnId, LockMode)> = lm
            .table
            .get(&key("k"))
            .unwrap()
            .waiters
            .iter()
            .copied()
            .collect();
        assert_eq!(queue, vec![(t, LockMode::Exclusive), (u, LockMode::Shared)]);
        lm.assert_consistent();

        assert_eq!(lm.release(holder), vec![t]);
        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holder_ids(), vec![t]);
        assert_eq!(
            lm.acquire(t, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready
        );
        lm.assert_consistent();

        assert_eq!(lm.release(t), vec![u]);
        lm.assert_consistent();
    }

    /// A holder that raises past the other holders drops its hold and waits.
    /// A front waiter compatible with the remaining holders takes the key at
    /// once, and the acquire reports it.
    #[test]
    fn a_raise_that_drops_a_hold_promotes_the_compatible_front_waiter() {
        let mut lm = LockManager::new();
        let (x, t, u, v) = (txn(1, 0), txn(1, 1), txn(1, 2), txn(1, 3));
        let k = coll("c");
        assert_eq!(
            lm.acquire(x, one(k.clone(), LockMode::Exclusive)).outcome,
            AcquireOutcome::Ready
        );
        for (waiter, mode) in [
            (t, LockMode::Intent),
            (u, LockMode::IntentShared),
            (v, LockMode::Shared),
        ] {
            assert_eq!(
                lm.acquire(waiter, one(k.clone(), mode)).outcome,
                AcquireOutcome::Blocked
            );
        }
        // Intent and IntentShared take the key together. Shared waits on
        // the Intent holder.
        assert_eq!(lm.release(x), vec![t, u]);
        let entry = lm.table.get(&k).unwrap();
        assert_eq!(
            entry.holders.iter().copied().collect::<Vec<_>>(),
            vec![(t, LockMode::Intent), (u, LockMode::IntentShared)]
        );
        assert_eq!(
            entry.waiters.iter().copied().collect::<Vec<_>>(),
            vec![(v, LockMode::Shared)]
        );
        lm.assert_consistent();

        assert_eq!(
            lm.acquire(t, one(k.clone(), LockMode::Exclusive)),
            Acquisition {
                outcome: AcquireOutcome::Blocked,
                promoted: vec![v],
            },
            "t drops its Intent hold, so v shares the key with u"
        );
        let entry = lm.table.get(&k).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert_eq!(
            entry.holders.iter().copied().collect::<Vec<_>>(),
            vec![(u, LockMode::IntentShared), (v, LockMode::Shared)]
        );
        assert_eq!(
            entry.waiters.iter().copied().collect::<Vec<_>>(),
            vec![(t, LockMode::Exclusive)]
        );
        assert!(lm.is_ready(v, &one(k.clone(), LockMode::Shared)));
        lm.assert_consistent();

        assert!(lm.release(u).is_empty(), "v still holds the key Shared");
        assert_eq!(lm.release(v), vec![t]);
        assert_eq!(lm.table.get(&k).unwrap().holder_ids(), vec![t]);
        lm.assert_consistent();
    }

    #[test]
    fn intent_intent_compatible() {
        let mut lm = LockManager::new();
        let (t1, t2) = (txn(1, 0), txn(1, 1));
        assert_eq!(
            lm.acquire(t1, one(coll("c"), LockMode::Intent)).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(t2, one(coll("c"), LockMode::Intent)).outcome,
            AcquireOutcome::Ready,
            "two row writers of one collection hold its key together"
        );
        let entry = lm.table.get(&coll("c")).unwrap();
        assert_eq!(entry.mode, LockMode::Intent);
        assert_eq!(entry.holder_ids(), vec![t1, t2]);
    }

    #[test]
    fn intent_blocks_against_exclusive_and_shared() {
        for other in [LockMode::Exclusive, LockMode::Shared] {
            // Intent held, the other mode requested.
            let mut lm = LockManager::new();
            let (t1, t2) = (txn(1, 0), txn(1, 1));
            assert_eq!(
                lm.acquire(t1, one(coll("c"), LockMode::Intent)).outcome,
                AcquireOutcome::Ready
            );
            assert_eq!(
                lm.acquire(t2, one(coll("c"), other)).outcome,
                AcquireOutcome::Blocked,
                "{other:?} waits behind an Intent holder"
            );
            assert_eq!(lm.release(t1), vec![t2]);

            // The other mode held, Intent requested.
            let mut lm = LockManager::new();
            assert_eq!(
                lm.acquire(t1, one(coll("c"), other)).outcome,
                AcquireOutcome::Ready
            );
            assert_eq!(
                lm.acquire(t2, one(coll("c"), LockMode::Intent)).outcome,
                AcquireOutcome::Blocked,
                "Intent waits behind a {other:?} holder"
            );
            assert_eq!(lm.release(t1), vec![t2]);
        }
    }

    /// A row read: its row `Shared`, its collection `IntentShared`.
    fn row_read(name: &str) -> BTreeMap<LockKey, LockMode> {
        BTreeMap::from([
            (key(name), LockMode::Shared),
            (coll(name), LockMode::IntentShared),
        ])
    }

    #[test]
    fn a_row_reader_orders_against_a_collection_writer() {
        // The reader holds first: the truncate waits for it.
        let mut lm = LockManager::new();
        let (reader, truncate) = (txn(1, 0), txn(1, 1));
        assert_eq!(
            lm.acquire(reader, row_read("c")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(truncate, one(coll("c"), LockMode::Exclusive))
                .outcome,
            AcquireOutcome::Blocked,
            "a truncate waits for the row reads of its collection"
        );
        assert_eq!(lm.release(reader), vec![truncate]);

        // The truncate holds first: the reader waits for it.
        let mut lm = LockManager::new();
        let (truncate, reader) = (txn(1, 0), txn(1, 1));
        assert_eq!(
            lm.acquire(truncate, one(coll("c"), LockMode::Exclusive))
                .outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(reader, row_read("c")).outcome,
            AcquireOutcome::Blocked,
            "a row read waits for the truncate sequenced before it"
        );
        assert_eq!(lm.release(truncate), vec![reader]);
    }

    #[test]
    fn a_row_reader_shares_its_collection_with_row_writers_and_predicate_readers() {
        let mut lm = LockManager::new();
        let (reader, writer, scan) = (txn(1, 0), txn(1, 1), txn(1, 2));
        assert_eq!(
            lm.acquire(reader, row_read("c")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(
                writer,
                BTreeMap::from([
                    (coll("c"), LockMode::Intent),
                    (key("d"), LockMode::Exclusive)
                ])
            )
            .outcome,
            AcquireOutcome::Ready,
            "a row writer joins a row reader"
        );
        assert_eq!(
            lm.acquire(scan, one(coll("c"), LockMode::Shared)).outcome,
            AcquireOutcome::Blocked,
            "a predicate reader waits for the row writer"
        );
        // The writer leaves: the group drops to the reader's mode, which the
        // predicate reader shares.
        assert_eq!(lm.release(writer), vec![scan]);
        let entry = lm.table.get(&coll("c")).unwrap();
        assert_eq!(entry.mode, LockMode::Shared);
        assert_eq!(entry.holder_ids(), vec![reader, scan]);
    }

    #[test]
    fn a_row_read_and_a_row_write_of_one_collection_merge_to_intent() {
        let mut lm = LockManager::new();
        let (t, other) = (txn(1, 0), txn(1, 1));
        let mut keys = row_read("c");
        keys.insert(coll("c"), LockMode::IntentShared.merge(LockMode::Intent));
        assert_eq!(keys.get(&coll("c")), Some(&LockMode::Intent));
        assert_eq!(lm.acquire(t, keys).outcome, AcquireOutcome::Ready);
        assert_eq!(
            lm.acquire(other, one(coll("c"), LockMode::Intent)).outcome,
            AcquireOutcome::Ready
        );
    }

    #[test]
    fn a_compatible_request_queues_behind_an_earlier_waiter() {
        let mut lm = LockManager::new();
        let (reader, truncate, writer) = (txn(1, 0), txn(1, 1), txn(1, 2));
        assert_eq!(
            lm.acquire(reader, one(coll("c"), LockMode::Shared)).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(truncate, one(coll("c"), LockMode::Exclusive))
                .outcome,
            AcquireOutcome::Blocked
        );
        // A later reader is compatible with the holder, yet it must not pass
        // the truncate sequenced before it.
        let late_reader = txn(1, 3);
        assert_eq!(
            lm.acquire(late_reader, one(coll("c"), LockMode::Shared))
                .outcome,
            AcquireOutcome::Blocked
        );
        assert_eq!(
            lm.acquire(writer, one(coll("c"), LockMode::Intent)).outcome,
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
        assert_eq!(
            lm.acquire(first, exclusive(&["a"])).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(second, exclusive(&["a", "b"])).outcome,
            AcquireOutcome::Blocked
        );
        // `b` is free at `second`'s request, so `second` holds it now. A
        // later txn on `b` waits for `second`, whatever order releases run in.
        assert_eq!(
            lm.acquire(third, exclusive(&["b"])).outcome,
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
            lm.acquire(t, one(coll("c"), LockMode::Shared)).outcome,
            AcquireOutcome::Ready
        );
        // The same txn now writes a row: Shared plus Intent merges to
        // Exclusive, held in place by the sole holder.
        assert_eq!(
            lm.acquire(t, one(coll("c"), LockMode::Intent)).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(lm.table.get(&coll("c")).unwrap().mode, LockMode::Exclusive);
        let other = txn(1, 1);
        assert_eq!(
            lm.acquire(other, one(coll("c"), LockMode::Shared)).outcome,
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
                        if lm.acquire(t, keys).outcome == AcquireOutcome::Ready {
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

        assert_eq!(
            lm.acquire_shared(t, key("k")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(t, exclusive(&["k"])).outcome,
            AcquireOutcome::Ready,
            "self-upgrade from shared to exclusive must not block"
        );

        let entry = lm.table.get(&key("k")).unwrap();
        assert_eq!(entry.mode, LockMode::Exclusive);
        assert_eq!(entry.holder_ids(), vec![t]);
    }

    #[test]
    fn self_upgrade_mixed_with_conflict_on_other_key() {
        let mut lm = LockManager::new();
        let t = txn(1, 0);
        let u = txn(1, 1);

        assert_eq!(
            lm.acquire_shared(t, key("k1")).outcome,
            AcquireOutcome::Ready
        );
        assert_eq!(
            lm.acquire(u, exclusive(&["k2"])).outcome,
            AcquireOutcome::Ready
        );

        assert_eq!(
            lm.acquire(t, exclusive(&["k1", "k2"])).outcome,
            AcquireOutcome::Blocked,
            "conflict on k2 blocks the whole set"
        );

        assert_eq!(lm.release(u), vec![t]);
        assert_eq!(
            lm.acquire(t, exclusive(&["k1", "k2"])).outcome,
            AcquireOutcome::Ready,
            "the promoted txn re-acquires its keys as a no-op"
        );
        for k in ["k1", "k2"] {
            let entry = lm.table.get(&key(k)).unwrap();
            assert_eq!(entry.mode, LockMode::Exclusive);
            assert_eq!(entry.holder_ids(), vec![t]);
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
            if lm.acquire(tid, keys).outcome == AcquireOutcome::Ready {
                dispatched.push(tid);
            }
        }

        let expected: BTreeSet<TxnId> = pairs.map(|(e, p)| TxnId::new(e, p)).into();
        let got: BTreeSet<TxnId> = dispatched.into_iter().collect();
        assert_eq!(got, expected, "all non-conflicting txns are ready");
    }
}
