// SPDX-License-Identifier: BUSL-1.1

//! Propose waiters and early results, keyed by `(group_id, log_index)`.
//!
//! A proposer registers its waiter after the proposal returns its log index.
//! The apply loop can finish the entry first, so a result with no waiter is
//! stored for the `register` that follows. Most stored results are never
//! claimed: a follower applies entries proposed on other nodes. Stored
//! results are therefore bounded by the waiter window and by count, and the
//! oldest are evicted.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::applied_write::ProposeResult;

/// Results stored before their waiter registered, kept at most this many.
pub(super) const MAX_STORED_RESULTS: usize = 1 << 16;

/// A pending waiter, or a result that arrived before its waiter.
enum Slot {
    /// `expected_key` is the proposer's idempotency key. `0` accepts any key.
    Waiting {
        tx: oneshot::Sender<ProposeResult>,
        expected_key: u64,
    },
    /// Stored at `at`. Evicted once older than the waiter window.
    Completed { result: ProposeResult, at: Instant },
}

/// The committed proposal keys a snapshot install covered in one group.
struct Covered {
    through: u64,
    /// Empty once the install is older than the waiter window.
    keys: HashSet<u64>,
    /// Lowest index the keys are complete from. Below it a missing key
    /// proves nothing. [`KEYS_EXPIRED`] once the keys are cleared.
    complete_from: u64,
    at: Instant,
}

/// `Covered::complete_from` of an install whose keys expired: no index is
/// complete, so every covered waiter gets an unknown outcome.
const KEYS_EXPIRED: u64 = u64::MAX;

/// Every waiter and stored result, plus what an install covered.
pub(super) struct WaiterSlots {
    slots: HashMap<(u64, u64), Slot>,
    /// Stored results in store order, for eviction by age and count.
    stored: VecDeque<(Instant, (u64, u64))>,
    /// Entries the apply loop sent toward a core and has not concluded. Their
    /// own `complete` answers their waiters, so an install never does.
    dispatched: HashSet<(u64, u64)>,
    covered: HashMap<u64, Covered>,
    window: Duration,
}

/// Whether the entry that committed carries a different proposal than the
/// waiter reserved. `0` on either side means no key to compare.
fn key_mismatch(applied_key: u64, expected_key: u64) -> bool {
    applied_key != 0 && expected_key != 0 && applied_key != expected_key
}

fn retryable_leader_change(group_id: u64, log_index: u64) -> ProposeResult {
    Err(crate::Error::RetryableLeaderChange {
        group_id,
        log_index,
    })
}

fn proposal_outcome_unknown(group_id: u64, log_index: u64) -> ProposeResult {
    Err(crate::Error::ProposalOutcomeUnknown {
        group_id,
        log_index,
    })
}

fn committed_result_unavailable(group_id: u64, log_index: u64) -> ProposeResult {
    Err(crate::Error::CommittedResultUnavailable {
        group_id,
        log_index,
    })
}

impl WaiterSlots {
    pub fn new(window: Duration) -> Self {
        Self {
            slots: HashMap::new(),
            stored: VecDeque::new(),
            dispatched: HashSet::new(),
            covered: HashMap::new(),
            window,
        }
    }

    pub fn set_window(&mut self, window: Duration) {
        self.window = window;
    }

    /// Number of stored results not yet claimed.
    #[cfg(test)]
    pub fn stored_len(&self) -> usize {
        self.slots
            .values()
            .filter(|slot| matches!(slot, Slot::Completed { .. }))
            .count()
    }

    /// The answer for a waiter at a covered index. A key the snapshot carried
    /// committed. A missing key means overwritten only at an index the keys
    /// are complete from; below it the outcome is unknown. A waiter with no
    /// key cannot be matched and is told it committed.
    fn covered_outcome(&self, group_id: u64, log_index: u64, expected_key: u64) -> ProposeResult {
        let Some(covered) = self.covered.get(&group_id) else {
            return proposal_outcome_unknown(group_id, log_index);
        };
        if covered.keys.contains(&expected_key) {
            return committed_result_unavailable(group_id, log_index);
        }
        if log_index < covered.complete_from {
            return proposal_outcome_unknown(group_id, log_index);
        }
        if expected_key == 0 {
            committed_result_unavailable(group_id, log_index)
        } else {
            retryable_leader_change(group_id, log_index)
        }
    }

    /// Clear the keys of every install older than the waiter window. Every
    /// waiter the keys can answer has passed its deadline, so holding them
    /// serves nobody. A later waiter in the covered range gets an unknown
    /// outcome, the safe answer once the keys are gone.
    fn expire_covered(&mut self, now: Instant) {
        let window = self.window;
        for covered in self.covered.values_mut() {
            if covered.complete_from != KEYS_EXPIRED
                && now.saturating_duration_since(covered.at) > window
            {
                covered.keys = HashSet::new();
                covered.complete_from = KEYS_EXPIRED;
            }
        }
    }

    fn is_covered(&self, group_id: u64, log_index: u64) -> bool {
        self.covered
            .get(&group_id)
            .is_some_and(|covered| log_index <= covered.through)
            && !self.dispatched.contains(&(group_id, log_index))
    }

    pub fn register(
        &mut self,
        group_id: u64,
        log_index: u64,
        expected_key: u64,
        tx: oneshot::Sender<ProposeResult>,
        now: Instant,
    ) {
        self.expire_covered(now);
        let key = (group_id, log_index);
        if !self.slots.contains_key(&key) && self.is_covered(group_id, log_index) {
            // No apply on this node will complete a covered entry.
            let _ = tx.send(self.covered_outcome(group_id, log_index, expected_key));
            return;
        }
        match self.slots.entry(key) {
            Entry::Vacant(e) => {
                e.insert(Slot::Waiting { tx, expected_key });
            }
            Entry::Occupied(mut e) => match e.get() {
                Slot::Completed { .. } => {
                    if let Slot::Completed { result, .. } = e.remove() {
                        let _ = tx.send(result);
                    }
                }
                Slot::Waiting { .. } => {
                    // A second register for one index. The older receiver
                    // sees its channel close.
                    e.insert(Slot::Waiting { tx, expected_key });
                }
            },
        }
    }

    pub fn abandon(&mut self, group_id: u64, log_index: u64) {
        if let Entry::Occupied(e) = self.slots.entry((group_id, log_index))
            && matches!(e.get(), Slot::Waiting { .. })
        {
            e.remove();
        }
    }

    /// Answer the waiter at `(group_id, log_index)` with `result`, or store
    /// the result for a `register` still to come. Returns whether a waiter
    /// was answered.
    pub fn complete(
        &mut self,
        group_id: u64,
        log_index: u64,
        applied_key: u64,
        result: ProposeResult,
        now: Instant,
    ) -> bool {
        let key = (group_id, log_index);
        match self.slots.entry(key) {
            Entry::Vacant(e) => {
                e.insert(Slot::Completed { result, at: now });
                self.stored.push_back((now, key));
                self.evict(now);
                false
            }
            Entry::Occupied(mut e) => match e.get() {
                Slot::Waiting { expected_key, .. } => {
                    let final_result = if key_mismatch(applied_key, *expected_key) {
                        tracing::warn!(
                            group_id,
                            log_index,
                            applied_key,
                            expected_key = *expected_key,
                            "raft entry at proposer's index was overwritten by \
                             a different proposal (idempotency_key mismatch); \
                             surfacing RetryableLeaderChange"
                        );
                        retryable_leader_change(group_id, log_index)
                    } else {
                        result
                    };
                    if let Slot::Waiting { tx, .. } = e.remove() {
                        let _ = tx.send(final_result);
                        return true;
                    }
                    false
                }
                Slot::Completed { .. } => {
                    // A second completion of one index: the latest wins and
                    // keeps the original store time.
                    if let Slot::Completed { at, .. } = e.get() {
                        let at = *at;
                        e.insert(Slot::Completed { result, at });
                    }
                    false
                }
            },
        }
    }

    /// Drop stored results older than the waiter window, then the oldest past
    /// the count bound. A waiter cannot register for them anymore. Install
    /// keys older than the window go too.
    fn evict(&mut self, now: Instant) {
        self.expire_covered(now);
        while let Some(&(at, key)) = self.stored.front() {
            let expired = now.saturating_duration_since(at) > self.window;
            if !expired && self.stored.len() <= MAX_STORED_RESULTS {
                break;
            }
            self.stored.pop_front();
            // Only the result this record names: a claimed slot or a newer
            // store at the same index stays.
            if let Entry::Occupied(e) = self.slots.entry(key)
                && matches!(e.get(), Slot::Completed { at: stored, .. } if *stored == at)
            {
                e.remove();
            }
        }
    }

    pub fn note_dispatched(&mut self, group_id: u64, log_index: u64) {
        self.dispatched.insert((group_id, log_index));
    }

    pub fn note_concluded(&mut self, group_id: u64, log_index: u64) {
        self.dispatched.remove(&(group_id, log_index));
    }

    /// Answer every waiter of `group_id` at or below `through` whose entry
    /// the apply loop has not dispatched, by its key against `keys`, which
    /// are complete from `complete_from`. Later registrations at or below
    /// `through` are answered the same way.
    pub fn resolve_covered(
        &mut self,
        group_id: u64,
        through: u64,
        keys: HashSet<u64>,
        complete_from: u64,
        now: Instant,
    ) {
        let window = self.window;
        match self.covered.entry(group_id) {
            Entry::Vacant(e) => {
                e.insert(Covered {
                    through,
                    keys,
                    complete_from,
                    at: now,
                });
            }
            Entry::Occupied(mut e) => {
                let covered = e.get_mut();
                // Keys of an install past the window serve no waiter.
                if now.saturating_duration_since(covered.at) > window {
                    covered.keys = keys;
                    covered.complete_from = complete_from;
                } else {
                    covered.keys.extend(keys);
                    // A gap in either install stays a gap.
                    covered.complete_from = covered.complete_from.max(complete_from);
                }
                covered.through = covered.through.max(through);
                covered.at = now;
            }
        }
        let answer: Vec<(u64, u64)> = self
            .slots
            .iter()
            .filter(|((gid, index), slot)| {
                *gid == group_id
                    && *index <= through
                    && matches!(slot, Slot::Waiting { .. })
                    && !self.dispatched.contains(&(*gid, *index))
            })
            .map(|(key, _)| *key)
            .collect();
        for (gid, index) in answer {
            if let Some(Slot::Waiting { tx, expected_key }) = self.slots.remove(&(gid, index)) {
                let _ = tx.send(self.covered_outcome(gid, index, expected_key));
            }
        }
    }

    /// Answer the waiter of an entry the apply loop skipped as covered. The
    /// entry's own key is known here. Stores nothing when no waiter exists.
    pub fn complete_covered(&mut self, group_id: u64, log_index: u64, applied_key: u64) -> bool {
        let Entry::Occupied(e) = self.slots.entry((group_id, log_index)) else {
            return false;
        };
        let Slot::Waiting { expected_key, .. } = e.get() else {
            return false;
        };
        let result = if key_mismatch(applied_key, *expected_key) {
            retryable_leader_change(group_id, log_index)
        } else {
            committed_result_unavailable(group_id, log_index)
        };
        if let Slot::Waiting { tx, .. } = e.remove() {
            let _ = tx.send(result);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::distributed_applier::propose_tracker::AppliedWrite;

    const WINDOW: Duration = Duration::from_secs(30);

    fn waiter(
        slots: &mut WaiterSlots,
        log_index: u64,
        key: u64,
    ) -> oneshot::Receiver<ProposeResult> {
        waiter_at(slots, log_index, key, Instant::now())
    }

    fn waiter_at(
        slots: &mut WaiterSlots,
        log_index: u64,
        key: u64,
        now: Instant,
    ) -> oneshot::Receiver<ProposeResult> {
        let (tx, rx) = oneshot::channel();
        slots.register(1, log_index, key, tx, now);
        rx
    }

    /// Once the waiter window passes, an install's keys are released and a
    /// waiter in its range gets an unknown outcome, even for a carried key.
    #[test]
    fn expired_install_keys_are_released_and_answer_unknown() {
        let mut slots = WaiterSlots::new(WINDOW);
        let installed = Instant::now();
        slots.resolve_covered(1, 8, HashSet::from([0xa, 0xb]), 0, installed);

        let mut fresh = waiter_at(&mut slots, 5, 0xa, installed);
        assert!(matches!(
            fresh.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { .. })
        ));

        let later = installed + WINDOW * 2;
        let mut late = waiter_at(&mut slots, 6, 0xb, later);
        assert!(matches!(
            late.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { log_index: 6, .. })
        ));
        let mut late_unkeyed = waiter_at(&mut slots, 7, 0, later);
        assert!(matches!(
            late_unkeyed.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { log_index: 7, .. })
        ));
        let covered = slots.covered.get(&1).expect("coverage kept");
        assert_eq!(covered.keys.capacity(), 0, "the key set is released");
    }

    /// A stored result past the window releases install keys too, with no
    /// register needed.
    #[test]
    fn a_later_completion_releases_expired_install_keys() {
        let mut slots = WaiterSlots::new(WINDOW);
        let installed = Instant::now();
        slots.resolve_covered(1, 8, HashSet::from([0xa]), 0, installed);
        slots.complete(1, 20, 0, applied(), installed + WINDOW * 2);
        let covered = slots.covered.get(&1).expect("coverage kept");
        assert!(covered.keys.is_empty());
        assert_eq!(covered.complete_from, KEYS_EXPIRED);
    }

    fn applied() -> ProposeResult {
        Ok(AppliedWrite::new(b"real".to_vec()))
    }

    /// A follower applies entries nobody on it waits for. Their stored
    /// results never grow past the count bound, and age out of the window.
    #[test]
    fn unclaimed_results_do_not_grow() {
        let mut slots = WaiterSlots::new(WINDOW);
        let start = Instant::now();
        let total = MAX_STORED_RESULTS as u64 + 1000;
        for index in 1..=total {
            slots.complete(1, index, 0, applied(), start);
        }
        assert_eq!(slots.stored_len(), MAX_STORED_RESULTS);

        slots.complete(1, total + 1, 0, applied(), start + WINDOW * 2);
        assert_eq!(slots.stored_len(), 1);
    }

    /// A result stored before its waiter registered reaches the waiter.
    #[test]
    fn a_late_register_claims_its_stored_result() {
        let mut slots = WaiterSlots::new(WINDOW);
        slots.complete(1, 5, 0xa, applied(), Instant::now());
        let mut rx = waiter(&mut slots, 5, 0xa);
        let result = rx.try_recv().expect("stored result delivered");
        assert_eq!(result.expect("applied").payload, b"real");
        assert_eq!(slots.stored_len(), 0);
    }

    /// Under a covering snapshot, a waiter whose key the snapshot carries
    /// committed; one whose key it lacks was overwritten and never commits.
    #[test]
    fn covered_waiters_are_answered_by_their_key() {
        let mut slots = WaiterSlots::new(WINDOW);
        let mut committed = waiter(&mut slots, 5, 0xa);
        let mut overwritten = waiter(&mut slots, 6, 0xb);
        slots.resolve_covered(1, 7, HashSet::from([0xa]), 0, Instant::now());

        assert!(matches!(
            committed.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { log_index: 5, .. })
        ));
        assert!(matches!(
            overwritten.try_recv().expect("answered"),
            Err(crate::Error::RetryableLeaderChange { log_index: 6, .. })
        ));

        let mut late_committed = waiter(&mut slots, 4, 0xa);
        assert!(matches!(
            late_committed.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { .. })
        ));
        let mut late_overwritten = waiter(&mut slots, 3, 0xc);
        assert!(matches!(
            late_overwritten.try_recv().expect("answered"),
            Err(crate::Error::RetryableLeaderChange { .. })
        ));
    }

    /// A snapshot whose keys the count bound capped answers an old waiter
    /// with an unknown outcome, and a waiter at a complete index exactly.
    #[test]
    fn a_capped_snapshot_answers_old_waiters_with_an_unknown_outcome() {
        let mut slots = WaiterSlots::new(WINDOW);
        let mut old = waiter(&mut slots, 3, 0xa);
        let mut committed = waiter(&mut slots, 6, 0xb);
        let mut overwritten = waiter(&mut slots, 7, 0xc);
        // Keys complete from index 5: the cap dropped everything below it.
        slots.resolve_covered(1, 8, HashSet::from([0xb]), 5, Instant::now());

        assert!(matches!(
            old.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { log_index: 3, .. })
        ));
        assert!(matches!(
            committed.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { log_index: 6, .. })
        ));
        assert!(matches!(
            overwritten.try_recv().expect("answered"),
            Err(crate::Error::RetryableLeaderChange { log_index: 7, .. })
        ));

        let mut late_old = waiter(&mut slots, 4, 0xd);
        assert!(matches!(
            late_old.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { .. })
        ));
        // A carried key proves the commit even below the gap.
        let mut late_carried = waiter(&mut slots, 2, 0xb);
        assert!(matches!(
            late_carried.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { .. })
        ));
    }

    /// An entry already sent to a core before the install keeps its waiter:
    /// its own completion answers it with the real result.
    #[test]
    fn a_dispatched_entry_keeps_its_waiter_through_an_install() {
        let mut slots = WaiterSlots::new(WINDOW);
        let mut rx = waiter(&mut slots, 5, 0xa);
        slots.note_dispatched(1, 5);
        slots.resolve_covered(1, 7, HashSet::from([0xa]), 0, Instant::now());
        assert!(rx.try_recv().is_err(), "the install must not answer it");

        assert!(slots.complete(1, 5, 0xa, applied(), Instant::now()));
        slots.note_concluded(1, 5);
        let result = rx.try_recv().expect("real result delivered");
        assert_eq!(result.expect("applied").payload, b"real");
    }

    /// A skipped covered entry answers its own waiter by the entry's key.
    #[test]
    fn a_skipped_covered_entry_answers_by_its_own_key() {
        let mut slots = WaiterSlots::new(WINDOW);
        let mut own = waiter(&mut slots, 5, 0xa);
        assert!(slots.complete_covered(1, 5, 0xa));
        assert!(matches!(
            own.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { .. })
        ));

        let mut replaced = waiter(&mut slots, 6, 0xa);
        assert!(slots.complete_covered(1, 6, 0xb));
        assert!(matches!(
            replaced.try_recv().expect("answered"),
            Err(crate::Error::RetryableLeaderChange { .. })
        ));

        assert!(!slots.complete_covered(1, 7, 0xc));
        assert_eq!(slots.stored_len(), 0, "no waiter, nothing stored");
    }
}
