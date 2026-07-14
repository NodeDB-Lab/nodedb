// SPDX-License-Identifier: BUSL-1.1

//! Vote/verdict-tally methods for [`super::completion::CalvinCompletionRegistry`].
//!
//! Split out of `completion.rs` (which hit the file-size limit): this module
//! holds every method that participates in the cross-shard commit-barrier
//! vote tally and verdict push, plus their tests. It reaches into
//! `completion.rs`'s otherwise-private `Inner` / `PendingCompletion` internals
//! via `pub(crate)` fields — no visibility is widened beyond this crate.

use std::collections::BTreeMap;

use tokio::sync::mpsc;

use super::TxnId;
use super::completion::{CalvinCompletionRegistry, PendingCompletion};

/// Push notification that a staged cross-shard txn's authoritative global
/// verdict is now durable on this node.
///
/// Emitted by [`CalvinCompletionRegistry::note_verdict`] to every locally
/// registered per-vShard Calvin scheduler (broadcast). A scheduler parked in
/// `AwaitingVerdict` matches by its parked `(epoch, position)` and resumes its
/// flush (commit) or drop (abort). This is a latency optimization only: a
/// dropped signal (full/closed channel) is backstopped by the scheduler's
/// probe-on-park and its stall re-probe sweep — the durable verdict stored on
/// the same registry mutex is the source of truth, never this signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerdictSignal {
    pub epoch: u64,
    pub position: u32,
    pub commit: bool,
}

impl CalvinCompletionRegistry {
    /// Register a per-vShard scheduler's verdict-push sender.
    ///
    /// Called once per hosted vShard when its Calvin scheduler is constructed.
    /// `note_verdict` broadcasts to every registered sender; a scheduler filters
    /// by its own parked `(epoch, position)`, so registering all local vShards
    /// on one broadcast list is correct (and matches the read-result sender
    /// registry's per-vShard registration shape). A re-registration for the same
    /// vShard replaces the prior sender.
    pub fn register_verdict_signal_sender(&self, vshard: u32, tx: mpsc::Sender<VerdictSignal>) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .verdict_signal_senders
            .insert(vshard, tx);
    }

    /// Seed the expected participant count for `txn` deterministically from the
    /// replicated `SequencerEntry::EpochBatch` — this runs on every replica (not
    /// just the epoch's originating leader), so vote-completeness becomes
    /// detectable even on a replica that later becomes leader via failover and
    /// never observed the original `note_assigned` seeding.
    ///
    /// Idempotent: takes the max with any existing seed, so ordering against
    /// `note_assigned` / `register_completion` (which also seed this field) is
    /// safe regardless of which one runs first. `expected == 0` is a harmless
    /// no-op (max with the existing value).
    pub fn seed_expected(&self, txn: TxnId, expected: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.expected_participants = entry.expected_participants.max(expected);
    }

    /// Record one participant vshard's durable commit vote for `txn`, tallied
    /// from a replicated `SequencerEntry::Vote` (last write wins per `vshard`,
    /// deterministic across retries). On the transition to a complete tally it
    /// emits the aggregated verdict signal exactly once (deduped by
    /// `verdict_proposed`); the leader turns that into a replicated
    /// `SequencerEntry::Verdict` whose apply stores the durable `verdict` gating
    /// each parked participant's flush (commit) or drop (abort).
    pub fn note_vote(&self, txn_id: TxnId, vshard: u32, commit: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn_id)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.votes.insert(vshard, commit);

        // On the transition to a complete tally, emit the aggregated verdict
        // exactly once. `expected_participants` is seeded deterministically on
        // every replica from the `EpochBatch` apply arm (see `seed_expected`),
        // as well as opportunistically by `note_assigned` / `register_completion`
        // on the leader/coordinator — so completeness is detectable here on any
        // replica, including one that becomes leader via failover after the
        // epoch was originally applied elsewhere. Only the leader's
        // `SequencerService` actually proposes the verdict from this signal
        // (leader-gated at the propose site); a follower computing and sending
        // the signal here is harmless. The `verdict_proposed` guard dedups a
        // re-tally caused by a re-proposed vote on retry.
        if entry.expected_participants > 0
            && entry.votes.len() == entry.expected_participants
            && !entry.verdict_proposed
        {
            let commit_all = entry.votes.values().all(|&v| v);
            entry.verdict_proposed = true;
            // Non-blocking: a full channel drops the signal, mirroring how the
            // apply fan-out drops on backpressure. A dropped signal is a missed
            // proposal (the leader re-drives on the next tally that isn't
            // deduped), never lost state — the verdict is stored separately via
            // `note_verdict` when the leader's proposal is applied.
            let _ = self.verdict_tx.try_send((txn_id, commit_all));
        }
    }

    /// Returns `(TxnId, commit)` for every txn whose vote tally is COMPLETE but
    /// whose global `Verdict` has NOT been applied (`verdict.is_none()`), so a
    /// newly elected sequencer leader can (re-)propose it. Deliberately IGNORES
    /// `verdict_proposed` (per-node, non-durable, already `true` on a follower
    /// that tallied completeness before failover) — the durable `verdict` is the
    /// only authority for "already decided". `commit = all votes true`.
    ///
    /// Read-only: it scans and returns candidates, never mutating/removing
    /// entries. A txn stays returnable until its replicated `Verdict` applies
    /// (via `note_verdict`) and sets `verdict` — so re-proposing on every leader
    /// tick self-heals and cannot loop.
    pub fn drain_unproposed_verdicts(&self) -> Vec<(TxnId, bool)> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner
            .completions
            .iter()
            .filter(|(_, entry)| {
                entry.expected_participants > 0
                    && entry.votes.len() == entry.expected_participants
                    && entry.verdict.is_none()
            })
            .map(|(txn, entry)| (*txn, entry.votes.values().all(|&v| v)))
            .collect()
    }

    /// Store the authoritative commit/abort verdict for `txn`, applied from a
    /// replicated `SequencerEntry::Verdict` on every replica, then PUSH the
    /// verdict to every locally parked Calvin scheduler.
    ///
    /// Idempotent: re-applying the same verdict is a no-op. A verdict that
    /// differs from a previously stored one is a determinism bug (the tally is
    /// computed deterministically from replicated votes) — it is logged at
    /// `warn` and the latest value is stored.
    ///
    /// Store-and-notify run under the ONE `inner` mutex so a scheduler that
    /// probes `verdict(txn)` after the store is guaranteed to see it, and the
    /// push (buffered in each scheduler's bounded channel) covers a scheduler
    /// that probed just before the store. A `try_send` that fails (full/closed
    /// channel) is a non-fatal drop — the scheduler's stall re-probe backstops
    /// it — never a block or panic, mirroring the apply fan-out drop discipline.
    pub fn note_verdict(&self, txn: TxnId, commit: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        {
            let entry = inner
                .completions
                .entry(txn)
                .or_insert_with(|| PendingCompletion::new(0));
            match entry.verdict {
                Some(existing) if existing != commit => {
                    tracing::warn!(
                        epoch = txn.epoch,
                        position = txn.position,
                        existing,
                        proposed = commit,
                        "calvin verdict differs from a previously applied one; \
                         determinism bug — overwriting with the latest"
                    );
                    entry.verdict = Some(commit);
                }
                Some(_) => {}
                None => entry.verdict = Some(commit),
            }
        }

        // Broadcast the push to every locally registered scheduler. Each filters
        // by its own parked `(epoch, position)`; a drop on a full/closed channel
        // is backstopped by the scheduler's probe/stall re-probe.
        let signal = VerdictSignal {
            epoch: txn.epoch,
            position: txn.position,
            commit,
        };
        for tx in inner.verdict_signal_senders.values() {
            let _ = tx.try_send(signal);
        }
    }

    /// Test/inspection accessor: returns the stored commit/abort verdict for
    /// `txn_id`, or `None` if no verdict has been applied yet.
    pub fn verdict(&self, txn_id: TxnId) -> Option<bool> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn_id)
            .and_then(|entry| entry.verdict)
    }

    /// Test/inspection accessor: returns the current per-vshard vote tally for
    /// `txn_id`, or `None` if no entry (and therefore no votes) exist yet.
    pub fn vote_tally(&self, txn_id: TxnId) -> Option<BTreeMap<u32, bool>> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn_id)
            .map(|entry| entry.votes.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn note_vote_tallies_one_vote_per_vshard() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(30, 0);
        reg.note_vote(txn, 1, true);
        reg.note_vote(txn, 2, false);
        let tally = reg.vote_tally(txn).expect("entry created by note_vote");
        assert_eq!(tally.len(), 2);
        assert_eq!(tally.get(&1), Some(&true));
        assert_eq!(tally.get(&2), Some(&false));
    }

    #[tokio::test]
    async fn complete_all_true_tally_emits_commit_verdict_once() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(30, 1);
        // Seed the expected participant count the way the leader does.
        reg.note_assigned(1, txn, 2);

        reg.note_vote(txn, 1, true);
        // First vote: tally incomplete (1 of 2), nothing emitted yet.
        assert!(rx.try_recv().is_err());

        reg.note_vote(txn, 2, true);
        // Second vote completes the tally → commit verdict emitted exactly once.
        assert_eq!(rx.try_recv().expect("verdict emitted"), (txn, true));

        // A re-proposed vote re-tallies but must NOT emit again (dedup).
        reg.note_vote(txn, 2, true);
        assert!(
            rx.try_recv().is_err(),
            "dedup: verdict must emit only on the first complete tally"
        );
    }

    #[tokio::test]
    async fn complete_tally_with_one_abort_emits_abort_verdict() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(31, 0);
        reg.note_assigned(1, txn, 2);

        reg.note_vote(txn, 1, true);
        reg.note_vote(txn, 2, false);
        // Any abort vote makes the aggregated verdict false.
        assert_eq!(rx.try_recv().expect("verdict emitted"), (txn, false));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn seed_expected_is_idempotent_max_and_enables_verdict_without_note_assigned() {
        // Mirrors complete_all_true_tally_emits_commit_verdict_once, but seeds
        // expected_participants via seed_expected (the EpochBatch apply-arm path
        // that runs on every replica) instead of the leader-only note_assigned.
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(40, 0);

        // Seeding a smaller value after a larger one must not shrink the count.
        reg.seed_expected(txn, 2);
        reg.seed_expected(txn, 1);

        reg.note_vote(txn, 1, true);
        assert!(
            rx.try_recv().is_err(),
            "only 1 of 2 expected votes in; must not emit yet"
        );

        reg.note_vote(txn, 2, true);
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (txn, true),
            "seed_expected alone (no note_assigned) must make completeness detectable"
        );
    }

    #[tokio::test]
    async fn seed_expected_and_note_assigned_take_the_max_regardless_of_order() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);

        // seed_expected first (larger), then note_assigned with a smaller count:
        // the max (3) must win, so 2 votes must NOT be enough to emit a verdict.
        let txn_a = TxnId::new(41, 0);
        reg.seed_expected(txn_a, 3);
        reg.note_assigned(1, txn_a, 1);
        reg.note_vote(txn_a, 1, true);
        reg.note_vote(txn_a, 2, true);
        assert!(
            rx.try_recv().is_err(),
            "expected_participants must be max(3, 1) = 3; 2 votes is not complete"
        );
        reg.note_vote(txn_a, 3, true);
        assert_eq!(
            rx.try_recv().expect("verdict emitted at the 3rd vote"),
            (txn_a, true)
        );

        // note_assigned first (smaller), then seed_expected with a larger count:
        // same invariant, opposite call order.
        let txn_b = TxnId::new(41, 1);
        reg.note_assigned(2, txn_b, 1);
        reg.seed_expected(txn_b, 3);
        reg.note_vote(txn_b, 1, true);
        reg.note_vote(txn_b, 2, true);
        assert!(
            rx.try_recv().is_err(),
            "expected_participants must be max(1, 3) = 3, not the smaller seed"
        );
        reg.note_vote(txn_b, 3, true);
        assert_eq!(
            rx.try_recv().expect("verdict emitted at the 3rd vote"),
            (txn_b, true)
        );
    }

    #[tokio::test]
    async fn note_verdict_pushes_signal_to_registered_scheduler() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx, mut sig_rx) = mpsc::channel(8);
        reg.register_verdict_signal_sender(7, tx);

        let txn = TxnId::new(50, 2);
        reg.note_verdict(txn, true);

        // The stored verdict and the pushed signal must agree.
        assert_eq!(reg.verdict(txn), Some(true));
        assert_eq!(
            sig_rx.try_recv().expect("verdict signal pushed"),
            VerdictSignal {
                epoch: 50,
                position: 2,
                commit: true,
            }
        );
    }

    #[tokio::test]
    async fn note_verdict_broadcasts_to_all_registered_schedulers() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        reg.register_verdict_signal_sender(1, tx1);
        reg.register_verdict_signal_sender(2, tx2);

        let txn = TxnId::new(51, 0);
        reg.note_verdict(txn, false);

        // Both locally registered vShard schedulers receive the broadcast; each
        // filters by its own parked (epoch, position).
        assert!(!rx1.try_recv().expect("push to vshard 1").commit);
        assert!(!rx2.try_recv().expect("push to vshard 2").commit);
    }

    #[tokio::test]
    async fn note_verdict_with_full_channel_drops_signal_but_stores_verdict() {
        let reg = CalvinCompletionRegistry::new_detached();
        // Capacity-1 channel, pre-filled so the next try_send fails.
        let (tx, mut rx) = mpsc::channel(1);
        reg.register_verdict_signal_sender(9, tx);
        let filler = TxnId::new(60, 0);
        reg.note_verdict(filler, true);
        // Buffer now holds one signal; do NOT drain it.

        let txn = TxnId::new(60, 1);
        reg.note_verdict(txn, true);
        // The push was dropped (channel full) but the durable verdict is stored —
        // the scheduler's stall re-probe reads it back. No panic, no block.
        assert_eq!(reg.verdict(txn), Some(true));
        // Only the first (filler) signal is buffered; the second was dropped.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn note_verdict_with_closed_channel_is_non_fatal() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx, rx) = mpsc::channel::<VerdictSignal>(8);
        reg.register_verdict_signal_sender(3, tx);
        drop(rx); // scheduler exited: receiver gone.

        let txn = TxnId::new(61, 0);
        // Must not panic despite the closed channel; the verdict still stores.
        reg.note_verdict(txn, false);
        assert_eq!(reg.verdict(txn), Some(false));
    }

    #[tokio::test]
    async fn drain_reproposes_complete_but_unstored_verdict_after_failover() {
        // Failover gap this locks: a follower applied the committed `Vote` entries,
        // so `note_vote` reached completeness, set `verdict_proposed`, and emitted a
        // signal the non-leader service dropped. The old leader then died BEFORE its
        // `Verdict` entry committed — so no verdict is stored anywhere. When the
        // follower promotes, the local emit path is deduped (`verdict_proposed`) and
        // never re-fires; only a leader-driven rescan of the durable votes can
        // recover. `drain_unproposed_verdicts` is that rescan.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(70, 0);
        reg.seed_expected(txn, 2);
        reg.note_vote(txn, 1, true);
        reg.note_vote(txn, 2, true);
        // Tally complete (verdict_proposed now set) but NO verdict stored — the
        // `Verdict` entry never committed before failover.
        assert_eq!(reg.verdict(txn), None);
        assert_eq!(
            reg.drain_unproposed_verdicts(),
            vec![(txn, true)],
            "a complete all-commit tally with no stored verdict must be drainable \
             despite verdict_proposed already being set"
        );

        // Once the re-proposed `Verdict` applies, the txn stops draining — idempotent
        // stop, so re-driving on every tick cannot loop.
        reg.note_verdict(txn, true);
        assert!(
            reg.drain_unproposed_verdicts().is_empty(),
            "a stored verdict must stop the txn from being re-drained"
        );
    }

    #[tokio::test]
    async fn drain_returns_abort_when_a_participant_voted_abort() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(71, 0);
        reg.seed_expected(txn, 2);
        reg.note_vote(txn, 1, true);
        reg.note_vote(txn, 2, false);
        assert_eq!(reg.verdict(txn), None);
        assert_eq!(
            reg.drain_unproposed_verdicts(),
            vec![(txn, false)],
            "any abort vote makes the re-proposed verdict false"
        );
    }

    #[tokio::test]
    async fn drain_ignores_incomplete_tallies() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(72, 0);
        reg.seed_expected(txn, 2);
        reg.note_vote(txn, 1, true);
        // Only 1 of 2 expected votes in: not complete, must not be drainable.
        assert!(
            reg.drain_unproposed_verdicts().is_empty(),
            "an incomplete tally must never be re-proposed as a verdict"
        );
    }
}
