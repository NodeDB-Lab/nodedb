// SPDX-License-Identifier: BUSL-1.1

//! Vote/verdict-tally methods for [`super::completion::CalvinCompletionRegistry`].
//!
//! This module holds every method that takes part in the cross-shard
//! commit-barrier vote tally and verdict push, plus their tests. It reads
//! `Inner` and `PendingCompletion` through `pub(crate)` fields.

use std::collections::BTreeMap;

use tokio::sync::mpsc;

use super::TxnId;
use super::completion::{AbortCause, CalvinCompletionRegistry, ParticipantVote, VerdictOutcome};
use super::sequencer::AbortReason;

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
    /// `true` for a commit decision. A participant needs only this bit to
    /// flush or drop. The abort cause reaches the coordinator through
    /// `AttemptOutcome`.
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

    /// Stop pushing verdicts to `vshard`'s scheduler: this node no longer
    /// replicates the vShard.
    pub fn unregister_verdict_signal_sender(&self, vshard: u32) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .verdict_signal_senders
            .remove(&vshard);
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
        let entry = inner.sequenced_entry_mut(txn);
        entry.expected_participants = entry.expected_participants.max(expected);
    }

    /// Record the vShards `txn` touches, from the replicated `EpochBatch` that
    /// sequenced it. A verdict reaches only these vShards' schedulers.
    pub fn seed_participants(&self, txn: TxnId, vshards: impl IntoIterator<Item = u32>) {
        let mut participants: Vec<u32> = vshards.into_iter().collect();
        participants.sort_unstable();
        participants.dedup();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.sequenced_entry_mut(txn).participants = participants;
    }

    /// Record one participant vshard's durable commit vote for `txn`, tallied
    /// from a replicated `Vote` / `AbortVote` (last write wins per `vshard`,
    /// deterministic across retries). On the transition to a complete tally it
    /// emits the aggregated verdict signal exactly once (deduped by
    /// `verdict_proposed`); the leader turns that into a replicated
    /// `SequencerEntry::Verdict` whose apply stores the durable `verdict` gating
    /// each parked participant's flush (commit) or drop (abort).
    pub fn note_vote(&self, txn_id: TxnId, vshard: u32, vote: ParticipantVote) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner.entry_mut(txn_id);
        entry.votes.insert(vshard, vote);

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
            let tally = tally_verdict(&entry.votes);
            entry.verdict_proposed = true;
            // Non-blocking: a full channel drops the signal, mirroring how the
            // apply fan-out drops on backpressure. A dropped signal is a missed
            // proposal (the leader re-drives on the next tally that isn't
            // deduped), never lost state — the verdict is stored separately via
            // `note_verdict` when the leader's proposal is applied.
            let _ = self.verdict_tx.try_send((txn_id, tally));
        }
    }

    /// Returns `(TxnId, verdict)` for every txn whose vote tally is COMPLETE but
    /// whose global `Verdict` has NOT been applied (`verdict.is_none()`), so a
    /// newly elected sequencer leader can (re-)propose it. Deliberately IGNORES
    /// `verdict_proposed` (per-node, non-durable, already `true` on a follower
    /// that tallied completeness before failover) — the durable `verdict` is the
    /// only authority for "already decided". The verdict is the aggregated
    /// tally: commit only when every participant voted commit.
    ///
    /// Read-only: it scans and returns candidates, never mutating/removing
    /// entries. A txn stays returnable until its replicated `Verdict` applies
    /// (via `note_verdict`) and sets `verdict` — so re-proposing on every leader
    /// tick self-heals and cannot loop.
    pub fn drain_unproposed_verdicts(&self) -> Vec<(TxnId, VerdictOutcome)> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner
            .completions
            .iter()
            .filter(|(_, entry)| {
                entry.expected_participants > 0
                    && entry.votes.len() == entry.expected_participants
                    && entry.verdict.is_none()
            })
            .map(|(txn, entry)| (*txn, tally_verdict(&entry.votes)))
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
    pub fn note_verdict(&self, txn: TxnId, verdict: VerdictOutcome) {
        let commit = verdict.is_commit();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let participants = {
            let entry = inner.entry_mut(txn);
            let differs = entry
                .verdict
                .as_ref()
                .map(|existing| existing.is_commit() != commit);
            match differs {
                Some(true) => {
                    tracing::warn!(
                        epoch = txn.epoch,
                        position = txn.position,
                        existing = ?entry.verdict,
                        proposed = ?verdict,
                        "calvin verdict differs from a previously applied one; \
                         determinism bug — overwriting with the latest"
                    );
                    entry.verdict = Some(verdict);
                }
                Some(false) => {}
                None => entry.verdict = Some(verdict),
            }
            entry.participants.clone()
        };

        // Push to the schedulers of the vShards the transaction touches. A
        // transaction whose participants are not seeded reaches every locally
        // registered scheduler, and each filters by its own parked
        // `(epoch, position)`. A drop on a full/closed channel is backstopped
        // by the scheduler's probe/stall re-probe.
        let signal = VerdictSignal {
            epoch: txn.epoch,
            position: txn.position,
            commit,
        };
        if participants.is_empty() {
            for tx in inner.verdict_signal_senders.values() {
                let _ = tx.try_send(signal);
            }
        } else {
            for vshard in &participants {
                if let Some(tx) = inner.verdict_signal_senders.get(vshard) {
                    let _ = tx.try_send(signal);
                }
            }
        }
        // A waiter fires here when every ack is in already.
        inner.settle(txn);
    }

    /// The parked scheduler's flush/drop gate: `Some(true)` commit, `Some(false)`
    /// abort, `None` if no verdict has been applied yet. Deliberately drops the
    /// abort cause — the cause reaches the coordinator via `AttemptOutcome`,
    /// and a participant only needs to know whether to flush.
    pub fn verdict(&self, txn_id: TxnId) -> Option<bool> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn_id)
            .and_then(|entry| entry.verdict.as_ref())
            .map(VerdictOutcome::is_commit)
    }

    /// Test/inspection accessor: returns the current per-vshard vote tally for
    /// `txn_id`, or `None` if no entry (and therefore no votes) exist yet.
    pub fn vote_tally(&self, txn_id: TxnId) -> Option<BTreeMap<u32, ParticipantVote>> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn_id)
            .map(|entry| entry.votes.clone())
    }
}

/// Aggregate a complete vote tally: commit only when every participant voted
/// commit, otherwise abort with the highest-precedence cause.
///
/// Precedence, highest first:
///
/// - `PartsLost`: no participant holds the whole transaction.
/// - `PlanRejected`: the plans fail the same way on every resubmit.
/// - `ParticipantError` with a refusal: the stage refused the slice, and a
///   resubmit refuses it the same way. The client sees that refusal.
/// - `ParticipantError` without a refusal: a peer that never staged makes
///   every other verdict unverifiable.
/// - `CollectionSuperseded`: a retry against the same read-set is pointless.
/// - `PredictionDrift`: the coordinator retries with a fresh reconnaissance.
/// - `SerializationConflict`.
///
/// Among causes of equal rank the lowest vShard wins. The votes iterate in
/// vShard order, so every replica picks the same cause, whatever the order
/// the votes applied in.
fn tally_verdict(votes: &BTreeMap<u32, ParticipantVote>) -> VerdictOutcome {
    fn rank(cause: &AbortCause) -> u8 {
        match cause.reason {
            AbortReason::PartsLost => 6,
            AbortReason::PlanRejected => 5,
            AbortReason::ParticipantError if cause.refusal.is_some() => 4,
            AbortReason::ParticipantError => 3,
            AbortReason::CollectionSuperseded => 2,
            AbortReason::PredictionDrift => 1,
            AbortReason::SerializationConflict => 0,
        }
    }
    let mut held: Option<&AbortCause> = None;
    for vote in votes.values() {
        if let ParticipantVote::Abort(cause) = vote
            && held.is_none_or(|held| rank(cause) > rank(held))
        {
            held = Some(cause);
        }
    }
    match held {
        Some(cause) => VerdictOutcome::Abort(cause.clone()),
        None => VerdictOutcome::Commit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn note_vote_tallies_one_vote_per_vshard() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(30, 0);
        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_vote(
            txn,
            2,
            ParticipantVote::Abort(AbortReason::SerializationConflict.into()),
        );
        let tally = reg.vote_tally(txn).expect("entry created by note_vote");
        assert_eq!(tally.len(), 2);
        assert_eq!(tally.get(&1), Some(&ParticipantVote::Commit));
        assert_eq!(
            tally.get(&2),
            Some(&ParticipantVote::Abort(
                AbortReason::SerializationConflict.into()
            ))
        );
    }

    #[tokio::test]
    async fn complete_all_true_tally_emits_commit_verdict_once() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(30, 1);
        // Seed the expected participant count the way the leader does.
        reg.note_assigned(1, txn, 2);

        reg.note_vote(txn, 1, ParticipantVote::Commit);
        // First vote: tally incomplete (1 of 2), nothing emitted yet.
        assert!(rx.try_recv().is_err());

        reg.note_vote(txn, 2, ParticipantVote::Commit);
        // Second vote completes the tally → commit verdict emitted exactly once.
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (txn, VerdictOutcome::Commit)
        );

        // A re-proposed vote re-tallies but must NOT emit again (dedup).
        reg.note_vote(txn, 2, ParticipantVote::Commit);
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

        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_vote(
            txn,
            2,
            ParticipantVote::Abort(AbortReason::SerializationConflict.into()),
        );
        // Any abort vote makes the aggregated verdict an abort.
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (
                txn,
                VerdictOutcome::Abort(AbortReason::SerializationConflict.into())
            )
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn participant_error_outranks_serialization_conflict_in_the_tally() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(31, 1);
        reg.note_assigned(1, txn, 2);

        // Lower-precedence reason first, so the winner cannot be "last write".
        reg.note_vote(
            txn,
            1,
            ParticipantVote::Abort(AbortReason::SerializationConflict.into()),
        );
        reg.note_vote(
            txn,
            2,
            ParticipantVote::Abort(AbortReason::ParticipantError.into()),
        );
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (
                txn,
                VerdictOutcome::Abort(AbortReason::ParticipantError.into())
            ),
            "a peer that never staged makes the stale read-set unverifiable"
        );
    }

    #[tokio::test]
    async fn participant_error_outranks_a_later_serialization_conflict() {
        let (tx, mut rx) = mpsc::channel(8);
        let reg = CalvinCompletionRegistry::new(tx);
        let txn = TxnId::new(31, 2);
        reg.note_assigned(1, txn, 2);

        // Reverse arrival order: precedence must not depend on vote order.
        reg.note_vote(
            txn,
            1,
            ParticipantVote::Abort(AbortReason::ParticipantError.into()),
        );
        reg.note_vote(
            txn,
            2,
            ParticipantVote::Abort(AbortReason::SerializationConflict.into()),
        );
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (
                txn,
                VerdictOutcome::Abort(AbortReason::ParticipantError.into())
            )
        );
    }

    /// Tally `votes`, the vote of vShard `1`, `2`, ... in turn.
    fn tally<const N: usize>(votes: [&ParticipantVote; N]) -> VerdictOutcome {
        let votes: BTreeMap<u32, ParticipantVote> = (1..).zip(votes.into_iter().cloned()).collect();
        tally_verdict(&votes)
    }

    fn aborted(reason: AbortReason) -> VerdictOutcome {
        VerdictOutcome::Abort(reason.into())
    }

    /// A superseded collection aborts a transaction other slices voted to
    /// commit, outranks a conflict, and yields to a participant error, in any
    /// vote order.
    #[test]
    fn a_superseded_collection_aborts_the_whole_transaction() {
        let superseded = ParticipantVote::Abort(AbortReason::CollectionSuperseded.into());
        let conflict = ParticipantVote::Abort(AbortReason::SerializationConflict.into());
        let errored = ParticipantVote::Abort(AbortReason::ParticipantError.into());

        assert_eq!(
            tally([&ParticipantVote::Commit, &superseded]),
            aborted(AbortReason::CollectionSuperseded)
        );
        for votes in [[&superseded, &conflict], [&conflict, &superseded]] {
            assert_eq!(tally(votes), aborted(AbortReason::CollectionSuperseded));
        }
        for votes in [[&superseded, &errored], [&errored, &superseded]] {
            assert_eq!(tally(votes), aborted(AbortReason::ParticipantError));
        }
    }

    /// A rejected plan outranks every staged abort cause, in any vote order.
    #[test]
    fn plan_rejected_outranks_staged_abort_causes() {
        let rejected = ParticipantVote::Abort(AbortReason::PlanRejected.into());
        for other in [
            AbortCause::from(AbortReason::SerializationConflict),
            AbortCause::from(AbortReason::PredictionDrift),
            AbortCause::from(AbortReason::CollectionSuperseded),
            AbortCause::from(AbortReason::ParticipantError),
            AbortCause::refused(vec![1, 2, 3]),
        ] {
            let other = ParticipantVote::Abort(other);
            for votes in [[&rejected, &other], [&other, &rejected]] {
                assert_eq!(
                    tally(votes),
                    aborted(AbortReason::PlanRejected),
                    "{other:?}"
                );
            }
        }
        let lost = ParticipantVote::Abort(AbortReason::PartsLost.into());
        for votes in [[&rejected, &lost], [&lost, &rejected]] {
            assert_eq!(tally(votes), aborted(AbortReason::PartsLost));
        }
    }

    /// A refusal outranks a participant that never staged, a superseded
    /// collection, a drifted prediction and a conflict, in any vote order.
    #[test]
    fn a_refusal_outranks_every_unrefused_participant_cause() {
        let refused = ParticipantVote::Abort(AbortCause::refused(vec![7]));
        for other in [
            AbortReason::ParticipantError,
            AbortReason::CollectionSuperseded,
            AbortReason::PredictionDrift,
            AbortReason::SerializationConflict,
        ] {
            let other = ParticipantVote::Abort(other.into());
            for votes in [[&refused, &other], [&other, &refused]] {
                assert_eq!(
                    tally(votes),
                    VerdictOutcome::Abort(AbortCause::refused(vec![7])),
                    "{other:?}"
                );
            }
        }
    }

    /// Two participants refuse with different refusals. Every replica picks
    /// the lowest vShard's refusal, whatever order the votes applied in.
    #[tokio::test]
    async fn two_refusals_choose_the_lowest_vshard_on_every_replica() {
        let txn = TxnId::new(32, 0);
        let low = ParticipantVote::Abort(AbortCause::refused(b"vshard 3".to_vec()));
        let high = ParticipantVote::Abort(AbortCause::refused(b"vshard 9".to_vec()));
        let orders: [[(u32, &ParticipantVote); 3]; 2] = [
            [(3, &low), (5, &ParticipantVote::Commit), (9, &high)],
            [(9, &high), (5, &ParticipantVote::Commit), (3, &low)],
        ];
        for order in orders {
            let (tx, mut rx) = mpsc::channel(8);
            let replica = CalvinCompletionRegistry::new(tx);
            replica.seed_expected(txn, 3);
            for (vshard, vote) in order {
                replica.note_vote(txn, vshard, vote.clone());
            }
            let expected = VerdictOutcome::Abort(AbortCause::refused(b"vshard 3".to_vec()));
            assert_eq!(
                rx.try_recv().expect("verdict emitted"),
                (txn, expected.clone()),
                "{order:?}"
            );
            assert_eq!(
                replica.drain_unproposed_verdicts(),
                vec![(txn, expected)],
                "{order:?}"
            );
        }
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

        reg.note_vote(txn, 1, ParticipantVote::Commit);
        assert!(
            rx.try_recv().is_err(),
            "only 1 of 2 expected votes in; must not emit yet"
        );

        reg.note_vote(txn, 2, ParticipantVote::Commit);
        assert_eq!(
            rx.try_recv().expect("verdict emitted"),
            (txn, VerdictOutcome::Commit),
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
        reg.note_vote(txn_a, 1, ParticipantVote::Commit);
        reg.note_vote(txn_a, 2, ParticipantVote::Commit);
        assert!(
            rx.try_recv().is_err(),
            "expected_participants must be max(3, 1) = 3; 2 votes is not complete"
        );
        reg.note_vote(txn_a, 3, ParticipantVote::Commit);
        assert_eq!(
            rx.try_recv().expect("verdict emitted at the 3rd vote"),
            (txn_a, VerdictOutcome::Commit)
        );

        // note_assigned first (smaller), then seed_expected with a larger count:
        // same invariant, opposite call order.
        let txn_b = TxnId::new(41, 1);
        reg.note_assigned(2, txn_b, 1);
        reg.seed_expected(txn_b, 3);
        reg.note_vote(txn_b, 1, ParticipantVote::Commit);
        reg.note_vote(txn_b, 2, ParticipantVote::Commit);
        assert!(
            rx.try_recv().is_err(),
            "expected_participants must be max(1, 3) = 3, not the smaller seed"
        );
        reg.note_vote(txn_b, 3, ParticipantVote::Commit);
        assert_eq!(
            rx.try_recv().expect("verdict emitted at the 3rd vote"),
            (txn_b, VerdictOutcome::Commit)
        );
    }

    #[tokio::test]
    async fn note_verdict_pushes_signal_to_registered_scheduler() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx, mut sig_rx) = mpsc::channel(8);
        reg.register_verdict_signal_sender(7, tx);

        let txn = TxnId::new(50, 2);
        reg.note_verdict(txn, VerdictOutcome::Commit);

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
    async fn note_verdict_reaches_only_the_vshards_the_txn_touches() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        let (tx3, mut rx3) = mpsc::channel(8);
        reg.register_verdict_signal_sender(1, tx1);
        reg.register_verdict_signal_sender(2, tx2);
        reg.register_verdict_signal_sender(3, tx3);

        let txn = TxnId::new(52, 0);
        reg.seed_participants(txn, [2, 1, 9]);
        reg.note_verdict(txn, VerdictOutcome::Commit);

        assert!(rx1.try_recv().expect("push to vshard 1").commit);
        assert!(rx2.try_recv().expect("push to vshard 2").commit);
        assert!(rx3.try_recv().is_err(), "vshard 3 does not take part");
    }

    #[tokio::test]
    async fn note_verdict_broadcasts_to_all_registered_schedulers() {
        let reg = CalvinCompletionRegistry::new_detached();
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        reg.register_verdict_signal_sender(1, tx1);
        reg.register_verdict_signal_sender(2, tx2);

        let txn = TxnId::new(51, 0);
        reg.note_verdict(
            txn,
            VerdictOutcome::Abort(AbortReason::SerializationConflict.into()),
        );

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
        reg.note_verdict(filler, VerdictOutcome::Commit);
        // Buffer now holds one signal; do NOT drain it.

        let txn = TxnId::new(60, 1);
        reg.note_verdict(txn, VerdictOutcome::Commit);
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
        reg.note_verdict(
            txn,
            VerdictOutcome::Abort(AbortReason::SerializationConflict.into()),
        );
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
        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_vote(txn, 2, ParticipantVote::Commit);
        // Tally complete (verdict_proposed now set) but NO verdict stored — the
        // `Verdict` entry never committed before failover.
        assert_eq!(reg.verdict(txn), None);
        assert_eq!(
            reg.drain_unproposed_verdicts(),
            vec![(txn, VerdictOutcome::Commit)],
            "a complete all-commit tally with no stored verdict must be drainable \
             despite verdict_proposed already being set"
        );

        // Once the re-proposed `Verdict` applies, the txn stops draining — idempotent
        // stop, so re-driving on every tick cannot loop.
        reg.note_verdict(txn, VerdictOutcome::Commit);
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
        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_vote(
            txn,
            2,
            ParticipantVote::Abort(AbortReason::SerializationConflict.into()),
        );
        assert_eq!(reg.verdict(txn), None);
        assert_eq!(
            reg.drain_unproposed_verdicts(),
            vec![(
                txn,
                VerdictOutcome::Abort(AbortReason::SerializationConflict.into())
            )],
            "any abort vote makes the re-proposed verdict an abort"
        );
    }

    #[tokio::test]
    async fn drain_ignores_incomplete_tallies() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(72, 0);
        reg.seed_expected(txn, 2);
        reg.note_vote(txn, 1, ParticipantVote::Commit);
        // Only 1 of 2 expected votes in: not complete, must not be drainable.
        assert!(
            reg.drain_unproposed_verdicts().is_empty(),
            "an incomplete tally must never be re-proposed as a verdict"
        );
    }
}
