// SPDX-License-Identifier: BUSL-1.1

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::calvin::completion_entry::PendingCompletion;
use crate::calvin::completion_waiter::{CompletionReport, CompletionWaiter};
use crate::calvin::sequencer::AbortReason;

/// The sequencer assignment of one submission: `(epoch, position,
/// participants)`.
pub type Assignment = (u64, u32, usize);

/// Receives one submission's [`Assignment`]. It reads a closed channel when
/// the sequencer rejected or discarded the submission without sequencing it.
pub type AssignmentReceiver = oneshot::Receiver<Assignment>;

/// Calvin transaction identity in the sequencer-assigned coordinate space.
///
/// `(epoch, position)` is the unique key the sequencer Raft state machine
/// stamps onto every admitted transaction; it is the join key between the
/// completion-awaiter side and the per-vshard ack side of the registry.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct TxnId {
    pub epoch: u64,
    pub position: u32,
}

impl TxnId {
    pub fn new(epoch: u64, position: u32) -> Self {
        Self { epoch, position }
    }
}

/// Terminal outcome of a single Calvin transaction attempt.
///
/// Exactly one fires per attempt, once the global verdict is stored and every
/// expected vShard acked. A transaction that lost its parts fires at its
/// verdict, since no participant staged it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The global verdict was COMMIT.
    Completed,
    /// The global verdict was ABORT. `reason` names the cause.
    Aborted { reason: AbortReason },
}

/// One participant vShard's durable commit vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantVote {
    Commit,
    Abort(AbortReason),
}

/// A commit/abort decision: the tally aggregated from votes, and the
/// authoritative verdict stored on a `PendingCompletion`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerdictOutcome {
    Commit,
    Abort(AbortReason),
}

impl VerdictOutcome {
    /// `true` for a commit decision. The scheduler's flush/drop gate needs only
    /// this bit; the reason travels to the coordinator instead.
    pub fn is_commit(self) -> bool {
        matches!(self, Self::Commit)
    }
}

/// What this node's registry has applied for one participant vShard of a txn.
///
/// A scheduler reads it to learn whether a sequencer entry it proposed has
/// been applied here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantProgress {
    /// A `Vote` or `AbortVote` from this vShard is in the tally.
    pub voted: bool,
    /// A `CompletionAck` from this vShard is recorded.
    pub acked: bool,
    /// The txn's global verdict is stored.
    pub has_verdict: bool,
}

/// `pub(crate)`: also read by the vote/verdict-tally methods in
/// `completion_verdict.rs` (a sibling module in the same crate).
#[derive(Default)]
pub(crate) struct Inner {
    assignments: BTreeMap<u64, oneshot::Sender<Assignment>>,
    pub(crate) completions: BTreeMap<TxnId, PendingCompletion>,
    /// Per-vShard senders for the verdict push, keyed by vShard id. Each local
    /// Calvin scheduler registers its receiver's sender here at construction;
    /// `note_verdict` broadcasts a [`super::completion_verdict::VerdictSignal`]
    /// to all of them under this same mutex, so a stored verdict and its push
    /// notification never disagree.
    pub(crate) verdict_signal_senders:
        BTreeMap<u32, mpsc::Sender<super::completion_verdict::VerdictSignal>>,
    /// Terminal entries with no waiter, oldest first, with the instant each
    /// became one (`completion_gc`).
    pub(crate) waiterless: VecDeque<(Instant, TxnId)>,
    /// Entries no `EpochBatch` apply sequenced, oldest first, with the
    /// instant each was created (`completion_gc`).
    pub(crate) orphans: VecDeque<(Instant, TxnId)>,
    /// How long a terminal entry waits for a waiter before eviction, and how
    /// long an orphan stays with no live waiter. `None` takes
    /// [`super::completion_gc::DEFAULT_WAITERLESS_TTL`].
    pub(crate) waiterless_ttl: Option<Duration>,
}

impl Inner {
    /// Deliver `txn`'s outcome to its waiter once the entry is terminal, then
    /// queue or evict waiterless terminal entries.
    ///
    /// The entry stays after its outcome fires. Its votes, verdict, and acks
    /// answer the participants on this node that still probe it. The
    /// waiterless sweep removes it.
    pub(crate) fn settle(&mut self, txn: TxnId) {
        if let Some(entry) = self.completions.get_mut(&txn)
            && entry.is_terminal()
            && let Some(verdict) = entry.verdict
            && let Some(waiter) = entry.completion_tx.take()
        {
            let results = entry.take_ack_results();
            if !waiter.send(outcome_for(verdict), results) {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before its outcome fired; \
                     client likely timed out on completion wait"
                );
            }
        }
        self.settle_waiterless(txn);
    }
}

/// The coordinator's outcome for a stored verdict.
fn outcome_for(verdict: VerdictOutcome) -> AttemptOutcome {
    match verdict {
        VerdictOutcome::Commit => AttemptOutcome::Completed,
        VerdictOutcome::Abort(reason) => AttemptOutcome::Aborted { reason },
    }
}

pub struct CalvinCompletionRegistry {
    /// `pub(crate)`: also locked by the vote/verdict-tally methods in
    /// `completion_verdict.rs` (a sibling module in the same crate); never
    /// exposed beyond the crate.
    pub(crate) inner: Mutex<Inner>,
    /// Emits `(txn, verdict)` exactly once when a staged cross-shard txn's vote
    /// tally becomes complete (all expected participants voted). The paired
    /// receiver lives in the `SequencerService`, whose leader-guarded arm turns
    /// the signal into a `SequencerEntry::Verdict` proposal. `pub(crate)`: also
    /// used by `note_vote` in `completion_verdict.rs`.
    pub(crate) verdict_tx: mpsc::Sender<(TxnId, VerdictOutcome)>,
    /// Completion acks this node applied, with their Raft index.
    pub applied_acks: super::applied_acks::AppliedAckLog,
}

impl CalvinCompletionRegistry {
    /// Construct a registry wired to a verdict signal channel. The paired
    /// receiver must be handed to the `SequencerService` on this node so the
    /// leader can propose the aggregated verdict.
    pub fn new(verdict_tx: mpsc::Sender<(TxnId, VerdictOutcome)>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            verdict_tx,
            applied_acks: super::applied_acks::AppliedAckLog::default(),
        })
    }

    /// Construct a registry with no verdict consumer: the signal channel is
    /// created internally and its receiver dropped, so vote-complete transitions
    /// are still computed and stored but never delivered to a sequencer service.
    /// For callers (and tests) that do not drive verdict proposal.
    pub fn new_detached() -> Arc<Self> {
        let (verdict_tx, _verdict_rx) = mpsc::channel(1);
        Self::new(verdict_tx)
    }

    /// Register interest in the assignment of submission `inbox_seq`.
    ///
    /// Call it before the submission reaches the inbox channel, so the tick
    /// that drains it always finds the sender. `Inbox::submit_with` does so.
    pub fn register_submission(&self, inbox_seq: u64) -> AssignmentReceiver {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .insert(inbox_seq, tx);
        rx
    }

    /// Drop the assignment sender of submission `inbox_seq`, if one is held.
    ///
    /// The sequencer calls it for every submission it rejects or discards
    /// without sequencing. The waiting caller then reads a closed channel at
    /// once. A caller that gives up waiting calls it too, so no sender stays
    /// behind.
    pub fn drop_assignment(&self, inbox_seq: u64) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .remove(&inbox_seq);
    }

    pub fn note_assigned(&self, inbox_seq: u64, txn: TxnId, expected_participants: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = inner.assignments.remove(&inbox_seq)
            && tx
                .send((txn.epoch, txn.position, expected_participants))
                .is_err()
        {
            tracing::warn!(
                inbox_seq,
                epoch = txn.epoch,
                position = txn.position,
                "calvin assignment receiver dropped before sequencer position arrived; \
                 client likely timed out on submit_with_retry"
            );
        }
        let entry = inner.entry_mut(txn);
        entry.expected_participants = entry.expected_participants.max(expected_participants);
    }

    /// Register interest in `txn`'s terminal outcome, seeding the authoritative
    /// `expected_participants` from the (routed) assignment.
    ///
    /// Cross-node, the coordinator's registry never receives `note_assigned` —
    /// that fires only on the sequencer leader — so the participant count arrives
    /// here, via `RoutedAssignment.participants`. `max` upgrades the unknown (0)
    /// default and is idempotent when `note_assigned` already seeded it single-node.
    pub fn register_completion(
        &self,
        txn: TxnId,
        expected_participants: usize,
    ) -> oneshot::Receiver<AttemptOutcome> {
        let (tx, rx) = oneshot::channel();
        self.register_waiter(txn, expected_participants, CompletionWaiter::Outcome(tx));
        rx
    }

    /// [`Self::register_completion`] for a coordinator that also reads each
    /// participant's apply result, as its `CompletionAck` carried it. The
    /// acks reach every sequencer replica, so the results arrive whether or
    /// not this node hosts a replica of each participant.
    pub fn register_completion_report(
        &self,
        txn: TxnId,
        expected_participants: usize,
    ) -> oneshot::Receiver<CompletionReport> {
        let (tx, rx) = oneshot::channel();
        self.register_waiter(txn, expected_participants, CompletionWaiter::Report(tx));
        rx
    }

    /// Store `tx` as `txn`'s waiter. An entry that is terminal already, from
    /// a verdict and acks that raced ahead of registration, fires at once.
    fn register_waiter(&self, txn: TxnId, expected_participants: usize, tx: CompletionWaiter) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner.entry_mut(txn);
        entry.expected_participants = entry.expected_participants.max(expected_participants);
        entry.completion_tx = Some(tx);
        inner.settle(txn);
    }

    pub fn note_completion_ack(&self, txn: TxnId, vshard_id: u32) {
        self.note_completion_ack_with(txn, vshard_id, Vec::new());
    }

    /// Record `vshard_id`'s `CompletionAck` for `txn`, with the apply result
    /// it carried. An empty `result` records none.
    pub fn note_completion_ack_with(&self, txn: TxnId, vshard_id: u32, result: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner.entry_mut(txn);
        entry.acked_vshards.insert(vshard_id);
        if !result.is_empty() {
            entry.ack_results.insert(vshard_id, result);
        }
        // A participant acks only after it read the verdict, so the verdict
        // is stored before the last ack applies. A waiter that registers
        // after the last ack finds the terminal entry and fires then.
        inner.settle(txn);
    }

    /// Set how long a terminal entry with no waiter stays for one to
    /// register, and how long an orphan stays with no live waiter. The host
    /// sets it longer than its statement deadline.
    pub fn set_waiterless_ttl(&self, ttl: Duration) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .waiterless_ttl = Some(ttl);
    }

    /// What this registry holds for participant `vshard` of `txn`.
    ///
    /// `None` means no entry exists for `txn`. That is either a txn this node
    /// never seeded, or a terminal one the waiterless sweep evicted.
    pub fn participant_progress(&self, txn: TxnId, vshard: u32) -> Option<ParticipantProgress> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .get(&txn)
            .map(|entry| ParticipantProgress {
                voted: entry.votes.contains_key(&vshard),
                acked: entry.acked_vshards.contains(&vshard),
                has_verdict: entry.verdict.is_some(),
            })
    }

    /// Test-only: returns the number of completion entries held.
    #[cfg(test)]
    pub fn pending_completions_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .len()
    }

    /// Test-only: the number of assignment senders held.
    #[cfg(test)]
    pub fn pending_assignments_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry that evicts a waiterless terminal entry at its next change.
    fn evicting_registry() -> Arc<CalvinCompletionRegistry> {
        let reg = CalvinCompletionRegistry::new_detached();
        reg.set_waiterless_ttl(Duration::ZERO);
        reg
    }

    /// A replica on which no coordinator waits keeps no terminal entry, and
    /// no ack result, past the eviction window. An entry that has a waiter,
    /// or that is sequenced and not terminal, stays.
    #[tokio::test]
    async fn waiterless_sweep_evicts_only_terminal_entries() {
        let reg = evicting_registry();

        let acked = TxnId::new(3, 0);
        reg.seed_expected(acked, 1);
        reg.note_verdict(acked, VerdictOutcome::Commit);
        reg.note_completion_ack_with(acked, 5, vec![0xAB; 4096]);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "a terminal entry nobody waits for is evicted with its result"
        );

        let awaited = TxnId::new(3, 2);
        let undecided = TxnId::new(3, 3);
        let rx = reg.register_completion_report(awaited, 2);
        reg.note_verdict(awaited, VerdictOutcome::Commit);
        reg.note_completion_ack_with(awaited, 5, b"five".to_vec());
        reg.seed_expected(undecided, 2);
        reg.note_completion_ack_with(undecided, 6, b"other".to_vec());
        assert!(
            reg.participant_progress(awaited, 5).is_some(),
            "an entry with a waiter is never evicted"
        );
        reg.note_completion_ack_with(awaited, 6, b"six".to_vec());
        let report = rx.await.expect("completion fires");
        assert_eq!(report.ack_results, vec![b"five".to_vec(), b"six".to_vec()]);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "only the undecided entry stays"
        );
        assert!(reg.participant_progress(undecided, 6).is_some());
    }

    /// The coordinator hosts no replica of either participant: it learns
    /// their apply results only from the `CompletionAck`s its sequencer
    /// replica applies. The report carries each participant's result, in
    /// vShard order, whether the acks land before or after it registers.
    #[tokio::test]
    async fn a_report_carries_every_participants_ack_result() {
        let reg = evicting_registry();
        let txn = TxnId::new(11, 4);
        reg.seed_expected(txn, 3);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_completion_ack_with(txn, 20, b"twenty".to_vec());
        let rx = reg.register_completion_report(txn, 3);
        reg.note_completion_ack_with(txn, 10, b"ten".to_vec());
        reg.note_completion_ack_with(txn, 30, Vec::new());
        let report = rx.await.expect("completion fires");
        assert_eq!(report.outcome, AttemptOutcome::Completed);
        assert_eq!(
            report.ack_results,
            vec![b"ten".to_vec(), b"twenty".to_vec()]
        );
        assert_eq!(reg.pending_completions_len(), 0);

        // A terminal entry waits the window for its waiter.
        let reg = CalvinCompletionRegistry::new_detached();
        let late = TxnId::new(11, 5);
        reg.seed_expected(late, 1);
        reg.note_verdict(late, VerdictOutcome::Commit);
        reg.note_completion_ack_with(late, 7, b"seven".to_vec());
        let report = reg
            .register_completion_report(late, 1)
            .await
            .expect("an already terminal txn fires on registration");
        assert_eq!(report.ack_results, vec![b"seven".to_vec()]);
    }

    #[tokio::test]
    async fn completion_fires_once_the_verdict_and_every_ack_are_in() {
        let reg = evicting_registry();
        let txn = TxnId::new(7, 0);
        reg.note_assigned(1, txn, 2);
        let mut rx = reg.register_completion(txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_completion_ack(txn, 10);
        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
        reg.note_completion_ack(txn, 20);
        assert_eq!(
            rx.await.expect("completion fires"),
            AttemptOutcome::Completed
        );
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn completion_fires_when_register_arrives_between_acks() {
        let reg = evicting_registry();
        let txn = TxnId::new(9, 3);
        reg.seed_expected(txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires once both acks arrived");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// Every ack without a stored verdict is not terminal: the waiter keeps
    /// waiting, and the sweep keeps the entry.
    #[tokio::test]
    async fn all_acks_without_a_verdict_are_not_terminal() {
        let reg = evicting_registry();
        let txn = TxnId::new(34, 3);
        reg.note_assigned(1, txn, 2);
        let mut rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
        assert_eq!(reg.pending_completions_len(), 1);

        reg.note_verdict(txn, VerdictOutcome::Commit);
        assert_eq!(
            rx.await.expect("completion fires"),
            AttemptOutcome::Completed
        );
    }

    /// A waiterless entry with every ack but no verdict survives the sweep.
    #[tokio::test]
    async fn a_waiterless_entry_without_a_verdict_is_never_evicted() {
        let reg = evicting_registry();
        let txn = TxnId::new(34, 4);
        reg.seed_expected(txn, 1);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(TxnId::new(34, 5), 10);
        assert!(reg.participant_progress(txn, 10).is_some());
    }

    #[tokio::test]
    async fn register_completion_seeds_participants_without_note_assigned() {
        // Cross-node coordinator: no note_assigned ever fires on its registry, so
        // register_completion must seed expected_participants from the assignment.
        // Without the seed (or with the is_complete>0 guard absent) this would
        // spuriously fire Completed with zero acks.
        let reg = evicting_registry();
        let txn = TxnId::new(21, 0);
        let rx = reg.register_completion(txn, 1);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "expected=1, 0 acks → must NOT complete prematurely"
        );
        reg.note_completion_ack(txn, 7);
        let outcome = rx.await.expect("completion fires after the single ack");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn ack_racing_ahead_of_register_does_not_prematurely_complete() {
        // The replicated ack can reach a remote coordinator's registry BEFORE the
        // coordinator calls register_completion. With expected_participants still
        // unknown (0), the ack must persist without firing or eviction within the
        // window; the later register_completion seeds the count and then completes.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(22, 0);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_completion_ack(txn, 7);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "ack before seeding must persist, not self-complete on expected=0"
        );
        let rx = reg.register_completion(txn, 1);
        let outcome = rx.await.expect("completion fires once participants seeded");
        assert_eq!(outcome, AttemptOutcome::Completed);
        reg.set_waiterless_ttl(Duration::ZERO);
        reg.note_completion_ack(TxnId::new(22, 1), 7);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn abort_verdict_makes_completion_report_aborted() {
        // An ABORT verdict is stored (Raft-ordered) before the acks that complete
        // the entry. Completion must report `Aborted`, never a silent
        // `Completed` that would drop the writes and report COMMIT success.
        let reg = evicting_registry();
        let txn = TxnId::new(31, 0);
        reg.seed_expected(txn, 2);
        reg.note_verdict(
            txn,
            VerdictOutcome::Abort(AbortReason::SerializationConflict),
        );
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Aborted {
                reason: AbortReason::SerializationConflict
            },
            "a stored ABORT verdict must surface as Aborted, not Completed"
        );
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// The verdict and acks race ahead of waiter registration. The terminal
    /// entry waits the window for its waiter, and registration reports
    /// `Aborted` at once. The sweep evicts the entry after its window.
    #[tokio::test]
    async fn abort_verdict_reports_aborted_when_register_arrives_after_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(32, 1);
        reg.seed_expected(txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Abort(AbortReason::PlanRejected));
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let mut rx = reg.register_completion(txn, 2);
        assert_eq!(
            rx.try_recv(),
            Ok(AttemptOutcome::Aborted {
                reason: AbortReason::PlanRejected
            }),
            "registration on a terminal entry fires at once"
        );
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "the entry waits its window"
        );

        reg.set_waiterless_ttl(Duration::ZERO);
        reg.note_completion_ack(TxnId::new(32, 2), 10);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// An ack or verdict that applies after the sweep evicted its terminal
    /// entry creates an orphan. The sweep evicts the orphan once its window
    /// passed, so late signals never pile up.
    #[tokio::test]
    async fn a_late_signal_for_an_evicted_txn_leaves_no_entry() {
        let reg = evicting_registry();
        let txn = TxnId::new(35, 0);
        reg.seed_expected(txn, 1);
        reg.note_vote(txn, 10, ParticipantVote::Commit);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        reg.note_completion_ack(txn, 10);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "the terminal entry is evicted"
        );

        reg.note_completion_ack(txn, 10);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "a late ack leaves no entry"
        );
        reg.note_vote(txn, 10, ParticipantVote::Commit);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "a late vote and verdict leave no entry"
        );
        assert_eq!(reg.participant_progress(txn, 10), None);
    }

    /// An orphan a coordinator waits on stays while the coordinator listens,
    /// past its window. Once the coordinator drops its receiver, the sweep
    /// evicts the orphan.
    #[tokio::test]
    async fn an_orphan_stays_while_its_coordinator_listens() {
        let reg = evicting_registry();
        let txn = TxnId::new(36, 0);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(TxnId::new(36, 1), 10);
        let progress = reg
            .participant_progress(txn, 10)
            .expect("a listening coordinator keeps its orphan");
        assert!(progress.acked && !progress.has_verdict);

        drop(rx);
        reg.note_completion_ack(TxnId::new(36, 2), 10);
        assert_eq!(reg.participant_progress(txn, 10), None);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// A sequenced entry is never an orphan. With no waiter and no verdict,
    /// it stays past the window: its participants still need its tally.
    #[tokio::test]
    async fn a_sequenced_entry_is_never_evicted_as_an_orphan() {
        let reg = evicting_registry();
        let txn = TxnId::new(37, 0);
        reg.note_assigned(1, txn, 2);
        reg.note_completion_ack(TxnId::new(37, 1), 10);
        assert_eq!(
            reg.participant_progress(txn, 10),
            None,
            "an assignment alone does not sequence the entry"
        );

        reg.seed_expected(txn, 2);
        reg.note_vote(txn, 10, ParticipantVote::Commit);
        reg.note_completion_ack(TxnId::new(37, 2), 10);
        assert!(
            reg.participant_progress(txn, 10)
                .is_some_and(|progress| progress.voted)
        );
    }

    #[tokio::test]
    async fn commit_verdict_reports_completed() {
        let reg = evicting_registry();
        let txn = TxnId::new(33, 2);
        reg.seed_expected(txn, 2);
        reg.note_verdict(txn, VerdictOutcome::Commit);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    /// A fired outcome keeps the entry's votes, verdict, and acks for the
    /// participants that still probe it, until the sweep evicts it.
    #[tokio::test]
    async fn a_fired_outcome_keeps_the_tally_until_the_sweep() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(40, 2);
        reg.seed_expected(txn, 1);
        let rx = reg.register_completion(txn, 1);
        reg.note_vote(txn, 1, ParticipantVote::Abort(AbortReason::PlanRejected));
        reg.note_verdict(txn, VerdictOutcome::Abort(AbortReason::PlanRejected));
        reg.note_completion_ack(txn, 1);
        assert_eq!(
            rx.await.expect("completion fires"),
            AttemptOutcome::Aborted {
                reason: AbortReason::PlanRejected
            }
        );

        let progress = reg.participant_progress(txn, 1).expect("entry stays");
        assert!(progress.voted && progress.acked && progress.has_verdict);
        assert_eq!(reg.verdict(txn), Some(false));
        assert_eq!(
            reg.vote_tally(txn).and_then(|tally| tally.get(&1).copied()),
            Some(ParticipantVote::Abort(AbortReason::PlanRejected))
        );

        reg.set_waiterless_ttl(Duration::ZERO);
        reg.note_completion_ack(TxnId::new(40, 3), 1);
        assert_eq!(reg.participant_progress(txn, 1), None);
    }

    /// A dropped assignment closes the caller's channel at once and leaves no
    /// sender behind. A later `note_assigned` for the seq finds none.
    #[test]
    fn drop_assignment_closes_the_callers_channel_and_frees_the_sender() {
        let reg = CalvinCompletionRegistry::new_detached();
        let mut rejected = reg.register_submission(4);
        let mut kept = reg.register_submission(5);
        assert_eq!(reg.pending_assignments_len(), 2);

        reg.drop_assignment(4);
        assert_eq!(
            rejected.try_recv(),
            Err(oneshot::error::TryRecvError::Closed),
            "the caller must see a closed channel, not wait out its timeout"
        );
        assert_eq!(reg.pending_assignments_len(), 1);

        reg.note_assigned(5, TxnId::new(2, 0), 3);
        assert_eq!(kept.try_recv(), Ok((2, 0, 3)));
        assert_eq!(reg.pending_assignments_len(), 0);

        // Dropping a seq with no sender is a no-op.
        reg.drop_assignment(4);
        assert_eq!(reg.pending_assignments_len(), 0);
    }

    #[tokio::test]
    async fn participant_progress_is_none_before_any_entry_exists() {
        let reg = CalvinCompletionRegistry::new_detached();
        assert_eq!(reg.participant_progress(TxnId::new(40, 0), 1), None);
    }

    #[tokio::test]
    async fn participant_progress_reports_each_applied_signal_for_its_vshard() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(40, 1);
        reg.seed_expected(txn, 2);
        let empty = reg.participant_progress(txn, 1).expect("seeded entry");
        assert!(!empty.voted && !empty.acked && !empty.has_verdict);

        reg.note_vote(txn, 1, ParticipantVote::Commit);
        reg.note_completion_ack(txn, 1);
        let own = reg.participant_progress(txn, 1).expect("entry");
        assert!(own.voted && own.acked);
        let peer = reg.participant_progress(txn, 2).expect("entry");
        assert!(
            !peer.voted && !peer.acked,
            "another vShard's signals do not count"
        );

        reg.note_verdict(txn, VerdictOutcome::Commit);
        let txn_wide = reg.participant_progress(txn, 2).expect("entry");
        assert!(txn_wide.has_verdict);
    }
}
