// SPDX-License-Identifier: BUSL-1.1

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

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
/// Exactly one of these fires per attempt on the unified completion channel:
/// all expected vshards acked AND the global verdict was commit (`Completed`),
/// all expected vshards acked but the global cross-shard verdict was ABORT
/// (`Aborted`), the executor reported an OLLP prediction mismatch that forces a
/// retry (`Mismatch`), or the scheduler rejected the transaction's routing as
/// terminally broken (`Failed`). `Aborted` and `Failed` are NEVER retried,
/// unlike `Mismatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Completed,
    /// The global cross-shard verdict was ABORT (read-set/OCC validation failed).
    /// A serialization failure — surfaced to the client as SQLSTATE 40001, never retried.
    Aborted,
    Mismatch,
    Failed {
        detail: String,
    },
}

pub(crate) struct PendingCompletion {
    /// `pub(crate)`: also read/written by the vote/verdict-tally methods in
    /// `completion_verdict.rs` (a sibling module in the same crate).
    pub(crate) expected_participants: usize,
    acked_vshards: BTreeSet<u32>,
    completion_tx: Option<oneshot::Sender<AttemptOutcome>>,
    /// Set when an OLLP mismatch is observed before the coordinator registers
    /// its waiter, so the outcome is not lost across registration order (mirrors
    /// how `acked_vshards` persists ack state regardless of registration order).
    mismatched: bool,
    /// Set when a terminal routing failure is observed before the coordinator
    /// registers its waiter, mirroring `mismatched`. Takes precedence over both
    /// `mismatched` and completion: a routing failure is never retried and never
    /// falsely reported as success.
    routing_failed: Option<String>,
    /// Durable per-participant commit votes tallied from `SequencerEntry::Vote`,
    /// keyed by vshard so a re-proposed vote (retry) overwrites deterministically.
    /// Once complete, the leader aggregates the tally into the global `verdict`
    /// that gates each participant's flush/drop at the cross-shard commit barrier.
    pub(crate) votes: BTreeMap<u32, bool>,
    /// The authoritative commit/abort verdict, applied from a replicated
    /// `SequencerEntry::Verdict`; `None` until applied. This is the durable
    /// barrier gate: a participant parked in `AwaitingVerdict` resumes into its
    /// flush (commit) or drop (abort) once it is set. It is also the ONLY
    /// authority for "already decided" — a post-failover leader re-proposes from
    /// `votes` until it is stored (see `drain_unproposed_verdicts`).
    pub(crate) verdict: Option<bool>,
    /// Dedup guard for the LOCAL emit path: set the first time the tally becomes
    /// complete so the verdict signal is emitted exactly once across vote
    /// re-proposals. Per-node, non-durable, never reset on failover — so it is
    /// NOT consulted by `drain_unproposed_verdicts` (which trusts only the
    /// durable `verdict`, letting a promoted leader re-propose a still-unstored one).
    pub(crate) verdict_proposed: bool,
}

/// Choose the terminal outcome for a COMPLETED entry (all expected vshards
/// acked) by consulting the durable global verdict.
///
/// The verdict is applied from a replicated `SequencerEntry::Verdict` that is
/// strictly ordered BEFORE the abort's `CompletionAck` in the sequencer Raft
/// log, so an ABORT verdict is always stored (`verdict == Some(false)`) by the
/// time this entry's completion fires. `Some(false)` is a serialization abort
/// (`Aborted`); `None` (single-shard / no-verdict paths) and `Some(true)` are
/// `Completed`. We deliberately do NOT gate on `verdict.is_some()` — that would
/// stall the no-verdict completion paths.
fn outcome_for(entry: &PendingCompletion) -> AttemptOutcome {
    match entry.verdict {
        Some(false) => AttemptOutcome::Aborted,
        _ => AttemptOutcome::Completed,
    }
}

impl PendingCompletion {
    pub(crate) fn new(expected_participants: usize) -> Self {
        Self {
            expected_participants,
            acked_vshards: BTreeSet::new(),
            completion_tx: None,
            mismatched: false,
            routing_failed: None,
            votes: BTreeMap::new(),
            verdict: None,
            verdict_proposed: false,
        }
    }

    fn is_complete(&self) -> bool {
        // Require a KNOWN participant count (>0). The `expected_participants == 0`
        // default means "not yet seeded" — completion must not fire until the
        // count is known (via `note_assigned` on the leader, or `register_completion`
        // from the routed assignment on a remote coordinator). Without this guard a
        // replicated ack that races ahead of seeding, or a bare `register_completion`,
        // would spuriously report `Completed` with zero acks.
        self.expected_participants > 0 && self.acked_vshards.len() >= self.expected_participants
    }
}

/// `pub(crate)`: also read by the vote/verdict-tally methods in
/// `completion_verdict.rs` (a sibling module in the same crate).
#[derive(Default)]
pub(crate) struct Inner {
    assignments: BTreeMap<u64, oneshot::Sender<(u64, u32, usize)>>,
    pub(crate) completions: BTreeMap<TxnId, PendingCompletion>,
    /// Per-vShard senders for the verdict push, keyed by vShard id. Each local
    /// Calvin scheduler registers its receiver's sender here at construction;
    /// `note_verdict` broadcasts a [`super::completion_verdict::VerdictSignal`]
    /// to all of them under this same mutex, so a stored verdict and its push
    /// notification never disagree.
    pub(crate) verdict_signal_senders:
        BTreeMap<u32, mpsc::Sender<super::completion_verdict::VerdictSignal>>,
}

pub struct CalvinCompletionRegistry {
    /// `pub(crate)`: also locked by the vote/verdict-tally methods in
    /// `completion_verdict.rs` (a sibling module in the same crate); never
    /// exposed beyond the crate.
    pub(crate) inner: Mutex<Inner>,
    /// Emits `(txn, commit)` exactly once when a staged cross-shard txn's vote
    /// tally becomes complete (all expected participants voted). The paired
    /// receiver lives in the `SequencerService`, whose leader-guarded arm turns
    /// the signal into a `SequencerEntry::Verdict` proposal. `pub(crate)`: also
    /// used by `note_vote` in `completion_verdict.rs`.
    pub(crate) verdict_tx: mpsc::Sender<(TxnId, bool)>,
}

impl CalvinCompletionRegistry {
    /// Construct a registry wired to a verdict signal channel. The paired
    /// receiver must be handed to the `SequencerService` on this node so the
    /// leader can propose the aggregated verdict.
    pub fn new(verdict_tx: mpsc::Sender<(TxnId, bool)>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            verdict_tx,
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

    pub fn register_submission(&self, inbox_seq: u64) -> oneshot::Receiver<(u64, u32, usize)> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .insert(inbox_seq, tx);
        rx
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
        inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
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
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
        entry.expected_participants = entry.expected_participants.max(expected_participants);
        // Routing failure takes precedence over everything else: it is terminal
        // and must never be masked by a later ack or mismatch signal.
        if let Some(detail) = entry.routing_failed.take() {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Failed { detail }).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before routing-failure signal; \
                     client likely timed out on completion wait"
                );
            }
        } else if entry.mismatched {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Mismatch).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before OLLP-mismatch signal; \
                     client likely timed out on completion wait"
                );
            }
        } else if entry.is_complete() {
            // Acks raced ahead of waiter registration: consult the stored
            // verdict so an already-complete ABORT surfaces as `Aborted`, not a
            // false `Completed`.
            let outcome = outcome_for(entry);
            inner.completions.remove(&txn);
            if tx.send(outcome).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before all-acked signal; \
                     client likely timed out on completion wait"
                );
            }
        } else {
            entry.completion_tx = Some(tx);
        }
        rx
    }

    pub fn note_completion_ack(&self, txn: TxnId, vshard_id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.acked_vshards.insert(vshard_id);
        if entry.is_complete() {
            // Consult the stored global verdict: `Verdict` is applied strictly
            // before this abort's `CompletionAck` in the sequencer Raft log, so
            // an ABORT is `Some(false)` here and must surface as `Aborted`
            // rather than a silent `Completed` (which would drop the writes and
            // report COMMIT SUCCESS to the client).
            //
            // Only fire + evict when the coordinator's waiter is registered. If
            // the final ack races AHEAD of registration, LEAVE the entry — its
            // `acked_vshards` (and the stored verdict) persist, so
            // `register_completion`'s `is_complete()` branch fires the outcome.
            // Evicting here would strand that branch (a fresh entry created by
            // the later `register_completion` never re-completes). Mirrors how
            // `mismatched`/`routing_failed` persist across the same race.
            if let Some(tx) = entry.completion_tx.take() {
                let outcome = outcome_for(entry);
                inner.completions.remove(&txn);
                if tx.send(outcome).is_err() {
                    tracing::warn!(
                        epoch = txn.epoch,
                        position = txn.position,
                        vshard_id,
                        "calvin completion receiver dropped before final ack; \
                         client likely timed out on completion wait"
                    );
                }
            }
        }
    }

    /// Record an OLLP prediction mismatch for `txn`, the second terminal outcome
    /// of an attempt. Mismatch takes precedence over completion: a mismatched
    /// attempt must retry, never falsely report success.
    ///
    /// If the coordinator's waiter is already registered, fire `Mismatch` and
    /// evict the entry. Otherwise leave the `mismatched` flag set so a later
    /// `register_completion` fires it (mirrors `acked_vshards` persistence).
    pub fn note_ollp_mismatch(&self, txn: TxnId) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.mismatched = true;
        if let Some(tx) = entry.completion_tx.take() {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Mismatch).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before OLLP-mismatch signal; \
                     client likely timed out on completion wait"
                );
            }
        }
    }

    /// Record a terminal, NON-retryable routing failure for `txn` — the
    /// scheduler rejected the transaction's local plan routing as
    /// `Unroutable`, `ControlPlaneOnly`, or `NotAWrite`. Takes precedence over
    /// completion AND over an OLLP mismatch: a routing failure can never
    /// converge via retry, so it must never be masked by a later ack or
    /// mismatch signal.
    ///
    /// If the coordinator's waiter is already registered, fire `Failed` and
    /// evict the entry. Otherwise leave the `routing_failed` detail set so a
    /// later `register_completion` fires it (mirrors `mismatched` persistence).
    pub fn note_routing_failed(&self, txn: TxnId, detail: String) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.routing_failed = Some(detail.clone());
        if let Some(tx) = entry.completion_tx.take() {
            inner.completions.remove(&txn);
            if tx.send(AttemptOutcome::Failed { detail }).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before routing-failure signal; \
                     client likely timed out on completion wait"
                );
            }
        }
    }

    /// Test-only: returns the number of pending completion entries.
    /// Used to verify entries are removed once all acks arrive (no leak).
    #[cfg(test)]
    pub fn pending_completions_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completion_entry_removed_after_all_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(7, 0);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once all expected vshards have acked"
        );
    }

    #[tokio::test]
    async fn completion_entry_removed_when_register_arrives_after_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(9, 3);
        reg.note_assigned(1, txn, 2);
        reg.note_completion_ack(txn, 10);
        // Entry remains: expected=2, only 1 ack received.
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires once both acks arrived");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once awaiter is signalled"
        );
    }

    #[tokio::test]
    async fn mismatch_arriving_before_register_fires_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(11, 1);
        reg.note_assigned(1, txn, 2);
        // Mismatch observed before the coordinator registers its waiter: the
        // flag must persist so a later register_completion fires it.
        reg.note_ollp_mismatch(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }

    #[tokio::test]
    async fn register_before_mismatch_fires_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(12, 5);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        // Waiter already stored; the mismatch must wake it directly.
        reg.note_ollp_mismatch(txn);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }

    #[tokio::test]
    async fn register_completion_seeds_participants_without_note_assigned() {
        // Cross-node coordinator: no note_assigned ever fires on its registry, so
        // register_completion must seed expected_participants from the assignment.
        // Without the seed (or with the is_complete>0 guard absent) this would
        // spuriously fire Completed with zero acks.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(21, 0);
        let rx = reg.register_completion(txn, 1);
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
        // unknown (0), the ack must persist without firing/evicting; the later
        // register_completion seeds the count and then completes.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(22, 0);
        reg.note_completion_ack(txn, 7);
        assert_eq!(
            reg.pending_completions_len(),
            1,
            "ack before seeding must persist, not self-complete on expected=0"
        );
        let rx = reg.register_completion(txn, 1);
        let outcome = rx.await.expect("completion fires once participants seeded");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn routing_failed_arriving_before_register_fires_failed() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(14, 1);
        reg.note_assigned(1, txn, 2);
        // Routing failure observed before the coordinator registers its
        // waiter: the detail must persist so a later register_completion
        // fires it.
        reg.note_routing_failed(txn, "unroutable plan".to_owned());
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "unroutable plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn register_before_routing_failed_fires_failed() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(15, 4);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        assert_eq!(reg.pending_completions_len(), 1);
        // Waiter already stored; the routing failure must wake it directly.
        reg.note_routing_failed(txn, "control-plane-only plan".to_owned());
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "control-plane-only plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn routing_failed_takes_precedence_over_pending_acks_and_mismatch() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(16, 0);
        reg.note_assigned(1, txn, 2);
        // No waiter registered yet: an ack and a mismatch both persist onto
        // the entry without firing anything.
        reg.note_completion_ack(txn, 10);
        reg.note_ollp_mismatch(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        // The routing failure also persists (still no waiter)...
        reg.note_routing_failed(txn, "non-write plan".to_owned());
        // ...and when the coordinator finally registers, it must observe the
        // routing failure, not the mismatch or the ack — routing failure is
        // terminal and must never be masked by either.
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Failed {
                detail: "non-write plan".to_owned()
            }
        );
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once routing failure is signalled"
        );
    }

    #[tokio::test]
    async fn abort_verdict_makes_completion_report_aborted() {
        // An ABORT verdict is stored (Raft-ordered) before the acks that complete
        // the tally. Completion must consult it and report `Aborted`, never a
        // silent `Completed` that would drop the writes and report COMMIT success.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(31, 0);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(txn, false);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(
            outcome,
            AttemptOutcome::Aborted,
            "a stored ABORT verdict must surface as Aborted, not Completed"
        );
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn abort_verdict_reports_aborted_when_register_arrives_after_acks() {
        // Acks (and the verdict) race ahead of waiter registration: the
        // already-complete branch in `register_completion` must also consult the
        // stored verdict and report `Aborted`.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(32, 1);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(txn, false);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let rx = reg.register_completion(txn, 2);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Aborted);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn commit_verdict_reports_completed() {
        // A COMMIT verdict (Some(true)) must still report `Completed`.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(33, 2);
        reg.note_assigned(1, txn, 2);
        reg.note_verdict(txn, true);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn no_verdict_reports_completed_unchanged() {
        // Single-shard / no-verdict paths (`verdict == None`) must report
        // `Completed` unchanged — the fix must not stall them.
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(34, 3);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        reg.note_completion_ack(txn, 10);
        reg.note_completion_ack(txn, 20);
        let outcome = rx.await.expect("completion fires");
        assert_eq!(outcome, AttemptOutcome::Completed);
        assert_eq!(reg.pending_completions_len(), 0);
    }

    #[tokio::test]
    async fn mismatch_takes_precedence_over_pending_acks() {
        let reg = CalvinCompletionRegistry::new_detached();
        let txn = TxnId::new(13, 2);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn, 2);
        // One ack arrives but the attempt is not yet complete (expected=2).
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        // A mismatch on the same attempt must win and force a retry.
        reg.note_ollp_mismatch(txn);
        let outcome = rx.await.expect("mismatch fires");
        assert_eq!(outcome, AttemptOutcome::Mismatch);
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once mismatch is signalled"
        );
    }
}
