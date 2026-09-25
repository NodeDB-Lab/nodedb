// SPDX-License-Identifier: BUSL-1.1

//! Local commit-vote casting for a staged static Calvin transaction.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::driver::core::halt::error_response_text;
use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::core::staged_vote::{
    StagedVote, staged_commit_vote,
};
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Cast this participant's local commit vote for a staged transaction, then
    /// PARK it on the cross-shard commit barrier awaiting the durable GLOBAL
    /// verdict — it does NOT self-decide flush-or-drop on its local vote.
    ///
    /// The staged executor response is validate-only: its `read_set_valid` is
    /// this shard's local commit vote (`Some(true)` => commit, `Some(false)` =>
    /// abort; a `None` from the active/dependent path is treated as commit). The
    /// leader proposes that vote via the sequencer Raft group; the sequencer
    /// aggregates all participants' votes into a single authoritative
    /// `SequencerEntry::Verdict`, applied on every replica.
    ///
    /// This method moves the txn to [`CommitState::AwaitingVerdict`] WITHOUT
    /// dispatching a resolve or drop, then immediately probes
    /// `registry.verdict(txn)`: if the verdict is already durable (replay, or a
    /// push we raced) it resumes at once via [`Self::resume_on_verdict`];
    /// otherwise it stays parked, holding locks and its staged buffer, until the
    /// verdict push, a later probe, or the stall re-probe sweep delivers the
    /// verdict. Resuming (in `resume_on_verdict`) is where the flush/drop is
    /// dispatched and the flushed/dropped counters bump — using the GLOBAL
    /// verdict, never the local vote.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resolve_staged_commit(
        &mut self,
        txn_id: TxnId,
        staged_response: &Response,
    ) {
        // A staged error is always an abort vote. Only successful staged
        // responses may use `None` for the dependent-read path; accepting an
        // error-plus-None as commit would let a failed participant flush after
        // its peers received a global commit verdict.
        let vote = staged_commit_vote(staged_response);

        // Durably propose this participant's commit vote via the sequencer
        // Raft group, leader-guarded like `OllpMismatch`: only the data-group
        // leader ran read-set validation, so only a leader's vote is
        // authoritative. The sequencer aggregates every participant's vote into
        // the single global verdict this txn parks on below. An abort travels as
        // `AbortVote` so its cause survives to the coordinator. The vote stays
        // owed until the tally holds it, so a refused or dropped proposal is
        // proposed again rather than lost.
        if self.is_group_leader() {
            self.propose_sequencer_entry(
                txn_id,
                SchedulerProposal::Vote {
                    abort: vote.abort_reason(),
                },
            );
        }

        if vote == StagedVote::SerializationConflict {
            // The staged slice's read-set was no longer current: observe it, the
            // same node-global signal the direct-apply path records. A
            // participant error never validated a read-set, so it must not count
            // here.
            self.shared
                .calvin
                .counters
                .read_set_validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        // PARK on the barrier: transition to `AwaitingVerdict` and arm the stall
        // deadline. Do NOT dispatch resolve/drop here — the GLOBAL verdict, not
        // this local vote, decides. If the txn already vanished (torn down
        // elsewhere), there is nothing to park.
        //
        // A stage error parks too. A deterministic error fails on every
        // replica, the leader votes abort, and every replica drops. A local
        // error on a follower leaves the leader's commit vote standing:
        // `resume_on_verdict` halts on that COMMIT verdict.
        match self.pending.get_mut(&txn_id) {
            Some(pending) => {
                pending.commit_state = Some(CommitState::AwaitingVerdict);
                pending.stage_error = (vote == StagedVote::ParticipantError)
                    .then(|| error_response_text("stage", staged_response));
                // no-determinism: local stall-warning deadline only; the global replicated verdict, not this wall-clock, decides commit/abort.
                pending.verdict_deadline = Some(Instant::now() + self.config.verdict_stall_warn());
            }
            None => return,
        }

        // PROBE on park (correctness backstop): the verdict may already be
        // durable — on replay, or a push that raced ahead of this park. Resume
        // immediately if so; the double-resume guard in `resume_on_verdict`
        // makes a later duplicate push/probe a no-op.
        if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        )) {
            self.resume_on_verdict(txn_id, verdict);
        }
    }
}
