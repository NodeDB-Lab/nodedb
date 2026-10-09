// SPDX-License-Identifier: BUSL-1.1

//! Local commit-vote casting for a staged static Calvin transaction.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::driver::core::halt::{
    HaltReason, HaltStep, error_response_text,
};
use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::core::staged_vote::{
    StageVoteError, StagedVote, staged_commit_vote,
};
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Cast this leader's commit vote for a staged transaction, then PARK it
    /// on the cross-shard commit barrier awaiting the durable GLOBAL verdict.
    /// It never self-decides commit or abort on its local vote.
    ///
    /// The staged executor response is validate-only: its `stage_vote` is this
    /// shard's local commit vote, read by [`staged_commit_vote`]. A stage
    /// response the scheduler cannot read a vote from halts apply. It never
    /// counts as a commit. The vote travels through the sequencer Raft group,
    /// which aggregates every participant's vote into one authoritative
    /// `SequencerEntry::Verdict`, applied on every replica.
    ///
    /// This method moves the txn to [`CommitState::AwaitingVerdict`] WITHOUT
    /// dispatching a resolve or drop, then immediately probes
    /// `registry.verdict(txn)`: if the verdict is already durable (replay, or a
    /// push we raced) it resumes at once via [`Self::resume_on_verdict`];
    /// otherwise it stays parked, holding locks and its staged buffer, until the
    /// verdict push, a later probe, or the stall re-probe sweep delivers the
    /// verdict.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resolve_staged_commit(
        &mut self,
        txn_id: TxnId,
        staged_response: &Response,
    ) {
        // A staged error is always an abort vote. A response with no readable
        // vote halts: counting it as a commit will let a failed participant
        // commit after its peers received a global commit verdict.
        let superseded = self
            .pending
            .get(&txn_id)
            .is_some_and(|pending| pending.superseded);
        let vote = if superseded {
            StagedVote::CollectionSuperseded
        } else {
            match staged_commit_vote(staged_response) {
                // A read another node served is numbered in that node's WAL:
                // this node's versions cannot show it still current.
                Ok(StagedVote::Commit) if self.validates_read_served_elsewhere(txn_id) => {
                    StagedVote::SerializationConflict
                }
                Ok(vote) => vote,
                Err(error) => {
                    self.metrics.record_executor_error();
                    let reason = match error {
                        StageVoteError::CoreFailStopped { .. } => HaltReason::CoreFailStopped,
                        StageVoteError::Missing { .. }
                        | StageVoteError::CommitWithoutSuccess { .. } => {
                            HaltReason::StageVoteInvalid
                        }
                    };
                    self.halt_apply(
                        txn_id,
                        reason,
                        HaltStep::Stage,
                        format!("{error}: {}", error_response_text("stage", staged_response)),
                    );
                    return;
                }
            }
        };

        // Durably propose this leader's commit vote via the sequencer Raft
        // group. Only the leader stages, so its vote is the vShard's. The
        // sequencer aggregates every participant's vote into the single
        // global verdict this txn parks on below. An abort travels as
        // `AbortVote` so its cause survives to the coordinator. The vote stays
        // owed until the tally holds it, so a refused or dropped proposal is
        // proposed again rather than lost. A promoted leader's vote for a
        // txn whose vote applied already changes nothing: the first counts.
        self.propose_sequencer_entry(
            txn_id,
            SchedulerProposal::Vote {
                abort: vote.abort_reason(),
            },
        );

        if vote == StagedVote::SerializationConflict {
            // The staged slice's read-set was no longer current: observe it on
            // the node-global counter. A participant error never validated a
            // read-set, so it must not count here.
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
        // A stage error parks too. The leader votes abort, and every
        // participant drops at the abort verdict. A COMMIT verdict for it
        // can follow only a vote an earlier leader cast: `resume_on_verdict`
        // halts on it.
        match self.pending.get_mut(&txn_id) {
            Some(pending) => {
                pending.commit_state = CommitState::AwaitingVerdict;
                // A superseded slice cannot commit here: its collection is gone.
                pending.stage_error = match vote {
                    StagedVote::ParticipantError | StagedVote::PredictionDrift => {
                        Some(error_response_text("stage", staged_response))
                    }
                    StagedVote::CollectionSuperseded => Some(
                        "a collection the transaction names no longer holds its planned \
                         incarnation"
                            .to_owned(),
                    ),
                    StagedVote::Commit | StagedVote::SerializationConflict => None,
                };
                // no-determinism: local stall-warning deadline only; the global replicated verdict, not this wall-clock, decides commit/abort.
                pending.verdict_deadline = Some(Instant::now() + self.config.verdict_stall_warn());
            }
            None => return,
        }

        // PROBE on park (correctness backstop): the verdict can already be
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

    /// Whether a read this vShard validates for `txn_id` was served by a node
    /// other than this one. Its `read_lsn` is a position in that node's WAL.
    fn validates_read_served_elsewhere(&self, txn_id: TxnId) -> bool {
        let Some(pending) = self.pending.get(&txn_id) else {
            return false;
        };
        let tx_class = &pending.txn.tx_class;
        tx_class.versioned_reads.iter().any(|entry| {
            super::super::routing::versioned_read_homes_on(
                entry,
                tx_class.database_id,
                self.vshard_id,
            ) && entry.served_by != self.shared.node_id
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::{AbortReason, SequencerEntry};

    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, error_response, make_sequenced_txn, staged_pending, staged_response,
    };

    const VSHARD: u32 = 7;

    /// A data-group leader holding `txn_id` staged, with a proposer that
    /// accepts every proposal.
    fn staged_leader(
        txn_id: TxnId,
    ) -> (
        Scheduler,
        tempfile::TempDir,
        std::sync::Arc<CapturingProposer>,
    ) {
        let (mut scheduler, dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
        );
        (scheduler, dir, proposer)
    }

    #[tokio::test]
    async fn each_stage_vote_proposes_its_own_vote() {
        let cases = [
            (Status::Ok, StageVote::Commit, None),
            (
                Status::Ok,
                StageVote::SerializationConflict,
                Some(AbortReason::SerializationConflict),
            ),
            (
                Status::Error,
                StageVote::PredictionDrift,
                Some(AbortReason::PredictionDrift),
            ),
            (
                Status::Error,
                StageVote::ParticipantError,
                Some(AbortReason::ParticipantError),
            ),
        ];
        for (status, vote, abort) in cases {
            let txn_id = TxnId::new(30, 1);
            let (mut scheduler, _dir, proposer) = staged_leader(txn_id);

            scheduler.resolve_staged_commit(txn_id, &staged_response(status, Some(vote)));

            let expected = match abort {
                None => SequencerEntry::Vote {
                    epoch: 30,
                    position: 1,
                    vshard: VSHARD,
                },
                Some(reason) => SequencerEntry::AbortVote {
                    epoch: 30,
                    position: 1,
                    vshard: VSHARD,
                    reason,
                },
            };
            assert_eq!(proposer.accepted(), vec![expected], "{vote:?}");
            assert_eq!(
                scheduler.pending.get(&txn_id).map(|p| p.commit_state),
                Some(CommitState::AwaitingVerdict),
                "{vote:?}"
            );
            assert!(scheduler.apply_halt().is_none(), "{vote:?}");
        }
    }

    #[tokio::test]
    async fn a_successful_stage_response_without_a_vote_halts_and_never_votes() {
        let txn_id = TxnId::new(31, 0);
        let (mut scheduler, _dir, proposer) = staged_leader(txn_id);

        scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, None));

        assert_eq!(proposer.attempt_count(), 0, "no vote is proposed");
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::StageVoteInvalid)
        );
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged),
            "the txn does not park on the verdict barrier"
        );
        assert!(!scheduler.applied.is_applied(31, 0));
    }

    #[tokio::test]
    async fn a_commit_vote_on_an_error_response_halts_and_never_votes() {
        let txn_id = TxnId::new(32, 0);
        let (mut scheduler, _dir, proposer) = staged_leader(txn_id);

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Error, Some(StageVote::Commit)),
        );

        assert_eq!(proposer.attempt_count(), 0, "no vote is proposed");
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::StageVoteInvalid)
        );
    }

    /// A fail-stopped core refuses the stage and every later request. The
    /// leader halts on it and votes nothing.
    #[tokio::test]
    async fn a_fail_stopped_core_halts_the_stage_and_never_votes() {
        let txn_id = TxnId::new(33, 0);
        let (mut scheduler, _dir, proposer) = staged_leader(txn_id);

        scheduler.resolve_staged_commit(
            txn_id,
            &error_response(ErrorCode::CoreFailStopped {
                core_id: 0,
                detail: "rollback failed".to_string(),
            }),
        );

        assert_eq!(proposer.attempt_count(), 0, "no vote is proposed");
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::CoreFailStopped)
        );
        assert!(!scheduler.applied.is_applied(33, 0));
    }

    /// The leader's vote is its real one, never a forced abort. A node that
    /// lost its Raft leadership after it staged keeps the vote owed and
    /// proposes nothing.
    #[tokio::test]
    async fn a_vote_is_proposed_only_while_this_node_leads() {
        let txn_id = TxnId::new(34, 0);
        let (mut scheduler, _dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
        );

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );

        assert_eq!(proposer.attempt_count(), 0, "a non-leader proposes nothing");
        assert!(scheduler.owed.contains_key(&(txn_id, OwedKind::Vote)));
        lead_data_group(&mut scheduler);
        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::Vote {
                epoch: 34,
                position: 0,
                vshard: VSHARD,
            }],
            "the vote is the stage's commit, not a forced abort"
        );
    }
}
