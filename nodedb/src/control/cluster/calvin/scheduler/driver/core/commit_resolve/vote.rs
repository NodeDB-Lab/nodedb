// SPDX-License-Identifier: BUSL-1.1

//! Local commit-vote casting for a staged static Calvin transaction.

use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::driver::core::halt::{
    HaltReason, HaltStep, error_response_text,
};
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::core::staged_vote::{
    StageVoteError, StagedVote, staged_commit_vote,
};
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
    /// A commit of a slice that holds a `RETURNING` plan of the contract
    /// waits for the plan's evaluation, which casts the vote (see
    /// [`Self::hold_vote_for_returning`]). An evaluation error votes abort.
    ///
    /// [`Self::cast_staged_vote`] moves the txn to `CommitState::AwaitingVerdict` WITHOUT
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
                Ok(vote) => vote,
                Err(error) => {
                    self.metrics.record_executor_error();
                    let reason = match error {
                        StageVoteError::CoreFailStopped { .. } => HaltReason::CoreFailStopped,
                        StageVoteError::Missing { .. }
                        | StageVoteError::CommitWithoutSuccess { .. }
                        | StageVoteError::RefusalUnencodable { .. } => HaltReason::StageVoteInvalid,
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
        // A commit waits for the slice's `RETURNING` evaluation, which
        // casts the vote once it finishes: commit, or abort with the
        // evaluation's refusal.
        if vote == StagedVote::Commit && self.hold_vote_for_returning(txn_id) {
            return;
        }
        // A superseded slice cannot commit here: its collection is gone.
        let stage_error = match vote {
            StagedVote::ParticipantError { .. } | StagedVote::PredictionDrift => {
                Some(error_response_text("stage", staged_response))
            }
            StagedVote::CollectionSuperseded => Some(
                "a collection the transaction names no longer holds its planned incarnation"
                    .to_owned(),
            ),
            StagedVote::Commit | StagedVote::SerializationConflict => None,
        };
        self.cast_staged_vote(txn_id, vote, stage_error);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::{AbortReason, SequencerEntry};

    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
    use crate::control::cluster::calvin::scheduler::driver::core::returning_eval::{
        contract, division_by_zero, image_rows, with_contract,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, error_response, make_sequenced_txn, staged_pending, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::staged_returning::StagedReturning;
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    const VSHARD: u32 = 7;

    /// [`staged_leader`] whose txn carries `contract`. Its one local plan
    /// sits at class index 0, with `images` image rows when `Some`.
    fn returning_leader(
        txn_id: TxnId,
        contract: nodedb_cluster::calvin::types::ReturningContract,
        images: Option<usize>,
    ) -> (
        Scheduler,
        tempfile::TempDir,
        std::sync::Arc<CapturingProposer>,
    ) {
        let (mut scheduler, dir, proposer) = staged_leader(txn_id);
        let txn = with_contract(make_sequenced_txn(txn_id.epoch, txn_id.position), contract);
        let mut pending = staged_pending(txn, txn_id);
        pending.returning = StagedReturning::of_slice(vec![0]);
        if let Some(rows) = images {
            pending.returning.images.insert(0, image_rows(rows));
        }
        scheduler.pending.insert(txn_id, pending);
        (scheduler, dir, proposer)
    }

    /// The refusal of the one abort vote `proposer` accepted.
    fn refusal_of_the_vote(proposer: &CapturingProposer) -> ErrorCode {
        match proposer.accepted().as_slice() {
            [
                SequencerEntry::AbortVote {
                    reason: AbortReason::ParticipantError,
                    refusal: Some(refusal),
                    ..
                },
            ] => {
                crate::control::cluster::calvin::refusal::decode_refusal(refusal)
                    .expect("the refusal decodes")
                    .code
            }
            other => panic!("expected one refused abort vote, got {other:?}"),
        }
    }

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
                    refusal: None,
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

    /// A stage refused with a typed code: the abort vote carries the code.
    #[tokio::test]
    async fn a_refused_stage_proposes_its_refusal_with_the_vote() {
        let txn_id = TxnId::new(30, 2);
        let (mut scheduler, _dir, proposer) = staged_leader(txn_id);
        let code = ErrorCode::RejectedConstraint {
            constraint: "not_null".into(),
            detail: "column 'v' is NOT NULL".into(),
        };
        let mut response = staged_response(Status::Error, Some(StageVote::ParticipantError));
        response.error_code = Some(Box::new(code.clone()));

        scheduler.resolve_staged_commit(txn_id, &response);

        let refusal = crate::control::cluster::calvin::refusal::encode_refusal(&code)
            .expect("the refusal encodes");
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::AbortVote {
                epoch: 30,
                position: 2,
                vshard: VSHARD,
                reason: AbortReason::ParticipantError,
                refusal: Some(refusal),
            }]
        );
        assert!(scheduler.apply_halt().is_none());
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

    /// A commit stage whose `RETURNING` list fails votes abort with the
    /// error's code, once the evaluation finishes. Nothing halts.
    #[tokio::test]
    async fn a_returning_evaluation_error_votes_abort_with_its_refusal() {
        let txn_id = TxnId::new(35, 0);
        let (mut scheduler, _dir, proposer) =
            returning_leader(txn_id, contract(vec![division_by_zero()], 10), Some(1));

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert_eq!(proposer.attempt_count(), 0, "the vote waits");
        scheduler.settle_returning_eval().await;

        assert_eq!(refusal_of_the_vote(&proposer), ErrorCode::DivisionByZero);
        let pending = scheduler.pending.get(&txn_id).expect("parked");
        assert_eq!(pending.commit_state, CommitState::AwaitingVerdict);
        assert!(pending.stage_error.is_some());
        assert!(pending.returning.frozen.is_empty());
        assert!(scheduler.apply_halt().is_none());
    }

    /// Rows past the contract's limit vote abort before any expression runs.
    #[tokio::test]
    async fn a_returning_limit_exceeded_votes_abort() {
        let txn_id = TxnId::new(36, 0);
        let (mut scheduler, _dir, proposer) =
            returning_leader(txn_id, contract(vec![division_by_zero()], 1), Some(2));

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        scheduler.settle_returning_eval().await;

        match refusal_of_the_vote(&proposer) {
            ErrorCode::ExecutionLimitExceeded { detail } => {
                assert_eq!(detail, "RETURNING rows 2 exceeds limit 1")
            }
            other => panic!("expected the limit refusal, got {other:?}"),
        }
        assert!(scheduler.apply_halt().is_none());
    }

    /// Images the leader cannot read, or no images for a plan the contract
    /// names, vote abort at once with an internal refusal. Nothing halts.
    #[tokio::test]
    async fn unreadable_or_missing_images_vote_an_internal_abort() {
        let unreadable = TxnId::new(37, 0);
        let (mut scheduler, _dir, proposer) =
            returning_leader(unreadable, contract(vec![division_by_zero()], 10), Some(1));
        if let Some(pending) = scheduler.pending.get_mut(&unreadable) {
            pending.returning.failed = Some("the images do not decode".into());
        }
        scheduler.resolve_staged_commit(
            unreadable,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert!(matches!(
            refusal_of_the_vote(&proposer),
            ErrorCode::Internal { .. }
        ));
        assert!(scheduler.apply_halt().is_none());

        let missing = TxnId::new(38, 0);
        let (mut scheduler, _dir, proposer) =
            returning_leader(missing, contract(vec![division_by_zero()], 10), None);
        scheduler.resolve_staged_commit(
            missing,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert!(matches!(
            refusal_of_the_vote(&proposer),
            ErrorCode::Internal { .. }
        ));
        assert!(scheduler.apply_halt().is_none());
    }
}
