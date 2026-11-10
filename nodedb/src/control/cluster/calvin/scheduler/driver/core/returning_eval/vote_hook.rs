// SPDX-License-Identifier: BUSL-1.1

//! Hold a staged txn's commit vote for its `RETURNING` evaluation.
//!
//! A slice that would vote commit and holds a plan its contract names runs
//! the evaluation first. The evaluation can wait on a sequence range
//! reservation, so it runs on a blocking thread, and the scheduler loop
//! goes on meanwhile. The txn stays `Staged` with its locks until the
//! result returns, so a later txn that needs one of its keys still waits
//! behind it. Only then does the leader vote: commit with the frozen
//! replies held on the txn, or abort with the refusal.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::super::scheduler::Scheduler;
use super::super::staged_vote::StagedVote;
use super::evals::Evaluated;
use super::evaluate::{ReturningJob, ReturningRefusal, evaluate};
use crate::control::cluster::calvin::refusal::encode_refusal;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// What a commit vote waits for.
enum ReturningGate {
    /// Nothing: the slice holds no plan its contract names.
    Open,
    /// The evaluation of `job`.
    Evaluate(ReturningJob),
    /// Nothing more: the slice cannot commit.
    Refuse(ReturningRefusal),
}

impl Scheduler {
    /// Hold `txn_id`'s commit vote for its `RETURNING` evaluation.
    ///
    /// Returns `false` when the vote waits for nothing. Returns `true` once
    /// this method owns the vote: the evaluation runs, or the slice already
    /// voted abort because its images cannot be read.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn hold_vote_for_returning(
        &mut self,
        txn_id: TxnId,
    ) -> bool {
        match self.returning_gate(txn_id) {
            ReturningGate::Open => false,
            ReturningGate::Refuse(refusal) => {
                self.vote_refused(txn_id, refusal);
                true
            }
            ReturningGate::Evaluate(job) => {
                self.spawn_returning_eval(txn_id, job);
                true
            }
        }
    }

    /// What `txn_id`'s commit vote waits for.
    fn returning_gate(&mut self, txn_id: TxnId) -> ReturningGate {
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return ReturningGate::Open;
        };
        if let Some(failed) = &pending.returning.failed {
            return ReturningGate::Refuse(ReturningRefusal::internal(failed.clone()));
        }
        let Some(contract) = pending.txn.tx_class.returning.as_ref() else {
            return ReturningGate::Open;
        };
        let mut plans = BTreeMap::new();
        let mut taken_rows = 0;
        for plan in &contract.plans {
            if !pending.returning.plan_indexes.contains(&plan.plan_index) {
                continue;
            }
            let Some(rows) = pending.returning.images.remove(&plan.plan_index) else {
                return ReturningGate::Refuse(ReturningRefusal::internal(format!(
                    "calvin RETURNING: the stage answered no images for plan {}",
                    plan.plan_index
                )));
            };
            taken_rows += rows.len();
            plans.insert(plan.plan_index, rows);
        }
        pending.returning.taken_rows += taken_rows;
        if plans.is_empty() {
            return ReturningGate::Open;
        }
        ReturningGate::Evaluate(ReturningJob {
            shared: Arc::clone(&self.shared),
            tx_class: Arc::clone(&pending.txn.tx_class),
            plans,
            transcripts: Arc::clone(&pending.returning.transcripts),
        })
    }

    /// Run `job` on a blocking thread. Its result returns to the loop as an
    /// [`Evaluated`].
    fn spawn_returning_eval(&mut self, txn_id: TxnId, job: ReturningJob) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.vote_refused(
                txn_id,
                ReturningRefusal::internal(
                    "calvin RETURNING: the evaluation needs a Tokio runtime".to_owned(),
                ),
            );
            return;
        };
        let seq = self.returning_evals.next_seq();
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.returning.evaluating = Some(seq);
        }
        let sender = self.returning_evals.sender();
        let vshard_id = self.vshard_id;
        handle.spawn(async move {
            let outcome = match tokio::task::spawn_blocking(move || evaluate(job)).await {
                Ok(outcome) => outcome,
                Err(error) => Err(ReturningRefusal::internal(format!(
                    "calvin RETURNING: the evaluation stopped: {error}"
                ))),
            };
            if sender
                .send(Evaluated {
                    txn_id,
                    seq,
                    outcome,
                })
                .is_err()
            {
                tracing::debug!(
                    vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: the scheduler left before its RETURNING evaluation finished"
                );
            }
        });
    }

    /// Cast the vote a finished evaluation decides. A result for a txn that
    /// left `Staged`, or for an evaluation it no longer waits for, changes
    /// nothing.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_returning_eval(
        &mut self,
        evaluated: Evaluated,
    ) {
        let Evaluated {
            txn_id,
            seq,
            outcome,
        } = evaluated;
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return;
        };
        if pending.commit_state != CommitState::Staged || pending.returning.evaluating != Some(seq)
        {
            return;
        }
        pending.returning.evaluating = None;
        match outcome {
            Ok(frozen) => {
                pending.returning.frozen = frozen;
                self.cast_staged_vote(txn_id, StagedVote::Commit, None);
            }
            Err(refusal) => self.vote_refused(txn_id, refusal),
        }
    }

    /// Vote abort for `txn_id` with `refusal`. A code with no wire form
    /// still votes abort, with no refusal.
    fn vote_refused(&mut self, txn_id: TxnId, refusal: ReturningRefusal) {
        tracing::warn!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            detail = %refusal.detail,
            "calvin: the RETURNING evaluation refuses the slice; voting abort"
        );
        let encoded = match encode_refusal(&refusal.code) {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: the RETURNING refusal has no wire form; voting abort without it"
                );
                None
            }
        };
        self.cast_staged_vote(
            txn_id,
            StagedVote::ParticipantError { refusal: encoded },
            Some(refusal.detail),
        );
    }

    /// Wait for one evaluation and cast its vote.
    #[cfg(test)]
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn settle_returning_eval(
        &mut self,
    ) {
        if let Some(evaluated) = self.returning_evals.recv().await {
            self.finish_returning_eval(evaluated);
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::SequencerEntry;
    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::super::fixtures::{
        contract, create_sequence, image_rows, nextval, sequence_next, with_contract,
    };
    use super::*;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, staged_response,
        test_coll_vshard,
    };

    /// Stage `txn_id` with two image rows, answer it with a commit stage
    /// vote, and wait for its evaluation.
    async fn stage_and_vote(scheduler: &mut Scheduler, txn_id: TxnId) {
        let pending = scheduler.pending.get_mut(&txn_id).expect("staged");
        assert_eq!(pending.returning.plan_indexes, vec![0]);
        pending.returning.images.insert(0, image_rows(2));
        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        scheduler.settle_returning_eval().await;
    }

    fn frozen_payload(scheduler: &Scheduler, txn_id: TxnId) -> Vec<u8> {
        scheduler
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.returning.frozen.get(&0))
            .map(|reply| reply.payload.clone())
            .expect("a frozen reply for plan 0")
    }

    /// The leader evaluates, votes commit, loses its leadership, and stages
    /// the txn again once it leads again, before any verdict. The second
    /// evaluation replays the transcript: it draws no new value and freezes
    /// the same reply.
    #[tokio::test]
    async fn a_same_leader_restage_replays_without_a_new_nextval() {
        let registry = nodedb_cluster::calvin::CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        create_sequence(&scheduler.shared, "s");
        let txn_id = TxnId::new(3, 0);
        let txn = with_contract(make_local_write_txn(3, 0), contract(vec![nextval("s")], 10));
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));

        stage_and_vote(&mut scheduler, txn_id).await;
        let first = frozen_payload(&scheduler, txn_id);
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::Vote {
                epoch: 3,
                position: 0,
                vshard: test_coll_vshard(),
            }]
        );

        step_down_from_data_group(&scheduler);
        scheduler.refresh_role();
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        lead_data_group(&mut scheduler);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged),
            "the new term stages the txn again"
        );

        stage_and_vote(&mut scheduler, txn_id).await;
        assert_eq!(frozen_payload(&scheduler, txn_id), first);
        assert_eq!(
            sequence_next(&scheduler.shared, "s"),
            3,
            "two rows drew two values, once"
        );
        assert!(scheduler.returning_evals.holds(txn_id));
    }
}
