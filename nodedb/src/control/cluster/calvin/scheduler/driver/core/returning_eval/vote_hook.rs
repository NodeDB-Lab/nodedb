// SPDX-License-Identifier: BUSL-1.1

//! Hold a staged txn's commit vote for its `RETURNING` evaluation.
//!
//! A slice that would vote commit and holds a plan its contract names runs
//! the evaluation first, in rounds (see [`super::rounds`]). The evaluation
//! can wait on a sequence range reservation, so it runs on a blocking
//! thread, and the scheduler loop goes on meanwhile. The txn stays `Staged`
//! with its locks until the last round is stored, so a later txn that
//! needs one of its keys still waits behind it. A failed evaluation votes
//! abort with its refusal. A finished round is proposed as the slice's
//! frozen-reply entry (see [`super::frozen_entry`]). Once the stored reply
//! holds the round, the leader sends its sequence changes on and starts the
//! next round, or votes commit after the last.
//!
//! A slice whose stored reply already holds every local plan, from an
//! earlier stage or an earlier leader, sends its changes and votes commit
//! at once. It evaluates nothing.

use super::super::scheduler::Scheduler;
use super::super::staged_vote::StagedVote;
use super::evals::Evaluated;
use super::evaluate::{ReturningRefusal, evaluate};
use super::rounds::{ReturningGate, Round};
use crate::control::cluster::calvin::refusal::encode_refusal_with_sequences;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Hold `txn_id`'s commit vote for its `RETURNING` evaluation.
    ///
    /// Returns `false` when the vote waits for nothing. Returns `true` once
    /// this method owns the vote: the evaluation runs or waits, or the slice
    /// already voted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn hold_vote_for_returning(
        &mut self,
        txn_id: TxnId,
    ) -> bool {
        let gate = self.returning_gate(txn_id);
        if matches!(gate, ReturningGate::Open) {
            return false;
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.returning.holds_vote = true;
        }
        self.act_on_gate(txn_id, gate);
        true
    }

    /// Take the next step of `txn_id`'s evaluation, when its vote waits for
    /// one: no round in flight and no round's entry owed. Runs after a
    /// round is stored, and after the barrier log took entries of the txn.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn advance_returning(
        &mut self,
        txn_id: TxnId,
    ) {
        let awaits = self.pending.get(&txn_id).is_some_and(|pending| {
            pending.commit_state == CommitState::Staged
                && pending.returning.holds_vote
                && pending.returning.evaluating.is_none()
                && pending.returning.frozen_entry.is_none()
        });
        if !awaits {
            return;
        }
        let gate = self.returning_gate(txn_id);
        self.act_on_gate(txn_id, gate);
    }

    /// Whether `txn_id`'s vote waits for another participant's sequence
    /// changes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn waits_for_sequences(
        &self,
        txn_id: TxnId,
    ) -> bool {
        self.pending.get(&txn_id).is_some_and(|pending| {
            pending.commit_state == CommitState::Staged
                && pending.returning.holds_vote
                && pending.returning.wait_due.is_some()
        })
    }

    /// Act on `gate`, what `txn_id`'s held vote waits for.
    fn act_on_gate(&mut self, txn_id: TxnId, gate: ReturningGate) {
        match gate {
            // Nothing more holds the vote.
            ReturningGate::Open => {
                self.clear_wait(txn_id);
                self.cast_staged_vote(txn_id, StagedVote::Commit, None);
            }
            ReturningGate::Stored(stored) => {
                self.clear_wait(txn_id);
                self.send_sequence_effects(txn_id, &stored);
                self.cast_staged_vote(txn_id, StagedVote::Commit, None);
            }
            ReturningGate::Evaluate(round) => {
                self.clear_wait(txn_id);
                if let Some(stored) = &round.stored {
                    self.send_sequence_effects(txn_id, stored);
                }
                self.spawn_returning_eval(txn_id, round);
            }
            ReturningGate::Wait { stored } => {
                if let Some(stored) = &stored {
                    self.send_sequence_effects(txn_id, stored);
                }
                let due = self.config.passive_timeout();
                if let Some(pending) = self.pending.get_mut(&txn_id) {
                    // no-determinism: the leader's clock decides only when it proposes the timeout entry.
                    let now = std::time::Instant::now();
                    pending.returning.wait_due.get_or_insert(now + due);
                }
            }
            ReturningGate::Refuse(refusal) => {
                self.clear_wait(txn_id);
                self.vote_refused(txn_id, refusal);
            }
        }
    }

    fn clear_wait(&mut self, txn_id: TxnId) {
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.returning.wait_due = None;
        }
    }

    /// Run `round` on a blocking thread. Its result returns to the loop as
    /// an [`Evaluated`].
    fn spawn_returning_eval(&mut self, txn_id: TxnId, round: Round) {
        let Round { job, stored, plans } = round;
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
        let base: std::collections::BTreeSet<u32> = stored
            .iter()
            .flat_map(|reply| &reply.plans)
            .map(|plan| plan.plan_index)
            .collect();
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
                    base,
                    round: plans,
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
            base,
            round,
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
                self.propose_frozen_reply(txn_id, base, round);
            }
            Err(refusal) => self.vote_refused(txn_id, refusal),
        }
    }

    /// Vote abort for `txn_id` with `refusal`, its code and the sequence
    /// results it drew. A refusal with no wire form still votes abort, with
    /// no refusal.
    pub(super) fn vote_refused(&mut self, txn_id: TxnId, refusal: ReturningRefusal) {
        tracing::warn!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            detail = %refusal.detail,
            "calvin: the RETURNING evaluation refuses the slice; voting abort"
        );
        let encoded = if !refusal.reported {
            // The verdict reports the cause this refusal follows.
            None
        } else {
            match encode_refusal_with_sequences(&refusal.code, refusal.sequences.as_ref()) {
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
        apply_frozen_reply, contract, create_sequence, image_rows, nextval, sequence_next,
        with_contract,
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
    async fn stage_and_evaluate(scheduler: &mut Scheduler, txn_id: TxnId) {
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

    fn commit_vote() -> SequencerEntry {
        SequencerEntry::Vote {
            epoch: 3,
            position: 0,
            vshard: test_coll_vshard(),
        }
    }

    /// The leader evaluates and proposes its frozen-reply entry, then loses
    /// its leadership before the entry applies. It stages the txn again
    /// once it leads again, before any verdict. The second evaluation
    /// replays the transcript: it draws no new value and freezes the same
    /// reply. The vote waits for the stored reply.
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

        stage_and_evaluate(&mut scheduler, txn_id).await;
        let first = frozen_payload(&scheduler, txn_id);
        assert!(
            proposer.accepted().is_empty(),
            "the vote waits for the entry"
        );

        step_down_from_data_group(&scheduler);
        scheduler.refresh_role();
        let pending = scheduler.pending.get(&txn_id).expect("held");
        assert_eq!(pending.commit_state, CommitState::Following);
        assert!(
            pending.returning.frozen.is_empty() && pending.returning.frozen_entry.is_none(),
            "a follower holds no frozen state"
        );
        lead_data_group(&mut scheduler);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged),
            "the new term stages the txn again"
        );

        stage_and_evaluate(&mut scheduler, txn_id).await;
        assert_eq!(frozen_payload(&scheduler, txn_id), first);
        apply_frozen_reply(&scheduler, txn_id);
        scheduler.vote_stored_frozen_replies();
        assert_eq!(proposer.accepted(), vec![commit_vote()]);
        assert_eq!(
            sequence_next(&scheduler.shared, "s"),
            3,
            "two rows drew two values, once"
        );
        assert!(scheduler.returning_evals.holds(txn_id));
    }

    /// The leader proposes the frozen replies to its data group before it
    /// votes, and votes commit once the entry applies here.
    #[tokio::test]
    async fn the_frozen_entry_is_proposed_before_the_vote() {
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

        stage_and_evaluate(&mut scheduler, txn_id).await;
        assert!(
            proposer.accepted().is_empty(),
            "no vote before the entry applies"
        );
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
        scheduler.vote_stored_frozen_replies();
        assert!(proposer.accepted().is_empty(), "nothing is stored yet");

        let reply = apply_frozen_reply(&scheduler, txn_id);
        assert_eq!((reply.epoch, reply.position), (3, 0));
        assert_eq!(reply.vshard, test_coll_vshard());
        assert_eq!(
            reply
                .plans
                .iter()
                .map(|plan| plan.plan_index)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(reply.plans[0].payload, frozen_payload(&scheduler, txn_id));
        assert_eq!(
            reply.plans[0].sequence_effects,
            vec![
                crate::control::wal_replication::types::FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 1,
                },
                crate::control::wal_replication::types::FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 2,
                },
            ]
        );

        scheduler.vote_stored_frozen_replies();
        assert_eq!(proposer.accepted(), vec![commit_vote()]);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
    }

    /// A stage of a txn whose frozen reply is stored votes commit at once.
    /// It evaluates nothing and draws no value.
    #[tokio::test]
    async fn a_stage_with_a_stored_reply_votes_without_evaluating() {
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
        crate::control::cluster::calvin::scheduler::frozen_replies::store(
            scheduler.shared.credentials.catalog(),
            &super::super::fixtures::stored_plan_zero(test_coll_vshard(), txn_id),
        )
        .expect("store");

        let pending = scheduler.pending.get_mut(&txn_id).expect("staged");
        pending.returning.images.insert(0, image_rows(2));
        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );

        assert_eq!(proposer.accepted(), vec![commit_vote()]);
        let pending = scheduler.pending.get(&txn_id).expect("parked");
        assert_eq!(pending.returning.evaluating, None);
        assert!(pending.returning.frozen.is_empty());
        assert_eq!(sequence_next(&scheduler.shared, "s"), 1, "no value drawn");
    }
}
