// SPDX-License-Identifier: BUSL-1.1

//! The next round of a slice's `RETURNING` evaluation.
//!
//! A slice evaluates its local plans in plan order, in one session. A plan
//! whose contract entry follows another participant's plans waits for
//! their sequence changes, which reach this vShard's barrier log (see
//! [`super::sequence_send`]). A round is the longest run of the slice's
//! unstored plans whose changes are all counted in the log. The leader
//! evaluates the round, stores it with every plan stored before it, sends
//! the round's changes on, and starts the next round. The last round's
//! store casts the commit vote.
//!
//! Every input of a round is in a log: the stored plans in this vShard's
//! frozen-reply row, the other participants' changes in its barrier log. So
//! a new leader that evaluates a round again reads the same inputs.
//!
//! The session takes every change in statement order, `(plan, vShard)`:
//! each stored plan's changes, each counted source's, and each round
//! plan's own as it evaluates.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::super::scheduler::Scheduler;
use super::evaluate::{EvalStep, ReturningJob, ReturningRefusal};
use crate::control::cluster::calvin::scheduler::BarrierLog;
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::types::{CalvinFrozenReply, FrozenSequenceEffect};

/// What a slice's commit vote waits for.
pub(super) enum ReturningGate {
    /// Nothing: the slice holds no plan its contract names.
    Open,
    /// Nothing more: the stored reply holds every local plan.
    Stored(CalvinFrozenReply),
    /// The evaluation of the next round.
    Evaluate(Round),
    /// Another participant's sequence changes. `stored` is the stored
    /// reply, whose changes this leader sends on meanwhile.
    Wait { stored: Option<CalvinFrozenReply> },
    /// Nothing more: the slice cannot commit.
    Refuse(ReturningRefusal),
}

/// One round's evaluation and what it builds on.
pub(super) struct Round {
    pub job: ReturningJob,
    /// The stored reply the round follows.
    pub stored: Option<CalvinFrozenReply>,
    /// The plans the round evaluates.
    pub plans: BTreeSet<u32>,
}

impl Scheduler {
    /// What `txn_id`'s commit vote waits for now. Takes the images of the
    /// round it hands out.
    pub(super) fn returning_gate(&mut self, txn_id: TxnId) -> ReturningGate {
        let vshard_id = self.vshard_id;
        // Barrier entries the apply loop buffered before this scheduler
        // held the txn.
        self.absorb_buffered_reads(txn_id);
        let Some(pending) = self.pending.get(&txn_id) else {
            return ReturningGate::Open;
        };
        if let Some(failed) = &pending.returning.failed {
            return ReturningGate::Refuse(ReturningRefusal::internal(failed.clone()));
        }
        let tx_class = Arc::clone(&pending.txn.tx_class);
        let Some(contract) = tx_class.returning.as_ref() else {
            return ReturningGate::Open;
        };
        let local: Vec<u32> = contract
            .plans
            .iter()
            .map(|plan| plan.plan_index)
            .filter(|index| pending.returning.plan_indexes.contains(index))
            .collect();
        if local.is_empty() {
            return ReturningGate::Open;
        }
        let stored =
            match frozen_replies::load(self.shared.credentials.catalog(), vshard_id, txn_id) {
                Ok(stored) => stored,
                Err(error) => {
                    crate::diag::calvin_frozen_reply_store_failed(
                        vshard_id,
                        Some((txn_id.epoch, txn_id.position)),
                        "load",
                        &error,
                    );
                    return ReturningGate::Refuse(ReturningRefusal::internal(format!(
                        "calvin RETURNING: the stored frozen reply does not read: {error}"
                    )));
                }
            };
        let held_plans: BTreeMap<u32, Vec<FrozenSequenceEffect>> = stored
            .iter()
            .flat_map(|reply| &reply.plans)
            .map(|plan| (plan.plan_index, plan.sequence_effects.clone()))
            .collect();
        if let Some(stored) = stored
            .as_ref()
            .filter(|_| local.iter().all(|index| held_plans.contains_key(index)))
        {
            return ReturningGate::Stored(stored.clone());
        }

        let empty = BarrierLog::default();
        let log = self.barrier_logs.get(&txn_id).unwrap_or(&empty);
        let mut changes: BTreeMap<(u32, u32), Vec<FrozenSequenceEffect>> = BTreeMap::new();
        let mut seeded: BTreeSet<(u32, u32)> = BTreeSet::new();
        let mut steps = Vec::new();
        let mut round = BTreeSet::new();
        for index in &local {
            let mut missing = false;
            for (plan, source) in contract.sequence_sources(*index, vshard_id) {
                if log.sequence_refused(plan, source) {
                    return ReturningGate::Refuse(ReturningRefusal::follows(format!(
                        "calvin RETURNING: the participant of plan {plan} on vShard {source} \
                         refused before its sequence changes were stored"
                    )));
                }
                match log.sequence_effects(plan, source) {
                    Some(effects) => {
                        changes.insert((plan, source), effects.to_vec());
                    }
                    None => {
                        missing = true;
                        break;
                    }
                }
            }
            if missing {
                break;
            }
            let own = (*index, vshard_id);
            for (key, effects) in changes.range(..own) {
                if seeded.insert(*key) {
                    steps.push(EvalStep::Seed(effects.clone()));
                }
            }
            match held_plans.get(index) {
                Some(effects) => {
                    changes.insert(own, effects.clone());
                }
                None => {
                    steps.push(EvalStep::Plan(*index));
                    round.insert(*index);
                }
            }
        }
        if round.is_empty() {
            return self.wait_or_refuse(txn_id, log.timed_out(), stored);
        }

        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return ReturningGate::Open;
        };
        // A stored plan evaluates no more. A round plan keeps its images
        // until it is stored: another copy can supersede the round, which
        // then evaluates again.
        for index in held_plans.keys() {
            if let Some(rows) = pending.returning.images.remove(index) {
                pending.returning.taken_rows += rows.len();
            }
        }
        let mut plans = BTreeMap::new();
        for index in &round {
            let Some(rows) = pending.returning.images.get(index) else {
                return ReturningGate::Refuse(ReturningRefusal::internal(format!(
                    "calvin RETURNING: the stage answered no images for plan {index}"
                )));
            };
            plans.insert(*index, rows.clone());
        }
        ReturningGate::Evaluate(Round {
            job: ReturningJob {
                shared: Arc::clone(&self.shared),
                tx_class,
                steps,
                plans,
                transcripts: Arc::clone(&pending.returning.transcripts),
            },
            stored,
            plans: round,
        })
    }

    /// The gate of a slice whose next plan waits for another participant's
    /// changes. The log's timeout entry, or an abort verdict, ends the
    /// wait: no change counts after the timeout, and an aborted txn never
    /// commits.
    fn wait_or_refuse(
        &self,
        txn_id: TxnId,
        timed_out: bool,
        stored: Option<CalvinFrozenReply>,
    ) -> ReturningGate {
        if timed_out {
            return ReturningGate::Refuse(ReturningRefusal::follows(
                "calvin RETURNING: the barrier timed out before the sequence changes of an \
                 earlier plan arrived"
                    .to_owned(),
            ));
        }
        let verdict = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        ));
        if verdict == Some(false) {
            return ReturningGate::Refuse(ReturningRefusal::follows(
                "calvin RETURNING: an abort verdict reached a plan that waits for an earlier \
                 plan's sequence changes"
                    .to_owned(),
            ));
        }
        ReturningGate::Wait { stored }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::{PlanReturning, ReturningContract, SchedulerInput};
    use nodedb_cluster::calvin::{AbortReason, CalvinCompletionRegistry, SequencerEntry};
    use nodedb_query::expr::{BinaryOp, SqlExpr};
    use nodedb_types::{ReturningItem, Value};

    use super::super::fixtures::{
        apply_frozen_reply, contract, create_sequence, division_by_zero, image_rows, nextval,
        with_contract,
    };
    use super::*;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::BarrierEvent;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, staged_response,
        test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::wal_replication::types::PlanSequenceEffects;

    const TXN: TxnId = TxnId {
        epoch: 3,
        position: 0,
    };
    /// The vShard of the other plan: not the scheduler's own.
    fn peer() -> u32 {
        test_coll_vshard().wrapping_add(1)
    }

    fn currval_plus_one() -> ReturningItem {
        ReturningItem::Computed {
            name: "n".into(),
            expression: zerompk::to_msgpack_vec(&SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Function {
                    name: "currval".into(),
                    args: vec![SqlExpr::Literal(Value::String("s".into()))],
                }),
                op: BinaryOp::Add,
                right: Box::new(SqlExpr::Literal(Value::Integer(1))),
            })
            .expect("encode expression"),
        }
    }

    /// Plan `index` of `items`, evaluated on `vshards`, after the plans
    /// `after`.
    fn plan(
        index: u32,
        items: Vec<ReturningItem>,
        vshards: &[u32],
        after: &[u32],
    ) -> PlanReturning {
        let mut plan = contract(items, 10).plans.remove(0);
        plan.plan_index = index;
        plan.vshards = vshards.to_vec();
        plan.sequence_after = after.to_vec();
        plan
    }

    fn two_plans(first: PlanReturning, second: PlanReturning) -> ReturningContract {
        ReturningContract {
            plans: vec![first, second],
            ..contract(Vec::new(), 10)
        }
    }

    /// A leader of the scheduler's vShard holding `TXN` with `contract`.
    fn leader(
        contract: ReturningContract,
    ) -> (
        Scheduler,
        (
            tempfile::TempDir,
            crate::bridge::dispatch::CoreChannelDataSide,
        ),
        Arc<CapturingProposer>,
        Arc<CalvinCompletionRegistry>,
    ) {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(&registry));
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        create_sequence(&scheduler.shared, "s");
        let txn = with_contract(make_local_write_txn(TXN.epoch, TXN.position), contract);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        (scheduler, (dir, data_side), proposer, registry)
    }

    /// Answer the stage of `TXN` with one image row of local plan `local`,
    /// with a commit stage vote.
    fn stage(scheduler: &mut Scheduler, local: u32) {
        let pending = scheduler.pending.get_mut(&TXN).expect("staged");
        assert_eq!(pending.commit_state, CommitState::Staged);
        pending.returning.plan_indexes = vec![local];
        pending.returning.images.insert(local, image_rows(1));
        scheduler.resolve_staged_commit(TXN, &staged_response(Status::Ok, Some(StageVote::Commit)));
    }

    fn waits(scheduler: &Scheduler) -> bool {
        let pending = scheduler.pending.get(&TXN).expect("held");
        pending.commit_state == CommitState::Staged
            && pending.returning.evaluating.is_none()
            && pending.returning.frozen_entry.is_none()
            && pending.returning.wait_due.is_some()
    }

    fn change_new_leader(scheduler: &mut Scheduler) {
        step_down_from_data_group(scheduler);
        scheduler.refresh_role();
        assert_eq!(
            scheduler
                .pending
                .get(&TXN)
                .map(|pending| pending.commit_state),
            Some(CommitState::Following)
        );
        lead_data_group(scheduler);
    }

    /// The earlier plan's `nextval('s')` result 7, as the log carries it.
    fn earlier_changes() -> BarrierEvent {
        BarrierEvent::Sequences {
            source_vshard: peer(),
            plans: vec![PlanSequenceEffects {
                plan_index: 0,
                effects: vec![FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 7,
                }],
                refused: false,
            }],
        }
    }

    fn frozen_n(scheduler: &Scheduler) -> (Value, Vec<u8>) {
        let reply = scheduler
            .pending
            .get(&TXN)
            .and_then(|pending| pending.returning.frozen.get(&1))
            .expect("a frozen reply for plan 1");
        (reply.shaped.rows[0]["n"].clone(), reply.payload.clone())
    }

    fn commit_vote() -> SequencerEntry {
        SequencerEntry::Vote {
            epoch: TXN.epoch,
            position: TXN.position,
            vshard: test_coll_vshard(),
        }
    }

    /// Plan 1 reads `currval('s') + 1` after plan 0 drew on another vShard.
    /// It waits with no vote, across a leader change, until plan 0's
    /// changes reach the log. It evaluates `7 + 1`. A leader change before
    /// its reply is stored evaluates again from the same log, freezes the
    /// same reply, and votes commit once it is stored.
    #[tokio::test]
    async fn a_dependent_plan_waits_for_the_barrier_and_answers_alike_after_a_leader_change() {
        let (mut scheduler, _dir, proposer, _registry) = leader(two_plans(
            plan(0, vec![nextval("s")], &[peer()], &[]),
            plan(1, vec![currval_plus_one()], &[test_coll_vshard()], &[0]),
        ));
        stage(&mut scheduler, 1);
        assert!(waits(&scheduler), "plan 1 waits for plan 0's changes");
        assert!(proposer.accepted().is_empty(), "no vote while it waits");

        change_new_leader(&mut scheduler);
        stage(&mut scheduler, 1);
        assert!(waits(&scheduler), "the new leader waits too");

        scheduler
            .read_results
            .push(TXN, earlier_changes(), 64, false);
        scheduler.drain_read_results();
        scheduler.settle_returning_eval().await;
        let (n, first) = frozen_n(&scheduler);
        assert_eq!(n, Value::Integer(8), "currval is plan 0's draw");

        change_new_leader(&mut scheduler);
        stage(&mut scheduler, 1);
        scheduler.settle_returning_eval().await;
        assert_eq!(frozen_n(&scheduler).1, first, "the same frozen reply");

        let stored = apply_frozen_reply(&scheduler, TXN);
        assert_eq!(stored.plans[0].payload, first);
        scheduler.vote_stored_frozen_replies();
        assert_eq!(proposer.accepted(), vec![commit_vote()]);
    }

    /// The earlier plan's leader sends its stored changes on before it
    /// votes commit.
    #[tokio::test]
    async fn an_earlier_plan_sends_its_stored_changes_then_votes() {
        let (mut scheduler, _dir, proposer, _registry) = leader(two_plans(
            plan(0, vec![nextval("s")], &[test_coll_vshard()], &[]),
            plan(1, vec![currval_plus_one()], &[peer()], &[0]),
        ));
        stage(&mut scheduler, 0);
        scheduler.settle_returning_eval().await;
        apply_frozen_reply(&scheduler, TXN);
        scheduler.vote_stored_frozen_replies();

        let pending = scheduler.pending.get(&TXN).expect("parked");
        assert!(pending.returning.sent.contains(&0), "plan 0's changes went");
        assert_eq!(pending.commit_state, CommitState::AwaitingVerdict);
        assert_eq!(proposer.accepted(), vec![commit_vote()]);
    }

    fn abort_reasons(proposer: &CapturingProposer) -> Vec<AbortReason> {
        proposer
            .accepted()
            .into_iter()
            .filter_map(|entry| match entry {
                SequencerEntry::AbortVote { reason, .. } => Some(reason),
                _ => None,
            })
            .collect()
    }

    /// The log's timeout entry below the earlier plan's changes ends the
    /// wait with an abort vote. The late changes do not count.
    #[tokio::test]
    async fn a_timeout_before_the_changes_votes_abort() {
        let (mut scheduler, _dir, proposer, _registry) = leader(two_plans(
            plan(0, vec![nextval("s")], &[peer()], &[]),
            plan(1, vec![currval_plus_one()], &[test_coll_vshard()], &[0]),
        ));
        stage(&mut scheduler, 1);
        scheduler
            .read_results
            .push(TXN, BarrierEvent::Timeout, 64, false);
        scheduler
            .read_results
            .push(TXN, earlier_changes(), 64, false);
        scheduler.drain_read_results();

        assert_eq!(
            abort_reasons(&proposer),
            vec![AbortReason::ParticipantError]
        );
        let pending = scheduler.pending.get(&TXN).expect("parked");
        assert_eq!(pending.returning.evaluating, None, "nothing evaluated");
    }

    /// The earlier participant's refusal ends the wait at once: the slice
    /// votes abort with no refusal of its own, so the verdict reports the
    /// earlier one, and the vote completes the tally.
    #[tokio::test]
    async fn an_earlier_refusal_votes_abort_without_a_refusal() {
        let (mut scheduler, _dir, proposer, _registry) = leader(two_plans(
            plan(0, vec![nextval("s")], &[peer()], &[]),
            plan(1, vec![currval_plus_one()], &[test_coll_vshard()], &[0]),
        ));
        stage(&mut scheduler, 1);
        let refusal = BarrierEvent::Sequences {
            source_vshard: peer(),
            plans: vec![PlanSequenceEffects {
                plan_index: 0,
                effects: Vec::new(),
                refused: true,
            }],
        };
        scheduler.read_results.push(TXN, refusal, 64, false);
        scheduler.drain_read_results();

        let votes: Vec<(AbortReason, bool)> = proposer
            .accepted()
            .into_iter()
            .filter_map(|entry| match entry {
                SequencerEntry::AbortVote {
                    reason, refusal, ..
                } => Some((reason, refusal.is_some())),
                _ => None,
            })
            .collect();
        assert_eq!(votes, vec![(AbortReason::ParticipantError, false)]);
    }

    /// An abort vote marks every local plan sent: its refusal went to each
    /// participant that follows it.
    #[tokio::test]
    async fn an_abort_vote_sends_the_refusal_of_every_local_plan() {
        let (mut scheduler, _dir, proposer, _registry) = leader(two_plans(
            plan(0, vec![division_by_zero()], &[test_coll_vshard()], &[]),
            plan(1, vec![currval_plus_one()], &[peer()], &[0]),
        ));
        stage(&mut scheduler, 0);
        scheduler.settle_returning_eval().await;

        assert_eq!(
            abort_reasons(&proposer),
            vec![AbortReason::ParticipantError]
        );
        let pending = scheduler.pending.get(&TXN).expect("parked");
        assert!(pending.returning.sent.contains(&0), "plan 0's refusal went");
    }

    /// An abort verdict ends the wait: the slice votes abort, evaluates
    /// nothing, and drops.
    #[tokio::test]
    async fn an_abort_verdict_ends_the_wait() {
        let (mut scheduler, _dir, proposer, registry) = leader(two_plans(
            plan(0, vec![nextval("s")], &[peer()], &[]),
            plan(1, vec![currval_plus_one()], &[test_coll_vshard()], &[0]),
        ));
        stage(&mut scheduler, 1);
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(TXN.epoch, TXN.position),
            nodedb_cluster::calvin::VerdictOutcome::Abort(AbortReason::ParticipantError.into()),
        );
        scheduler.resume_on_verdict(TXN, false);

        assert_eq!(
            abort_reasons(&proposer),
            vec![AbortReason::ParticipantError]
        );
        assert_eq!(
            scheduler
                .pending
                .get(&TXN)
                .map(|pending| pending.commit_state),
            Some(CommitState::AwaitingDrop)
        );
    }
}
