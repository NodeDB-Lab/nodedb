// SPDX-License-Identifier: BUSL-1.1

//! A transaction the leader cannot stage: its plans are rejected, or its
//! dependent reads never arrived.
//!
//! The txn still takes the commit barrier. It enters `pending`, holds its
//! locks, and owes an abort vote. At the abort verdict it drops like any
//! staged txn: it proposes its `CompletionAck`, releases its locks, and only
//! then marks its position applied. A follower rejects the same plans and
//! holds the txn `Following` for the same verdict.

use std::time::Instant;

use tracing::error;

use nodedb_cluster::calvin::AbortReason;
use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::owed::SchedulerProposal;
use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::types::{
    CommitState, PendingTxn, SliceScope,
};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Park `txn` with an abort vote for plans this vShard rejects.
    ///
    /// Plan decode, part assembly, and plan routing read only replicated
    /// input. So every replica rejects the same plans, and the leader votes
    /// `PlanRejected`. A follower owes no vote: it holds the txn for the
    /// abort verdict.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn reject_plan(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
        error: &crate::Error,
    ) {
        if !self.role.is_leader() {
            self.follow(txn, txn_id, lock_owner);
            return;
        }
        error!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            error = %error,
            "calvin scheduler: plans rejected; voting abort"
        );
        self.park_unstaged(
            txn,
            txn_id,
            lock_owner,
            AbortReason::PlanRejected,
            error.to_string(),
        );
    }

    /// Park `txn`, which never staged on this replica, on the commit barrier
    /// with an abort vote for `reason`.
    ///
    /// `stage_error` stays on the pending entry. This leader holds nothing
    /// to resolve, so a COMMIT verdict restages the txn through the grant
    /// path: the plans route again, or the barrier opens again.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn park_unstaged(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
        reason: AbortReason,
        stage_error: String,
    ) {
        // no-determinism: dispatch time and stall deadline are observability only; the replicated verdict decides.
        let now = Instant::now();
        self.pending.insert(
            txn_id,
            PendingTxn {
                txn,
                lock_owner,
                dispatch_time: now,
                has_primary_write: false,
                raises_write_mark: false,
                has_returning: false,
                commit_state: CommitState::AwaitingVerdict,
                awaiting: None,
                verdict_deadline: Some(now + self.config.verdict_stall_warn()),
                stage_error: Some(stage_error),
                scope: SliceScope::of_plans(&[]),
                redo: None,
                superseded: false,
                gates: Vec::new(),
                ungated: false,
            },
        );
        self.propose_sequencer_entry(
            txn_id,
            SchedulerProposal::Vote {
                abort: Some(reason),
            },
        );
        // The verdict can be stored already: on replay, or when a peer's
        // vote completed the tally first.
        if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        )) {
            self.resume_on_verdict(txn_id, verdict);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::types::SchedulerInput;
    use nodedb_cluster::calvin::{
        AttemptOutcome, CalvinCompletionRegistry, ParticipantVote, SequencerEntry,
    };

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_sequenced_txn, staged_pending, staged_response,
    };

    const REJECTING: u32 = 7;
    const PEER: u32 = 8;

    /// A scheduler on `vshard` that leads its data group and captures its
    /// proposals.
    fn leading_scheduler(
        vshard: u32,
        registry: &Arc<CalvinCompletionRegistry>,
    ) -> (
        Scheduler,
        tempfile::TempDir,
        CoreChannelDataSide,
        Arc<CapturingProposer>,
    ) {
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(vshard, Arc::clone(registry));
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        (scheduler, dir, data_side, proposer)
    }

    /// Apply every vote `proposer` accepted to `registry`, as the sequencer
    /// log applies them on every replica.
    fn apply_votes(registry: &CalvinCompletionRegistry, proposer: &CapturingProposer) {
        for entry in proposer.accepted() {
            if let SequencerEntry::Vote {
                epoch,
                position,
                vshard,
            } = entry
            {
                registry.note_vote(
                    nodedb_cluster::calvin::TxnId::new(epoch, position),
                    vshard,
                    ParticipantVote::Commit,
                );
            } else if let SequencerEntry::AbortVote {
                epoch,
                position,
                vshard,
                reason,
            } = entry
            {
                registry.note_vote(
                    nodedb_cluster::calvin::TxnId::new(epoch, position),
                    vshard,
                    ParticipantVote::Abort(reason),
                );
            }
        }
    }

    /// A txn whose plan bytes do not decode.
    fn undecodable_txn(epoch: u64, position: u32) -> SequencedTxn {
        let mut txn = make_sequenced_txn(epoch, position);
        txn.tx_class.plans = vec![0xc1];
        txn
    }

    /// A rejected plan votes `PlanRejected`. The tally completes with the
    /// peer's commit vote, the verdict aborts, and both participants drop,
    /// ack, release their locks, and mark the position applied.
    #[tokio::test]
    async fn a_rejected_plan_aborts_every_participant_through_the_verdict() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut rejecting, _dir_r, _data_r, proposer_r) = leading_scheduler(REJECTING, &registry);
        let (mut peer, _dir_p, _data_p, proposer_p) = leading_scheduler(PEER, &registry);
        let txn_id = TxnId::new(30, 0);
        let cluster_txn = nodedb_cluster::calvin::TxnId::new(30, 0);
        registry.seed_expected(cluster_txn, 2);
        let outcome = registry.register_completion(cluster_txn, 2);

        rejecting.process_scheduler_input(SchedulerInput::Txn(Box::new(undecodable_txn(30, 0))));
        // A later txn on the same key waits behind the rejected txn's locks.
        rejecting.process_scheduler_input(SchedulerInput::Txn(Box::new(undecodable_txn(31, 0))));
        assert!(rejecting.blocked.contains_key(&TxnId::new(31, 0)));
        assert_eq!(
            rejecting.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        assert!(!rejecting.applied.is_applied(30, 0));

        peer.pending
            .insert(txn_id, staged_pending(make_sequenced_txn(30, 0), txn_id));
        peer.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert_eq!(
            peer.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );

        apply_votes(&registry, &proposer_r);
        apply_votes(&registry, &proposer_p);
        assert!(proposer_r.accepted().contains(&SequencerEntry::AbortVote {
            epoch: 30,
            position: 0,
            vshard: REJECTING,
            reason: AbortReason::PlanRejected,
        }));
        let verdicts = registry.drain_unproposed_verdicts();
        assert_eq!(
            verdicts,
            vec![(
                cluster_txn,
                nodedb_cluster::calvin::VerdictOutcome::Abort(AbortReason::PlanRejected)
            )]
        );
        registry.note_verdict(cluster_txn, verdicts[0].1);

        for (scheduler, vshard) in [(&mut rejecting, REJECTING), (&mut peer, PEER)] {
            scheduler.resume_on_verdict(txn_id, false);
            assert_eq!(
                scheduler.pending.get(&txn_id).map(|p| p.commit_state),
                Some(CommitState::AwaitingDrop),
                "vShard {vshard} leaves AwaitingVerdict for its drop"
            );
            assert!(!scheduler.applied.is_applied(30, 0));
            scheduler.finish_drop(txn_id, &staged_response(Status::Ok, None));
            assert!(!scheduler.pending.contains_key(&txn_id));
            assert!(scheduler.applied.is_applied(30, 0));
            registry.note_completion_ack(cluster_txn, vshard);
        }
        assert!(
            !rejecting.blocked.contains_key(&TxnId::new(31, 0)),
            "the released locks promote the waiting txn"
        );
        assert_eq!(
            outcome.await.expect("outcome fires"),
            AttemptOutcome::Aborted {
                reason: AbortReason::PlanRejected
            }
        );
    }

    /// A verdict stored before the rejection resumes the txn at once.
    #[tokio::test]
    async fn a_rejection_after_the_verdict_drops_at_once() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data, _proposer) = leading_scheduler(REJECTING, &registry);
        let txn_id = TxnId::new(32, 0);
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(32, 0),
            nodedb_cluster::calvin::VerdictOutcome::Abort(AbortReason::PlanRejected),
        );

        scheduler.dispatch_txn(undecodable_txn(32, 0), txn_id, txn_id);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingDrop)
        );
    }
}
