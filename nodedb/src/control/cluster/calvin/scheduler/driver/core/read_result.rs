// SPDX-License-Identifier: BUSL-1.1

//! `CalvinReadResult` handling and dependent-read barrier timeout sweeps.
//!
//! A read result arrives once, from the data-group log. A dependent txn this
//! scheduler holds with no barrier keeps the results it received: a
//! follower's held txn, or a leader's txn whose barrier timed out. A later
//! barrier of the txn, after a promotion or a restage, starts from them.

use tracing::warn;

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent, ReceivedReads};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Handle a `ReadResultEvent` from the per-vshard Raft apply loop.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_read_result(
        &mut self,
        event: ReadResultEvent,
    ) {
        let txn_id = TxnId::new(event.epoch, event.position);

        let barrier = match self.dependent_barrier.get_mut(&txn_id) {
            Some(b) => b,
            None => {
                if self.holds_dependent(txn_id) {
                    self.hold_reads(
                        txn_id,
                        ReceivedReads::from([(event.passive_vshard, event.values)]),
                    );
                    return;
                }
                // Neither a barrier nor a held txn: the txn finished, or its
                // input has not been granted here. Log and ignore.
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = event.epoch,
                    position = event.position,
                    passive_vshard = event.passive_vshard,
                    "calvin: received CalvinReadResult for unknown txn; ignoring"
                );
                return;
            }
        };

        barrier.waiting_for.remove(&event.passive_vshard);
        barrier.received.insert(event.passive_vshard, event.values);

        if !barrier.is_complete() {
            return;
        }

        // All passive results in — remove barrier and dispatch active.
        let Some(barrier) = self.dependent_barrier.remove(&txn_id) else {
            return;
        };
        self.dispatch_complete_barrier(txn_id, barrier);
    }

    /// Open `barrier` for `txn_id`, starting from the reads the txn holds.
    /// A barrier those reads complete dispatches at once.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn open_barrier(
        &mut self,
        txn_id: TxnId,
        mut barrier: PendingDependentBarrier,
    ) {
        if let Some(held) = self.held_reads.remove(&txn_id) {
            for (passive_vshard, values) in held {
                barrier.waiting_for.remove(&passive_vshard);
                barrier.received.insert(passive_vshard, values);
            }
        }
        if barrier.is_complete() {
            self.dispatch_complete_barrier(txn_id, barrier);
        } else {
            self.dependent_barrier.insert(txn_id, barrier);
        }
    }

    /// Dispatch the active stage of a barrier every passive vShard answered.
    fn dispatch_complete_barrier(&mut self, txn_id: TxnId, barrier: PendingDependentBarrier) {
        let injected_reads = barrier.assemble_injected_reads();
        self.dispatch_active_txn(barrier.txn, txn_id, barrier.lock_owner, injected_reads);
    }

    /// Keep `reads` for `txn_id`, a dependent txn in `pending`. Reads for a
    /// txn that left `pending` are dropped: it finished.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn hold_reads(
        &mut self,
        txn_id: TxnId,
        reads: ReceivedReads,
    ) {
        if reads.is_empty() || !self.holds_dependent(txn_id) {
            return;
        }
        self.held_reads.entry(txn_id).or_default().extend(reads);
    }

    /// Whether `txn_id` is a dependent txn in `pending`.
    fn holds_dependent(&self, txn_id: TxnId) -> bool {
        self.pending
            .get(&txn_id)
            .is_some_and(|pending| pending.txn.tx_class.dependent_reads.is_some())
    }

    /// Check all pending dependent barriers for timeout.
    ///
    /// A timed-out txn never staged here. It parks on the commit barrier
    /// with a `ParticipantError` abort vote and keeps its locks. Its position
    /// is marked applied only once the abort verdict drops it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_dependent_barrier_timeouts(
        &mut self,
    ) {
        // Collect timed-out txn ids first (avoid borrowing issues).
        let timed_out: Vec<TxnId> = self
            .dependent_barrier
            .iter()
            .filter(|(_, b)| b.is_timed_out())
            .map(|(id, _)| *id)
            .collect();

        for txn_id in timed_out {
            if let Some(barrier) = self.dependent_barrier.remove(&txn_id) {
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    still_waiting = ?barrier.waiting_for,
                    "calvin: dependent-read barrier timed out; voting abort"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::PASSIVE_PARTICIPANT_TIMEOUT,
                );
                let stage_error = format!(
                    "dependent-read barrier timed out waiting for passive vShards {:?}",
                    barrier.waiting_for
                );
                self.park_unstaged(
                    barrier.txn,
                    txn_id,
                    barrier.lock_owner,
                    nodedb_cluster::calvin::AbortReason::ParticipantError,
                    stage_error,
                );
                // A restage under a COMMIT verdict opens a barrier again. It
                // starts from what this one received.
                self.hold_reads(txn_id, barrier.received);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::{Duration, Instant};

    use nodedb_cluster::calvin::{
        AbortReason, CalvinCompletionRegistry, ParticipantVote, SequencerEntry, VerdictOutcome,
    };

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::barrier::PendingDependentBarrier;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, make_sequenced_txn,
        test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    const VSHARD: u32 = 7;

    /// A timed-out barrier votes abort and parks. Its vote completes the
    /// tally into an abort verdict, and the position stays unapplied until
    /// that verdict drops the txn.
    #[tokio::test]
    async fn a_timed_out_barrier_votes_a_complete_abort_tally() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(VSHARD, std::sync::Arc::clone(&registry));
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        let txn_id = TxnId::new(40, 0);
        let cluster_txn = nodedb_cluster::calvin::TxnId::new(40, 0);
        registry.seed_expected(cluster_txn, 1);
        scheduler.dependent_barrier.insert(
            txn_id,
            PendingDependentBarrier {
                txn: make_sequenced_txn(40, 0),
                lock_owner: txn_id,
                waiting_for: BTreeSet::from([9]),
                received: BTreeMap::new(),
                // no-determinism: test-only deadline already passed.
                timeout_at: Instant::now() - Duration::from_millis(1),
            },
        );

        scheduler.check_dependent_barrier_timeouts();
        assert!(scheduler.dependent_barrier.is_empty());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        assert!(!scheduler.applied.is_applied(40, 0));
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::AbortVote {
                epoch: 40,
                position: 0,
                vshard: VSHARD,
                reason: AbortReason::ParticipantError,
            }]
        );

        registry.note_vote(
            cluster_txn,
            VSHARD,
            ParticipantVote::Abort(AbortReason::ParticipantError),
        );
        assert_eq!(
            registry.drain_unproposed_verdicts(),
            vec![(
                cluster_txn,
                VerdictOutcome::Abort(AbortReason::ParticipantError)
            )]
        );
        registry.note_verdict(
            cluster_txn,
            VerdictOutcome::Abort(AbortReason::ParticipantError),
        );
        scheduler.resume_on_verdict(txn_id, false);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingDrop)
        );
        assert!(!scheduler.applied.is_applied(40, 0));
    }

    /// A barrier that timed out keeps what it received, and a read result
    /// that lands after the timeout is kept too. Under a COMMIT verdict the
    /// restage opens a barrier from those reads. They complete it, so the
    /// active stage dispatches at once.
    #[tokio::test]
    async fn a_restaged_barrier_starts_from_the_reads_it_received() {
        use nodedb_cluster::calvin::types::DependentReadSpec;
        use nodedb_physical::physical_plan::PhysicalPlan;
        use nodedb_physical::physical_plan::meta::MetaOp;

        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            std::sync::Arc::clone(&registry),
        );
        lead_data_group(&mut scheduler);
        let txn_id = TxnId::new(41, 0);
        let mut txn = make_local_write_txn(41, 0);
        txn.tx_class.dependent_reads = Some(DependentReadSpec {
            passive_reads: BTreeMap::from([(9, Vec::new()), (10, Vec::new())]),
        });
        scheduler.dependent_barrier.insert(
            txn_id,
            PendingDependentBarrier {
                txn,
                lock_owner: txn_id,
                waiting_for: BTreeSet::from([10]),
                received: BTreeMap::from([(9, Vec::new())]),
                // no-determinism: test-only deadline already passed.
                timeout_at: Instant::now() - Duration::from_millis(1),
            },
        );

        scheduler.check_dependent_barrier_timeouts();
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        scheduler.handle_read_result(ReadResultEvent {
            epoch: 41,
            position: 0,
            passive_vshard: 10,
            tenant_id: crate::types::TenantId::new(1),
            values: Vec::new(),
        });
        assert_eq!(
            scheduler
                .held_reads
                .get(&txn_id)
                .map(|held| held.keys().copied().collect::<Vec<_>>()),
            Some(vec![9, 10])
        );

        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(41, 0),
            VerdictOutcome::Commit,
        );
        scheduler.resume_on_verdict(txn_id, true);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );
        if let Some(restage) = scheduler.restages.get_mut(&txn_id) {
            // no-determinism: test-only backoff end in the past.
            restage.due = Some(Instant::now() - Duration::from_millis(1));
        }
        scheduler.restage_due();

        let mut staged_active = false;
        while let Ok(request) = data_side.request_rx.try_pop() {
            staged_active |= matches!(
                request.inner.plan,
                PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
                    epoch: 41,
                    position: 0,
                    ..
                })
            );
        }
        assert!(staged_active, "the restaged barrier completes at once");
        assert!(scheduler.dependent_barrier.is_empty());
        assert!(scheduler.held_reads.is_empty());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
    }
}
