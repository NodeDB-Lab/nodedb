// SPDX-License-Identifier: BUSL-1.1

//! `CalvinReadResult` handling and dependent-read barrier timeout sweeps.

use tracing::warn;

use super::super::barrier::ReadResultEvent;
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
                // No barrier for this txn — can have already timed out or
                // been dispatched. Log and ignore.
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

        let injected_reads = barrier.assemble_injected_reads();
        let lock_owner = barrier.lock_owner;
        let txn = barrier.txn;

        self.dispatch_active_txn(txn, txn_id, lock_owner, injected_reads);
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
        build_test_scheduler_with_data_side, make_sequenced_txn,
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
}
