// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-order flushes.
//!
//! A vShard's transactions stage, vote, and resolve in any order: their
//! responses and verdicts arrive in any order. Their flushes run one at a
//! time, in `(epoch, position)` order. A committed transaction's flush
//! dispatches once no lower transaction of this vShard is unfinished.
//!
//! One core applies every request of a vShard, in dispatch order. So the core
//! installs committed transactions, and emits their change events, in
//! sequencer order on every replica. The commit tail, which publishes the
//! transaction's Control-Plane changes, runs in the same order. A Calvin
//! change feed therefore never delivers a lower position after a higher one.
//!
//! Every wait points at a lower transaction: the scheduler takes input, and
//! grants locks, in sequencer order. So the turn order adds no wait cycle.

use super::super::types::CommitState;
use super::commit_resolution_dispatch::CommitResolution;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::install_gate::FlushAdmission;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Queue the committed flush of `txn_id` for its turn, then dispatch it
    /// when its turn came.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn queue_flush(
        &mut self,
        txn_id: TxnId,
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingFlushTurn { redo_lsn };
        }
        self.pump_flush_turn();
    }

    /// Dispatch the flush of the lowest unfinished transaction when it waits
    /// for its turn.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn pump_flush_turn(&mut self) {
        let Some(lowest) = self.lowest_unfinished() else {
            return;
        };
        let resolve_turn = self
            .pending
            .get(&lowest)
            .is_some_and(|pending| pending.commit_state == CommitState::AwaitingResolveTurn);
        if resolve_turn {
            self.dispatch_resolve_turn(lowest);
            return;
        }
        let Some(redo_lsn) =
            self.pending
                .get(&lowest)
                .and_then(|pending| match pending.commit_state {
                    CommitState::AwaitingFlushTurn { redo_lsn } => Some(redo_lsn),
                    _ => None,
                })
        else {
            return;
        };
        // The flush holds its data group's apply gate shared until its
        // position is marked applied (see `super::install_gate`).
        let permit = match self.admit_flush() {
            FlushAdmission::Go(permit) => permit,
            FlushAdmission::Wait | FlushAdmission::Retired => return,
        };
        if let Some(pending) = self.pending.get_mut(&lowest) {
            pending.install_permit = permit;
        }
        // A flush refused at capacity is parked for re-send. The txn awaits its
        // flush response either way, so the state below is the same.
        if let DispatchOutcome::Failed(error) =
            self.dispatch_commit_resolution(lowest, CommitResolution::Flush { redo_lsn })
        {
            self.fail_dispatch_step(lowest, DispatchStep::Flush, error);
            return;
        }
        if let Some(pending) = self.pending.get_mut(&lowest) {
            pending.commit_state = CommitState::AwaitingResolve {
                committed: true,
                redo_lsn,
            };
        }
    }

    /// Dispatch the resolve of `txn_id`, whose turn came. Its flush then
    /// follows at once: it stays the lowest unfinished txn.
    fn dispatch_resolve_turn(&mut self, txn_id: TxnId) {
        if let DispatchOutcome::Failed(error) = self.dispatch_calvin_resolve(txn_id) {
            self.fail_dispatch_step(txn_id, DispatchStep::Resolve, error);
            return;
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingRedoResolve;
        }
    }

    /// The lowest transaction of this vShard that has not finished: in
    /// flight, blocked on a lock, or waiting at a dependent-read barrier.
    fn lowest_unfinished(&self) -> Option<TxnId> {
        let pending = self.pending.keys().next().copied();
        let barrier = self.dependent_barrier.keys().next().copied();
        let blocked = self
            .blocked
            .values()
            .map(|blocked| TxnId::new(blocked.txn.epoch, blocked.txn.position))
            .min();
        // A multi-part txn holding its locks while its parts arrive is
        // unfinished too: every later flush waits for it.
        let awaiting_parts = self.parts.lowest_awaiting();
        [pending, barrier, blocked, awaiting_parts]
            .into_iter()
            .flatten()
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_sequenced_txn, staged_pending,
    };
    use nodedb_cluster::calvin::CalvinCompletionRegistry;

    /// A higher committed txn waits for the lower one; the lower one's
    /// completion gives it its turn.
    #[tokio::test]
    async fn a_higher_flush_waits_for_every_lower_txn() {
        let low = TxnId::new(5, 0);
        let high = TxnId::new(5, 1);
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(7, CalvinCompletionRegistry::new_detached());
        for txn_id in [low, high] {
            scheduler.pending.insert(
                txn_id,
                staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
            );
        }

        scheduler.queue_flush(high, None);
        assert_eq!(
            scheduler.pending.get(&high).map(|p| p.commit_state),
            Some(CommitState::AwaitingFlushTurn { redo_lsn: None }),
            "the lower txn is unfinished, so the higher flush waits"
        );

        scheduler.pending.remove(&low);
        scheduler.pump_flush_turn();
        assert_eq!(
            scheduler.pending.get(&high).map(|p| p.commit_state),
            Some(CommitState::AwaitingResolve {
                committed: true,
                redo_lsn: None,
            }),
            "with the lower txn finished, the higher flush dispatches"
        );
    }

    /// A committed TRUNCATE of rows resolves only at its turn: while a lower
    /// txn of this vShard is unfinished, its resolve waits, so it reads every
    /// row sequenced before it.
    #[tokio::test]
    async fn a_truncate_resolves_only_at_its_turn() {
        let low = TxnId::new(5, 0);
        let truncate = TxnId::new(5, 1);
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(7, CalvinCompletionRegistry::new_detached());
        for txn_id in [low, truncate] {
            scheduler.pending.insert(
                txn_id,
                staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
            );
        }
        if let Some(pending) = scheduler.pending.get_mut(&truncate) {
            pending.flush_scope.resolve_at_turn = true;
            pending.commit_state = CommitState::AwaitingResolveTurn;
        }

        scheduler.pump_flush_turn();
        assert_eq!(
            scheduler.pending.get(&truncate).map(|p| p.commit_state),
            Some(CommitState::AwaitingResolveTurn),
            "the lower txn is unfinished, so the resolve waits"
        );

        scheduler.pending.remove(&low);
        scheduler.pump_flush_turn();
        assert_eq!(
            scheduler.pending.get(&truncate).map(|p| p.commit_state),
            Some(CommitState::AwaitingRedoResolve),
            "with the lower txn finished, the resolve dispatches"
        );
    }

    /// Every truncate of rows reads a whole collection at resolve; a point
    /// write does not. A TRUNCATE's edge share records a cut, which reads no
    /// stored edge, so it resolves without waiting for its turn.
    #[test]
    fn a_truncate_slice_resolves_at_its_turn() {
        use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
        let collection = nodedb_types::QualifiedCollection::from_stored("c".to_string());
        let truncate = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: collection.clone(),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let share = PhysicalPlan::Graph(GraphOp::TruncateEdges {
            collection,
            vshard: 3,
        });
        use crate::control::cluster::calvin::scheduler::driver::types::FlushScope;
        assert!(FlushScope::of_plans(std::slice::from_ref(&truncate)).resolve_at_turn);
        assert!(!FlushScope::of_plans(std::slice::from_ref(&share)).resolve_at_turn);
        assert!(
            FlushScope::of_plans(&[truncate, share]).resolve_at_turn,
            "the rows' vShard holds both, and its rows' truncate waits"
        );
        assert!(!FlushScope::of_plans(&[]).resolve_at_turn);
    }
}
