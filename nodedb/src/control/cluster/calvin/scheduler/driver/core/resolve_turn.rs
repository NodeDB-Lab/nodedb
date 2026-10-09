// SPDX-License-Identifier: BUSL-1.1

//! The resolve turn of a slice that reads a whole collection at resolve.
//!
//! A committed slice resolves as soon as its verdict arrives, and its redo
//! is proposed at once: the log orders it against every other entry of the
//! group, and conflicting txns never overlap under the lock table. A
//! TRUNCATE of rows is the exception. Its resolve reads every row of the
//! collection, so it waits until every lower txn of this vShard finished:
//! their rows are then installed, and no higher txn's rows are.
//!
//! Every wait points at a lower transaction: the scheduler takes input, and
//! grants locks, in sequencer order. So the turn order adds no wait cycle.

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Resolve the lowest unfinished transaction when it waits for its turn.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn pump_resolve_turn(
        &mut self,
    ) {
        let Some(lowest) = self.lowest_unfinished() else {
            return;
        };
        let at_turn = self
            .pending
            .get(&lowest)
            .is_some_and(|pending| pending.commit_state == CommitState::AwaitingResolveTurn);
        if at_turn {
            self.start_resolve(lowest);
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
        // unfinished too: every later whole-collection resolve waits for it.
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
    use crate::control::cluster::calvin::scheduler::driver::types::SliceScope;
    use nodedb_cluster::calvin::CalvinCompletionRegistry;

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
            pending.scope.resolve_at_turn = true;
            pending.commit_state = CommitState::AwaitingResolveTurn;
        }

        scheduler.pump_resolve_turn();
        assert_eq!(
            scheduler.pending.get(&truncate).map(|p| p.commit_state),
            Some(CommitState::AwaitingResolveTurn),
            "the lower txn is unfinished, so the resolve waits"
        );

        scheduler.pending.remove(&low);
        scheduler.pump_resolve_turn();
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
        assert!(SliceScope::of_plans(std::slice::from_ref(&truncate)).resolve_at_turn);
        assert!(!SliceScope::of_plans(std::slice::from_ref(&share)).resolve_at_turn);
        assert!(
            SliceScope::of_plans(&[truncate, share]).resolve_at_turn,
            "the rows' vShard holds both, and its rows' truncate waits"
        );
        assert!(!SliceScope::of_plans(&[]).resolve_at_turn);
    }
}
