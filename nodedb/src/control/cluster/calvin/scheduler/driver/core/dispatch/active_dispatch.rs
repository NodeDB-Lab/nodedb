// SPDX-License-Identifier: BUSL-1.1

//! Active (dependent-read) transaction dispatch on the data-group leader:
//! submits a `CalvinExecuteActive` task once all passive results have landed.

use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::super::deferred::{DispatchOutcome, DispatchStep};
use super::super::scheduler::Scheduler;
use super::primary_write::{
    plans_have_primary_write, plans_have_returning, plans_raise_write_mark,
    txn_has_non_derived_write,
};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Dispatch an active dependent-read txn once all passive results are in.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_active_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
        injected_reads: std::collections::BTreeMap<
            nodedb_physical::physical_plan::meta::PassiveReadKeyId,
            nodedb_types::Value,
        >,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                self.reject_plan(txn, txn_id, lock_owner, &e);
                return;
            }
        };
        // An assembled multi-part txn carries only its local tasks. The
        // whole-transaction fact comes from its manifest.
        let has_non_derived_write = match &txn.tx_class.multi_part {
            Some(manifest) => manifest.user_write,
            None => txn_has_non_derived_write(&plans),
        };
        let mut plans =
            match self.local_calvin_plans(plans, txn.tx_class.database_id, epoch, position) {
                Ok(p) if !p.is_empty() => p,
                Ok(_) => {
                    // Only a vShard the write set names opens a barrier, so an
                    // active txn dispatched here carries a local write slice. An
                    // empty local slice means the plans and the write set
                    // disagree, so it rejects the plans rather than dispatching
                    // an active task with nothing to apply.
                    let e = crate::Error::Internal {
                        detail: format!(
                            "calvin active txn {epoch}/{position} homes no local write plans \
                         for vshard {}",
                            self.vshard_id
                        ),
                    };
                    self.reject_plan(txn, txn_id, lock_owner, &e);
                    return;
                }
                Err(e) => {
                    self.reject_plan(txn, txn_id, lock_owner, &e);
                    return;
                }
            };
        let Some(identities) =
            self.bind_local_identities(&mut plans, txn.tx_class.database_id, tenant_id, txn_id)
        else {
            return;
        };
        let has_primary_write = plans_have_primary_write(&plans, has_non_derived_write);
        let raises_write_mark = plans_raise_write_mark(&plans, has_primary_write);
        let has_returning = plans_have_returning(&plans);
        let mut scope = super::super::super::types::SliceScope::of_plans(&plans);
        scope.identities = identities;
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
            epoch,
            position,
            tenant_id,
            plans,
            injected_reads,
            epoch_system_ms: txn.epoch_system_ms,
        });

        // A stage writes no WAL record, so no committed LSN rides on it.
        let database_id = txn.tx_class.database_id;
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan);

        // The txn enters `pending` before the dispatch, so a stage refused at
        // capacity stays in flight with its locks until the re-send.
        // The leader checks the transaction's collection incarnations.
        let (superseded, gates) = self.check_incarnations(&txn.tx_class);
        self.pending.insert(
            txn_id,
            super::super::super::types::PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: Instant::now(),
                has_primary_write,
                raises_write_mark,
                has_returning,
                // The dependent-read active path STAGES (OLLP verify +
                // buffer, no base apply); its response drives the same
                // resolve and redo proposal as the static path.
                // `resolve_staged_commit` reads the `stage_vote` the active
                // handler sets.
                commit_state: super::super::super::types::CommitState::Staged,
                // The dispatch below records its request.
                awaiting: None,
                // Set only once the txn parks in `AwaitingVerdict`.
                verdict_deadline: None,
                stage_error: None,
                scope,
                // Set once a committed slice resolves its redo.
                redo: None,
                superseded,
                gates,
                ungated: false,
            },
        );

        if let DispatchOutcome::Failed(error) =
            self.dispatch_sequenced(txn_id, DispatchStep::StageActive, request)
        {
            self.fail_dispatch_step(txn_id, DispatchStep::StageActive, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_types::TenantId;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        await_data_plane_request, build_test_scheduler_with_data_side, fill_tenant_inflight,
        make_local_write_txn, release_filler, spawn_scheduler_loop, test_coll_vshard,
    };

    /// A refused active dispatch keeps the txn in flight: its position stays
    /// unapplied and its pending entry stays.
    #[tokio::test]
    async fn active_dispatch_refused_at_capacity_leaves_txn_unapplied() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let txn_id = TxnId::new(5, 0);

        scheduler.dispatch_active_txn(make_local_write_txn(5, 0), txn_id, txn_id, BTreeMap::new());

        assert!(
            !scheduler.applied.is_applied(5, 0),
            "a refused active dispatch must not mark the position applied"
        );
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "a refused active dispatch must keep the txn's pending entry"
        );
    }

    /// Once a Data Plane response frees tenant capacity, the refused active
    /// request reaches the Data Plane.
    #[tokio::test]
    async fn active_dispatch_refused_at_capacity_is_retried_after_capacity_frees() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let txn_id = TxnId::new(5, 0);

        scheduler.dispatch_active_txn(make_local_write_txn(5, 0), txn_id, txn_id, BTreeMap::new());
        let running = spawn_scheduler_loop(scheduler);
        release_filler(&shared, &mut data_side, fillers[0]);

        let arrived = await_data_plane_request(&mut data_side, |plan| {
            matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
                    epoch: 5,
                    position: 0,
                    ..
                })
            )
        })
        .await;
        running.stop().await;

        assert!(
            arrived,
            "the refused active request must reach the Data Plane once capacity frees"
        );
    }
}
