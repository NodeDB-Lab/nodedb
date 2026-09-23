// SPDX-License-Identifier: BUSL-1.1

//! Active (dependent-read) transaction dispatch: submits a
//! `CalvinExecuteActive` task once all passive results have landed.

use std::time::Instant;

use tracing::error;

use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::super::deferred::{DispatchOutcome, DispatchStep};
use super::super::scheduler::Scheduler;
use super::primary_write::{
    participant_change_sets, plans_have_primary_write, plans_have_returning,
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
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active plan decode failed; releasing locks"
                );
                self.on_unpending_txn_complete(txn_id, lock_owner);
                return;
            }
        };
        let has_non_derived_write = txn_has_non_derived_write(&plans);
        let mut plans =
            match self.local_calvin_plans(plans, txn.tx_class.database_id, epoch, position) {
                Ok(p) if !p.is_empty() => p,
                Ok(_) => {
                    // A dependent-read active txn dispatched here always carries a
                    // local write slice (the OLLP orchestrator only routes the write
                    // participant through this path). An empty local slice is a
                    // routing bug, not a read-only participant — surface it as a
                    // terminal routing failure rather than dispatching an
                    // active task with nothing to apply.
                    let e = crate::Error::Internal {
                        detail: format!(
                            "calvin active txn {epoch}/{position} homes no local write plans \
                         for vshard {}",
                            self.vshard_id
                        ),
                    };
                    error!(
                        vshard_id = self.vshard_id,
                        epoch,
                        position,
                        error = %e,
                        "calvin scheduler: active txn homes no local writes; releasing locks"
                    );
                    self.propose_routing_failure(epoch, position, txn_id, &e);
                    self.on_unpending_txn_complete(txn_id, lock_owner);
                    return;
                }
                Err(e) => {
                    error!(
                        vshard_id = self.vshard_id,
                        epoch,
                        position,
                        error = %e,
                        "calvin scheduler: active txn routing failed; releasing locks"
                    );
                    self.propose_routing_failure(epoch, position, txn_id, &e);
                    self.on_unpending_txn_complete(txn_id, lock_owner);
                    return;
                }
            };
        if !self.bind_local_identities(
            &mut plans,
            txn.tx_class.database_id,
            tenant_id,
            txn_id,
            lock_owner,
        ) {
            return;
        }
        let has_primary_write = plans_have_primary_write(&plans, has_non_derived_write);
        let has_returning = plans_have_returning(&plans);
        let change_sets = participant_change_sets(&plans, tenant_id, self.vshard_id);
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
            epoch,
            position,
            tenant_id,
            plans,
            injected_reads,
            epoch_system_ms: txn.epoch_system_ms,
            is_group_leader: self.is_group_leader(),
        });

        // Calvin allocates the CalvinApplied WAL LSN post-apply (in the
        // scheduler's response handler), so no committed LSN is known at
        // dispatch time to stamp here.
        let database_id = txn.tx_class.database_id;
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan, None);

        // The txn enters `pending` before the dispatch, so a stage refused at
        // capacity stays in flight with its locks until the re-send.
        self.pending.insert(
            txn_id,
            super::super::super::types::PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: Instant::now(),
                has_primary_write,
                has_returning,
                change_sets,
                // The dependent-read active path STAGES (leader-verify OLLP +
                // buffer, no base apply); its response drives the same
                // resolve → redo → flush as the static path, for
                // WAL-only-restart durability. `resolve_staged_commit` reads the
                // `read_set_valid: None` the active handler returns as "commit".
                commit_state: Some(super::super::super::types::CommitState::Staged),
                // Set only once the txn parks in `AwaitingVerdict`.
                verdict_deadline: None,
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
