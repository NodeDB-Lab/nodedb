// SPDX-License-Identifier: BUSL-1.1

//! Active (dependent-read) txn dispatch to the Data Plane executor.

use std::time::Instant;

use tracing::{debug, error};

use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::dispatch::{participant_change_sets, plans_have_primary_write, plans_have_returning};
use super::scheduler::Scheduler;
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

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active plan decode failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, txn.tx_class.database_id, epoch, position)
        {
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
                self.on_txn_complete(txn_id);
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
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let has_primary_write = plans_have_primary_write(&plans);
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
        let request =
            self.build_exempt_request(request_id, tenant_id, txn.tx_class.database_id, plan, None);

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            if self.note_dispatch_busy(&e, epoch) {
                debug!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active dispatch deferred (capacity busy)"
                );
                self.on_txn_complete(txn_id);
                return;
            }
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: active dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                has_primary_write,
                has_returning,
                change_sets,
                // The dependent-read active path now STAGES (leader-verify OLLP
                // + buffer, no base apply); its response drives the same
                // resolve → redo → flush as the static path, restoring
                // WAL-only-restart durability. `resolve_staged_commit` reads the
                // `read_set_valid: None` the active handler returns as "commit".
                commit_state: Some(super::super::types::CommitState::Staged),
                // Set only once the txn parks in `AwaitingVerdict`.
                verdict_deadline: None,
            },
        );
    }
}
