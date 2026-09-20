// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of staged Calvin flush/drop resolution operations.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Dispatch a flush or drop of a staged transaction's commit-pending buffer.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_commit_resolution(
        &mut self,
        txn_id: TxnId,
        committed: bool,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> bool {
        let Some(pending) = self.pending.get(&txn_id) else {
            return false;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;
        let plan = if committed {
            PhysicalPlan::Meta(MetaOp::CalvinFlush { epoch, position })
        } else {
            PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position })
        };
        let request_id = self.next_request_id();
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan, wal_lsn);
        let resp_rx = self.shared.tracker.register(request_id);
        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut dispatcher) => dispatcher.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if let Err(error) = dispatch_result {
            self.shared.tracker.cancel(&request_id);
            if self.note_dispatch_busy(&error, epoch) {
                // Retryable capacity condition (issue 352): paced by the drain.
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    committed,
                    %error,
                    "calvin: commit resolution dispatch deferred (capacity busy)"
                );
                return false;
            }
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                committed,
                %error,
                "calvin: commit resolution dispatch failed"
            );
            return false;
        }
        self.spawn_response_bridge(txn_id, request_id, resp_rx);
        true
    }
}
