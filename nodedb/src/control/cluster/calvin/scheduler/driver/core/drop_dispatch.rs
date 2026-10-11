// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of a `CalvinDrop`: discard what a txn staged on its core.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::commit_redo::missing_pending_error;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Dispatch the drop of `txn_id`'s staged state as `step`: a
    /// [`DispatchStep::Drop`] the txn completes on, or a
    /// [`DispatchStep::Discard`] nothing waits for.
    ///
    /// A drop writes nothing, so no WAL LSN rides on it. A capacity refusal
    /// returns [`DispatchOutcome::Deferred`]: the drop is parked for re-send
    /// and the txn stays in flight. A txn with no `pending` entry returns
    /// [`DispatchOutcome::Failed`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_drop(
        &mut self,
        txn_id: TxnId,
        step: DispatchStep,
    ) -> DispatchOutcome {
        let Some(pending) = self.pending.get(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let plan = PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: txn_id.epoch,
            position: txn_id.position,
        });
        let request_id = self.next_request_id();
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan);
        self.dispatch_sequenced(txn_id, step, request)
    }
}
