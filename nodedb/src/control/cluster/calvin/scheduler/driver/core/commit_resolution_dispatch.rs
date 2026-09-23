// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of staged Calvin flush/drop resolution operations.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::commit_redo::missing_pending_error;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Dispatch a flush or drop of a staged transaction's commit-pending buffer.
    ///
    /// A capacity refusal returns [`DispatchOutcome::Deferred`]: the flush or
    /// drop is parked for re-send and the txn stays in flight. A txn with no
    /// `pending` entry returns [`DispatchOutcome::Failed`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_commit_resolution(
        &mut self,
        txn_id: TxnId,
        committed: bool,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> DispatchOutcome {
        let Some(pending) = self.pending.get(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;
        let (plan, step) = if committed {
            (
                PhysicalPlan::Meta(MetaOp::CalvinFlush { epoch, position }),
                DispatchStep::Flush,
            )
        } else {
            (
                PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position }),
                DispatchStep::Drop,
            )
        };
        let request_id = self.next_request_id();
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan, wal_lsn);
        self.dispatch_sequenced(txn_id, step, request)
    }
}
