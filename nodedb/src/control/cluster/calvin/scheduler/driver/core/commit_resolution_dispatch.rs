// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of staged Calvin flush/drop resolution operations.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::commit_redo::missing_pending_error;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::Lsn;

/// How a staged transaction resolves on this vShard.
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum CommitResolution {
    /// Install the committed redo record the scheduler appended at
    /// `redo_lsn`. The flush scope holds its bytes. Both are empty when the
    /// transaction wrote nothing on this vShard.
    Flush { redo_lsn: Option<Lsn> },
    /// Discard the staged state under an abort verdict.
    Drop,
}

impl Scheduler {
    /// Dispatch a flush or drop of a staged transaction's commit-pending buffer.
    ///
    /// A flush carries the redo record, the collections the local plans
    /// write, and their materialized-sum targets, so the Data Plane installs
    /// the record the way every committed transaction installs.
    ///
    /// A capacity refusal returns [`DispatchOutcome::Deferred`]: the flush or
    /// drop is parked for re-send and the txn stays in flight. A txn with no
    /// `pending` entry returns [`DispatchOutcome::Failed`]. The flush takes its
    /// collections and sum targets from the scope derived at stage time.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_commit_resolution(
        &mut self,
        txn_id: TxnId,
        resolution: CommitResolution,
    ) -> DispatchOutcome {
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;
        let (plan, step, wal_lsn) = match resolution {
            CommitResolution::Flush { redo_lsn } => {
                pending.flush_scope.sends = pending.flush_scope.sends.saturating_add(1);
                let scope = &pending.flush_scope;
                (
                    PhysicalPlan::Meta(MetaOp::CalvinFlush {
                        epoch,
                        position,
                        redo: scope.redo.clone(),
                        collections: scope.collections.clone(),
                        sum_targets: scope.sum_targets.clone(),
                    }),
                    DispatchStep::Flush,
                    redo_lsn,
                )
            }
            CommitResolution::Drop => (
                PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position }),
                DispatchStep::Drop,
                None,
            ),
        };
        let request_id = self.next_request_id();
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan, wal_lsn);
        self.dispatch_sequenced(txn_id, step, request)
    }
}
