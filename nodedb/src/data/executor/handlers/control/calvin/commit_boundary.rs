// SPDX-License-Identifier: BUSL-1.1

//! The commit boundary of a staged Calvin slice.
//!
//! A committed Calvin transaction cannot abort, so a refusal at its install
//! halts the vShard. The stage therefore judges the slice with the check
//! the install runs, [`CoreLoop::check_commit_boundary`], on the writes the
//! slice's redo record will carry:
//!
//! * the writes come from the staged overlay through the resolve's own
//!   helpers (`staged_boundary_writes`);
//! * the origin, the sum targets and the roles come from the slice's plans
//!   and its request, as the install's do;
//! * retention is judged at the epoch instant, which the install judges at.
//!
//! The stage holds the slice's keys, granted in sequence order: every row,
//! UNIQUE value and collection the checks read. Every lower writer of them
//! installed before the stage ran, and no higher one installs before this
//! slice. So the install reads the state the stage read, and reaches its
//! verdict. A refusal here is an abort vote with the code the install gives.

use nodedb_physical::physical_plan::{PhysicalPlan, RedoOrigin};

use crate::bridge::envelope::ErrorCode;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::CommitBoundary;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Refuse the slice of `plans` staged under `txn_id` when its install
    /// would refuse its redo record. Reads only.
    pub(super) fn check_calvin_commit_boundary(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tenant_id: TenantId,
        plans: &[PhysicalPlan],
    ) -> Result<(), ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        let tid = tenant_id.as_u64();
        let writes = self.staged_boundary_writes(task, tid, txn_id, plans)?;
        let documents = self.without_skipped_clone_copies(database_id, tid, writes.documents)?;
        let sum_targets = redo_sum_targets(plans);
        self.check_commit_boundary(&CommitBoundary {
            database_id,
            tid,
            documents: &documents,
            kv: &writes.kv,
            sum_targets: &sum_targets,
            origin: RedoOrigin::of_plans(plans),
            user_roles: &task.request.user_roles,
            judged_at_ms: self.enforcement_instant_ms(),
        })
        .map_err(ErrorCode::from)
    }
}
