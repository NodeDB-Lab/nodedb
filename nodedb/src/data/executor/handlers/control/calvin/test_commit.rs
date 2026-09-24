// SPDX-License-Identifier: BUSL-1.1

//! Test driver for a committed Calvin transaction on one core: stage the
//! plans, resolve them into the redo record, and flush it.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::{Response, Status};
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, TenantId};

use super::flush::CalvinFlushRedo;
use super::shared::CalvinExecCtx;

impl CoreLoop {
    /// Commit `plans` as the Calvin transaction at `(epoch, 0)` and flush its
    /// redo record at `lsn`, the path a committed Calvin transaction takes on
    /// its participant core.
    ///
    /// Returns the first refusal: the stage, the resolve, or the flush.
    pub(in crate::data::executor) fn calvin_commit_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        epoch: u64,
        lsn: u64,
    ) -> Response {
        self.calvin_commit_at_for_test(task, tid, plans, epoch, 0, lsn)
    }

    /// Commit `plans` like [`Self::calvin_commit_for_test`], with the epoch
    /// instant `epoch_system_ms`.
    pub(in crate::data::executor) fn calvin_commit_at_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        epoch: u64,
        epoch_system_ms: i64,
        lsn: u64,
    ) -> Response {
        let ctx = CalvinExecCtx {
            epoch,
            position: 0,
            epoch_system_ms,
            is_group_leader: true,
        };
        let staged = self.execute_calvin_execute_static(task, ctx, &TenantId::new(tid), plans, &[]);
        if staged.status != Status::Ok {
            return staged;
        }
        let resolved = self.execute_calvin_resolve(task, epoch, 0);
        if resolved.status != Status::Ok {
            return resolved;
        }
        let redo = resolved.payload.as_bytes().to_vec();
        let mut request = task.request.clone();
        request.wal_lsn = Some(Lsn::new(lsn));
        let flush_task = ExecutionTask::new(request);
        let collections = written_collections(plans);
        let sum_targets = redo_sum_targets(plans);
        self.execute_calvin_flush(
            &flush_task,
            CalvinFlushRedo {
                epoch,
                position: 0,
                redo: &redo,
                collections: &collections,
                sum_targets: &sum_targets,
            },
        )
    }
}
