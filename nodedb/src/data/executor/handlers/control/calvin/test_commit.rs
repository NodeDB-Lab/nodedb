// SPDX-License-Identifier: BUSL-1.1

//! Test driver for a committed Calvin transaction on one core: stage the
//! plans, resolve them into the redo record, and install the record as the
//! slice's stamped `ApplyTransactionRedo`.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinResolved, MetaOp, PhysicalPlan, RedoOrigin,
};

use crate::bridge::envelope::{Response, Status};
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, TenantId};

use super::shared::CalvinExecCtx;

impl CoreLoop {
    /// Commit `plans` as the Calvin transaction at `(epoch, 0)` and install
    /// its redo record at `lsn`, the path a committed Calvin slice takes on
    /// every replica of its vShard.
    ///
    /// Returns the first refusal: the stage, the resolve, or the install.
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
        };
        let staged =
            self.execute_calvin_execute_static(task, ctx, &TenantId::new(tid), plans, &[], &[]);
        if staged.status != Status::Ok {
            return staged;
        }
        let resolved = self.execute_calvin_resolve(task, epoch, 0);
        if resolved.status != Status::Ok {
            return resolved;
        }
        let answer = match zerompk::from_msgpack::<CalvinResolved>(resolved.payload.as_bytes()) {
            Ok(answer) => answer,
            Err(error) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("decode the resolved answer: {error}"),
                    },
                );
            }
        };
        let install = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: answer.redo,
            collections: written_collections(plans),
            sum_targets: redo_sum_targets(plans),
            origin: RedoOrigin::Commit,
            calvin: Some(CalvinInstall {
                epoch,
                position: 0,
                epoch_system_ms,
                reply: answer.reply,
                user_write: true,
            }),
        });
        let mut request = task.request.clone();
        request.wal_lsn = Some(Lsn::new(lsn));
        let install_task = ExecutionTask::new(request);
        self.execute_plan(&install_task, &install)
    }
}
