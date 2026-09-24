// SPDX-License-Identifier: BUSL-1.1

//! Discard a staged Calvin transaction.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Discard a staged Calvin transaction.
    ///
    /// Removes the plans staged under `(epoch, position, vshard)` from the
    /// commit-pending buffer and fires nothing — no base mutation, no side
    /// effects. An
    /// absent key (already flushed or dropped) is an idempotent no-op.
    pub(in crate::data::executor) fn execute_calvin_drop(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        let existed = self
            .calvin
            .commit_pending
            .remove(&(epoch, position, vshard_id))
            .is_some();
        // Discard the synthetic overlay entry alongside the raw plan buffer;
        // idempotent no-op if it was never staged or already removed.
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        // Writes waiting on the rows this transaction owned run next.
        self.calvin
            .fence
            .note_resolved((epoch, position, vshard_id), None);
        debug!(
            core = self.core_id,
            epoch, position, vshard_id, existed, "calvin drop: discarding staged commit"
        );
        self.response_ok(task)
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::TenantId;

    use super::super::shared::CalvinExecCtx;
    use super::super::shared::test_support::{make_task, point_insert_plan};
    use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;

    #[test]
    fn calvin_drop_discards_synthetic_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);
        assert_eq!(resp.status, Status::Ok);

        let vshard_id = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(core.txn_overlays.contains_key(&synthetic));

        let drop_resp = core.execute_calvin_drop(&task, 1, 0);
        assert_eq!(drop_resp.status, Status::Ok);

        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "drop must discard the synthetic overlay entry alongside commit_pending"
        );
    }
}
