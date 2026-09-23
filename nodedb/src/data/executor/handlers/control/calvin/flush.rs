// SPDX-License-Identifier: BUSL-1.1

//! Flush a staged Calvin transaction to base storage.

use tracing::{debug, info_span};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
use crate::data::executor::task::ExecutionTask;

use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;

impl CoreLoop {
    /// Flush a staged Calvin transaction to base storage.
    ///
    /// Pops the plans staged by [`CoreLoop::execute_calvin_execute_static`]
    /// under `(epoch, position)` and replays them through the durable apply
    /// funnel (`execute_transaction_batch`) — the same funnel the single-shard
    /// commit and recovery use — so base mutation, side effects, and
    /// version recording all run exactly once here. The deterministic epoch
    /// time anchor and leadership scope captured at stage time are restored
    /// around the apply so time-dependent writes stay identical across
    /// replicas. An absent key (already flushed or dropped, e.g. a duplicate
    /// dispatch) is an idempotent no-op returning `Ok`.
    pub(in crate::data::executor) fn execute_calvin_flush(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        // Capture the resolve-time bitemporal stamps (if `CalvinResolve` staged
        // them into the synthetic overlay) BEFORE the overlay is dropped, so the
        // base install below reuses the exact stamp the redo carries rather than
        // minting a fresh one. Empty when this transaction wrote no bitemporal
        // document rows or resolve never ran.
        let synthetic_txn_id = calvin_synthetic_txn_id(epoch, position, vshard_id).ok();
        let bitemporal_stamps: Vec<(u32, BitemporalStamp)> = synthetic_txn_id
            .and_then(|synthetic| self.txn_overlays.get(&synthetic))
            .map(|overlay| overlay.all_bitemporal_stamps().collect())
            .unwrap_or_default();
        let graph_system_from = synthetic_txn_id
            .and_then(|synthetic| self.graph_txn_overlays.get(&synthetic))
            .and_then(|overlay| overlay.resolved_system_from());
        // Drop the synthetic overlay entry staged by
        // `execute_calvin_execute_static` unconditionally, before the apply
        // below: idempotent no-op on a duplicate dispatch.
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        let Some(pending) = self.commit_pending.remove(&(epoch, position, vshard_id)) else {
            debug!(
                core = self.core_id,
                epoch, position, vshard_id, "calvin flush: no staged commit (already resolved)"
            );
            return self.response_ok(task);
        };
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = pending.tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();
        const NANOS_PER_MS: i64 = 1_000_000;
        self.hlc
            .update_from_remote(pending.epoch_system_ms.saturating_mul(NANOS_PER_MS));
        self.epoch_system_ms = Some(pending.epoch_system_ms);
        // Scope OLLP verification to this participant's staged leadership for the
        // batch, then restore the resting (authoritative) state.
        let prev_group_leader = self.ollp_is_group_leader;
        self.ollp_is_group_leader = pending.is_group_leader;
        // Install the captured resolve-time stamps into apply scratch; the
        // batch consumes them for its bitemporal document puts and clears the
        // scratch when it returns. `txn_id = None`: the synthetic overlay was
        // already dropped above, so the stamps are threaded in directly here.
        for (surrogate, stamp) in bitemporal_stamps {
            self.active_bitemporal_stamps.insert(surrogate, stamp);
        }
        self.active_graph_system_from = graph_system_from;
        // The read-set was already validated at stage time and drives the
        // flush/drop decision; the replay itself carries no read-set to re-check.
        // Scope the flush key so `record_batch_index_write_values` stages this
        // batch's index tuples (the apply carries `wal_lsn: None`); the post-apply
        // `RecordCalvinWriteVersions` op drains them at the replicated applied LSN.
        self.calvin_flush_key = Some((epoch, position, vshard_id));
        let result = self.execute_transaction_batch(
            task,
            pending.tenant_id.as_u64(),
            &pending.plans,
            &[],
            None,
        );
        self.calvin_flush_key = None;
        self.ollp_is_group_leader = prev_group_leader;
        self.epoch_system_ms = None;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::TenantId;

    use super::super::shared::CalvinExecCtx;
    use super::super::shared::test_support::{make_task, point_insert_plan};

    #[test]
    fn calvin_flush_drops_synthetic_overlay() {
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

        let flush_resp = core.execute_calvin_flush(&task, 1, 0);
        assert_eq!(flush_resp.status, Status::Ok);

        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "flush must drop the synthetic overlay entry alongside commit_pending"
        );
    }
}
