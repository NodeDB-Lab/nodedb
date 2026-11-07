// SPDX-License-Identifier: BUSL-1.1

//! The meta operations that change stored versions in a way no write note
//! describes, and the newest-ordinal cache reset and history recharge they
//! take.

use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Whether `op` can remove or replace stored versions wholesale: a snapshot
/// install, a purge, a drop, a retention pass, or a conversion. The newest
/// ordinal of a slice can then fall, so the cache forgets every value.
fn replaces_stored_versions(op: &MetaOp) -> bool {
    matches!(
        op,
        MetaOp::RestoreTenantSnapshot { .. }
            | MetaOp::PurgeDepartedVShards { .. }
            | MetaOp::PurgeVersionsBelow { .. }
            | MetaOp::PurgeTenant { .. }
            | MetaOp::UnregisterCollection { .. }
            | MetaOp::UnregisterMaterializedView { .. }
            | MetaOp::ConvertCollection { .. }
            | MetaOp::EnforceTimeseriesRetention { .. }
            | MetaOp::ApplyContinuousAggRetention { .. }
            | MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. }
            | MetaOp::TemporalPurgeArray { .. }
            | MetaOp::Compact
    )
}

impl CoreLoop {
    /// Dispatch a meta operation. One that replaces stored versions resets
    /// the newest-ordinal cache, whether or not it finished: a partial purge
    /// removed versions too.
    pub(super) fn dispatch_meta(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &MetaOp,
    ) -> Response {
        let response = self.dispatch_meta_op(task, tid, op);
        if replaces_stored_versions(op) {
            self.forget_newest_ordinals();
            // A snapshot install, a tenant purge, or a departure moved
            // stores of any tenant wholesale, with no per-write delta: every
            // standing history charge follows. Any other op recharges its
            // own tenant as a write (`execute`), and a history purge
            // recharges the stores it purged, batch by batch.
            if matches!(
                op,
                MetaOp::RestoreTenantSnapshot { .. }
                    | MetaOp::PurgeTenant { .. }
                    | MetaOp::PurgeDepartedVShards { .. }
            ) {
                self.recharge_all_history();
            }
        }
        response
    }
}
