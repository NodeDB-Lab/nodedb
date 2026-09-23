// SPDX-License-Identifier: BUSL-1.1

//! Array CRDT variants — handled on the Control Plane, bypass the Data Plane.

use std::sync::Arc;

use crate::control::array_sync::ArrayOpTarget;
use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArraySchemaPayload, apply_array_op, apply_array_schema,
};
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;

/// Apply a committed `ReplicatedWrite::ArrayOp` entry.
///
/// Advances the durable prefix only when the op durably applied — same
/// safe-watermark rule as the Data Plane write path, and the same funnel: the
/// op path submits through `submit_write`, so its redo is fsynced before it
/// reports success. A failure breaks the prefix: the entry must stay
/// replayable.
pub(super) async fn apply_array_op_entry(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    target: ArrayOpTarget<'_>,
    op_bytes: &[u8],
    provenance: Option<&[u8]>,
) -> bool {
    apply_array_op(state, tracker, pos, target, op_bytes, provenance).await
}

/// Apply a committed `ReplicatedWrite::ArraySchema` entry.
///
/// Advances the durable prefix only when the schema snapshot durably
/// imported.
///
/// This is the one applied branch that mints no WAL redo record, and it needs
/// none: its entire effect is two fsync-committed redb transactions — the
/// schema registry's snapshot row and the array catalog's entry — both
/// written before it reports success. The floor's invariant ("this entry's
/// state survives a restart, so Raft need not redeliver it") is therefore
/// already met by the registries themselves. The cell paths have no such
/// durable store behind them: their state lives in Data-Plane memtables and
/// exists on disk only as the redo record the funnel appends, which is why
/// they must route through `submit_write`.
pub(super) fn apply_array_schema_entry(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    payload: ArraySchemaPayload<'_>,
) -> bool {
    apply_array_schema(state, tracker, pos, payload)
}
