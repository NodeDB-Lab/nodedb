// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed Raft-native array cell write (`ArrayCellPut` /
//! `ArrayCellDelete`) on the local node.
//!
//! This is the apply half of the cluster SQL DML array path: the owner
//! proposed `ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete` to the shard's
//! data Raft group; every replica (the proposer included) lands here after
//! commit. Unlike the plain committed-write path, an array Put/Delete requires
//! the Data Plane to have the array OPEN first — so this runs the array-open
//! bootstrap in [`super::common`] and THEN hands the write to the shared
//! Control-Plane write funnel, which mints this replica's redo record before the
//! enqueue and fsyncs it before the apply is reported durable. The bootstrap is
//! a prerequisite step, not a reason to bypass the funnel: an entry applied
//! without a redo record has no durability at all once the applied floor
//! advances past it and Raft stops redelivering it.
//!
//! Distinct from [`super::op::apply_array_op`], which applies a single Lite-sync
//! CRDT op through the array-sync op-log / HLC dedup. Here idempotency is the
//! Raft log's exactly-once ordering plus the array engine's coord-keyed
//! overwrite semantics (re-applying a Put re-writes the identical cell; a
//! Delete of an absent coord is a no-op), so no op-log entry is recorded.

use std::sync::Arc;

use tracing::warn;

use super::common::{AppliedPosition, ArrayWriteSubmit, ensure_array_open, submit_array_write};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::distributed_applier::ProposeTracker;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

/// Where a committed array cell write lands, and the instant its proposer
/// resolved for it.
///
/// `vshard` is the owning shard's vShard, carried verbatim in the committed
/// `ReplicatedEntry` header (set by the proposer from the array's Hilbert-tile
/// placement) — the same group every replica of this shard applies from.
/// `database_id` is likewise read off the entry, so this replica's redo record
/// is appended under the scope the write was proposed in.
pub(crate) struct ArrayCellTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard: VShardId,
    /// The wall-clock instant the proposing node resolved for this entry,
    /// forwarded so this replica's redo record carries it verbatim rather than
    /// re-reading a clock that has since moved.
    pub resolved_now_ms: Option<u64>,
}

/// Apply a decoded array cell write plan (`PhysicalPlan::Array(Put | Delete)`)
/// on the local node.
///
/// Returns `true` when the write durably applied, `false` on any
/// open/dispatch/apply failure. The caller gates the durable applied floor (and
/// with it Raft log compaction) on this.
pub(crate) async fn apply_array_cell_write(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    target: ArrayCellTarget,
    plan: PhysicalPlan,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;
    let ArrayCellTarget {
        tenant_id,
        database_id,
        vshard,
        resolved_now_ms,
    } = target;

    // The caller (the distributed apply loop) only routes decoded
    // `ArrayOp::Put` / `Delete` plans here, so this match is exhaustive in
    // practice; the guard arm turns a dispatch bug into a loud error rather
    // than a silent mis-apply.
    let array_id = match &plan {
        PhysicalPlan::Array(ArrayOp::Put { array_id, .. })
        | PhysicalPlan::Array(ArrayOp::Delete { array_id, .. }) => array_id.clone(),
        other => {
            let e = crate::Error::Internal {
                detail: format!(
                    "apply_array_cell_write called with a non-array-cell plan: {other:?}"
                ),
            };
            tracker.complete(group_id, log_index, applied_key, Err(e));
            return false;
        }
    };

    // A follower must have the array open on the Data Plane before a Put/Delete
    // can land. Idempotent on the Data Plane side (re-open with the same schema
    // hash returns Ok).
    if let Err(e) = ensure_array_open(state, &array_id, vshard, tenant_id, database_id).await {
        warn!(
            group_id, index = log_index, array = %array_id.name, error = %e,
            "apply_array_cell_write: ensure_array_open failed"
        );
        tracker.complete(group_id, log_index, applied_key, Err(e));
        return false;
    }

    let result = submit_array_write(
        state,
        ArrayWriteSubmit {
            tenant_id,
            database_id,
            vshard,
            plan,
            // Cluster mode has exactly one write-apply path — this one — so a
            // committed user write keeps the `User` source its proposer had,
            // exactly as the generic committed-write branch does.
            event_source: crate::event::EventSource::User,
            resolved_now_ms,
            apply_key: applied_key,
            op_label: "array cell write",
        },
    )
    .await;

    if let Err(e) = &result {
        warn!(
            group_id, index = log_index, array = %array_id.name, error = %e,
            "apply_array_cell_write: apply failed"
        );
    }
    let applied_ok = result.is_ok();
    tracker.complete(group_id, log_index, applied_key, result);
    applied_ok
}
