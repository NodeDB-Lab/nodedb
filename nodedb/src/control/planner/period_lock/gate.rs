// SPDX-License-Identifier: BUSL-1.1

//! Which `DocumentOp` variants a period lock gates, and how a resolved entry
//! lands back on one.

use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget};

/// The source collection a period lock would gate this op's write against,
/// or `None` for an op `check_period_lock` never runs against. Exhaustive so
/// a new `DocumentOp` variant must state which side it is on.
pub(super) fn period_lock_gated_collection(op: &DocumentOp) -> Option<&str> {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. } => Some(collection.as_str()),
        // `check_period_lock` runs only from `apply_point_put` /
        // `apply_point_delete` / `execute_point_update` /
        // `execute_bulk_update` / `execute_bulk_delete`. `Merge`,
        // `UpdateFromJoin`, and `InsertSelect` are Control-Plane orchestrated
        // over several round trips and resolve through their own
        // orchestrator's per-row expansion (`resolve_period_lock_targets_for_bodies`),
        // not this pass.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Append `entry` to the op's `resolved_sum_targets` slot. Exhaustive for the
/// same reason [`period_lock_gated_collection`] is. Never called for
/// `BatchInsert`, `PointUpdate`, `BulkUpdate`, or `BulkDelete`, which the
/// caller handles directly.
pub(super) fn push_resolved(op: &mut DocumentOp, entry: ResolvedSumTarget) {
    match op {
        DocumentOp::PointPut {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointInsert {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Upsert {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointDelete {
            resolved_sum_targets,
            ..
        } => resolved_sum_targets.push(entry),
        DocumentOp::BatchInsert { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => {}
    }
}
