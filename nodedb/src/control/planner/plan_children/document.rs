// SPDX-License-Identifier: BUSL-1.1

//! The child plan a document op reads its rows from.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// The child plan `op` reads its rows from, if any.
pub(super) fn child(op: &DocumentOp) -> Option<&PhysicalPlan> {
    match op {
        DocumentOp::InsertSelect { source, .. }
        | DocumentOp::Merge { source, .. }
        | DocumentOp::UpdateFromJoin { source, .. } => source.query_plan(),
        DocumentOp::ResolveWrite(inner) => child(inner),
        DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => None,
    }
}

/// The child plan `op` reads its rows from, if any, mutable.
pub(super) fn child_mut(op: &mut DocumentOp) -> Option<&mut PhysicalPlan> {
    match op {
        DocumentOp::InsertSelect { source, .. }
        | DocumentOp::Merge { source, .. }
        | DocumentOp::UpdateFromJoin { source, .. } => source.query_plan_mut(),
        DocumentOp::ResolveWrite(inner) => child_mut(inner),
        DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => None,
    }
}
