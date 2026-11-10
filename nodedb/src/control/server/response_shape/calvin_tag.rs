// SPDX-License-Identifier: BUSL-1.1

//! The count-bearing outcome of a Calvin plan that folds without a
//! round-trip to the Data Plane.

use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

use super::types::DmlOutcome;

/// The count-bearing outcome a plan answers without a round-trip to the
/// Data Plane, or `None` when its count depends on state the plan never read.
///
/// Folding is only sound for a write that CANNOT be a no-op — one that either
/// applies exactly one row or fails the statement. Any write whose row count
/// depends on state (a point delete of an absent row, an `ON CONFLICT DO
/// NOTHING` insert onto an existing key, a batch, a predicate) gets its
/// count from the mutation's own response, because a synthesised count is a
/// claim about rows nobody looked at.
///
/// Foldable: `DocumentOp::PointPut` (`INSERT 0 1`) and `KvOp::Put`
/// (`UPSERT 1`), the two upserts that always write one row. Every other plan,
/// read or write, answers `None`.
pub fn calvin_tag_for_plan(plan: &PhysicalPlan) -> Option<DmlOutcome> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. }) => Some(DmlOutcome {
            verb: "INSERT",
            affected: 1,
        }),
        // The SQL `UPSERT` statement: same tag as its `DocumentOp::Upsert`
        // sibling and as `describe_plan`'s `DmlResult("UPSERT")` arm.
        PhysicalPlan::Kv(KvOp::Put { .. }) => Some(DmlOutcome {
            verb: "UPSERT",
            affected: 1,
        }),
        // The foldable arms above take precedence; every remaining op of each
        // engine answers from its payload or folds opaque.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}
