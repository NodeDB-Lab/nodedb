// SPDX-License-Identifier: BUSL-1.1

//! Per-slice write classification: primary-write / RETURNING / change-set
//! predicates shared by the static and active dispatch paths.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::types::VShardId;

/// Whether this vShard's slice carries a PRIMARY user data write — the write
/// whose applied `Response` (affected-count + any RETURNING rows) the
/// coordinator surfaces.
///
/// A primary write is a Document / KV / Vector / Timeseries / Columnar / Array
/// write — NOT the implicit graph-edge cleanup (`EdgePut` / `EdgeDelete`) that
/// dual-homes alongside a document delete/update. For a single-collection user
/// DML (plus its implicit edges) exactly ONE participant carries the primary
/// write, so only it deposits the applied `Response` into the coordinator's
/// sidecar and the edge participants never clobber the entry.
///
/// This gate subsumes the RETURNING case (a RETURNING write IS a primary write,
/// so its rows are still deposited) while ALSO carrying the affected-count of a
/// plain (non-RETURNING) write — which a RETURNING-only gate dropped, making a
/// routed plain write report zero rows affected.
pub(super) fn participant_change_sets(
    plans: &[PhysicalPlan],
    tenant_id: crate::types::TenantId,
    vshard_id: u32,
) -> Vec<crate::control::server::dispatch_utils::WriteChangeSet> {
    plans
        .iter()
        .filter(|plan| match plan {
            // Edge plans are dual-homed; only the source participant publishes
            // the one logical Control-Plane event.
            PhysicalPlan::Graph(
                nodedb_physical::physical_plan::GraphOp::EdgePut { src_id, .. }
                | nodedb_physical::physical_plan::GraphOp::EdgeDelete { src_id, .. },
            ) => VShardId::from_key(src_id.as_bytes()).as_u32() == vshard_id,
            _ => true,
        })
        .map(|plan| {
            crate::control::server::dispatch_utils::extract_write_change_set(plan, tenant_id)
        })
        .collect()
}

/// Whether the transaction carries any write that is NOT a derived side
/// effect (an implicit graph edge, a cross-shard balance delta). Decided over
/// the FULL plan set before it is sliced per vShard, because a slice alone
/// cannot tell a lone derived participant from a derived-only statement.
pub(super) fn txn_has_non_derived_write(plans: &[PhysicalPlan]) -> bool {
    crate::control::planner::calvin::write_class::plans_have_user_write(plans)
}

/// Whether this vShard's slice carries the USER'S own write, as opposed to a
/// derived side effect the Control Plane appended alongside it.
///
/// It gates the applied-response deposit, and that is the whole reason the
/// distinction has to be made: a statement's `CommandComplete` is shaped from
/// ONE deposited response, primary-write participants coalesce first-wins, and
/// a derived participant's response describes a row the user's statement never
/// named. A balance write that won that race handed an `INSERT` tag a count —
/// or, when its flush found the commit already resolved and answered with an
/// empty payload, no count at all — belonging to a different write entirely.
///
/// `is_derived_side_effect` is the named predicate rather than an inline
/// `!matches!(plan, PhysicalPlan::Graph(_))`: the implicit graph edge and the
/// cross-shard balance are the same concept, and spelling it inline here is why
/// the second one never inherited the exclusion.
///
/// `txn_has_non_derived_write` is the transaction-level answer from
/// [`txn_has_non_derived_write`]. When the transaction has one, only the slice
/// holding it is primary. When it has none — the standalone `GRAPH INSERT /
/// DELETE EDGE` DSL's Graph-only tx_class — every write slice is the user's
/// write and deposits; first-wins coalescing then picks one identical count.
/// An empty slice (validate-only read) is never primary.
pub(super) fn plans_have_primary_write(
    plans: &[PhysicalPlan],
    txn_has_non_derived_write: bool,
) -> bool {
    if txn_has_non_derived_write {
        return self::txn_has_non_derived_write(plans);
    }
    plans
        .iter()
        .any(crate::control::planner::calvin::is_write_plan)
}

/// Whether this vShard's slice carries a RETURNING-bearing write — a plan whose
/// applied response is DATA-ROWs rather than a bare affected-count. Uses the
/// SAME `describe_plan` classification the coordinator's response-shaping uses,
/// so the two never disagree about which participant owns the returned rows.
pub(super) fn plans_have_returning(plans: &[PhysicalPlan]) -> bool {
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};
    plans
        .iter()
        .any(|plan| matches!(describe_plan(plan), PlanKind::ReturningRows))
}
