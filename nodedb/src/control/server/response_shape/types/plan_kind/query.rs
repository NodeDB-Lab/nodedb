// SPDX-License-Identifier: BUSL-1.1

//! `QueryOp` classification.

use nodedb_physical::physical_plan::QueryOp;

use super::describe::describe_plan;
use super::kind::PlanKind;

pub(super) fn describe_query(op: &QueryOp) -> PlanKind {
    match op {
        // Exchange means the plan wasn't yet resolved — recurse into the child.
        QueryOp::Exchange(exchange) => describe_plan(&exchange.child),

        // PostProcess reshapes a multi-row subquery; its kind is the child's.
        QueryOp::PostProcess { input, .. } => describe_plan(input),

        // Constant/catalog-scan expressions compile to ProviderScan, and a
        // SetOp resolves to a ProviderScan of merged rows: each element
        // streams as its own pgwire row.
        QueryOp::ProviderScan { .. }
        | QueryOp::SetOp { .. }
        | QueryOp::Aggregate { .. }
        | QueryOp::FacetCounts { .. }
        | QueryOp::HashJoin { .. }
        | QueryOp::NestedLoopJoin { .. }
        | QueryOp::SortMergeJoin { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. }
        | QueryOp::LateralTopK { .. }
        | QueryOp::LateralLoop { .. } => PlanKind::MultiRow,

        // Intra-plan stages: their payload feeds the next stage of the same
        // plan and never reaches a client.
        QueryOp::PartialAggregate { .. }
        | QueryOp::PartialAggregateState { .. }
        | QueryOp::ShuffleJoinConsume { .. }
        | QueryOp::ShuffleAggregateConsume { .. } => PlanKind::Execution,
    }
}
