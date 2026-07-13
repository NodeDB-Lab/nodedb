use crate::bridge::envelope::PhysicalPlan;

use super::AuthorizationRequirement;
use super::collect::{add_general_requirements, add_read};

pub(super) fn collect_query_requirements<'a>(
    plan: &'a PhysicalPlan,
    pending: &mut Vec<&'a PhysicalPlan>,
    out: &mut Vec<AuthorizationRequirement>,
) -> bool {
    use nodedb_physical::physical_plan::{QueryOp, VectorOp};

    match plan {
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        }) => {
            add_read(left_collection, out);
            add_read(right_collection, out);
            for nested in [left_input, right_input, left_bitmap, right_bitmap]
                .into_iter()
                .flatten()
            {
                pending.push(nested);
            }
            true
        }
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            ..
        }) => {
            add_read(left_collection, out);
            add_read(right_collection, out);
            true
        }
        PhysicalPlan::Query(QueryOp::Aggregate { input, .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregateState { input, .. }) => {
            add_general_requirements(plan, out);
            if let Some(nested) = input {
                pending.push(nested);
            }
            true
        }
        PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan,
            inner_collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan,
            inner_collection,
            ..
        }) => {
            add_read(inner_collection, out);
            pending.push(outer_plan);
            true
        }
        PhysicalPlan::Query(QueryOp::Exchange(exchange)) => {
            pending.push(&exchange.child);
            true
        }
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(provider),
            ..
        }) => {
            add_read(provider, out);
            true
        }
        PhysicalPlan::Query(QueryOp::ProviderScan { provider: None, .. }) => true,
        PhysicalPlan::Vector(VectorOp::Search {
            inline_prefilter_plan,
            ..
        }) => {
            add_general_requirements(plan, out);
            if let Some(nested) = inline_prefilter_plan {
                pending.push(nested);
            }
            true
        }
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}
