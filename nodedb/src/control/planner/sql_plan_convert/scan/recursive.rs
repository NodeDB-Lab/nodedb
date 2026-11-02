// SPDX-License-Identifier: BUSL-1.1

//! Recursive (CTE-style) scan converters.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::VShardId;
use nodedb_physical::physical_plan::*;

use super::super::filter::serialize_filters;
use super::super::scan_params::{RecursiveScanParams, RecursiveValueParams};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in crate::control::planner::sql_plan_convert) fn convert_recursive_scan(
    p: RecursiveScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let collection_key = nodedb_types::CollectionKey::from_bare(p.database_id, p.collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(p.database_id, p.collection);
    let vshard = collection_key.vshard();
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        database_id: p.database_id,
        plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
            collection: qualified_collection,
            base_filters: serialize_filters(p.base_filters)?,
            recursive_filters: serialize_filters(p.recursive_filters)?,
            join_link: p.join_link.clone(),
            max_iterations: *p.max_iterations,
            distinct: *p.distinct,
            limit: *p.limit,
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_recursive_value(
    p: RecursiveValueParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    // Value-generating CTEs do not target a specific vShard.  Route to shard 0
    // (the coordinator shard) which executes purely in-memory with no storage access.
    let vshard = VShardId::new(0);
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        database_id: p.database_id,
        plan: PhysicalPlan::Query(QueryOp::RecursiveValue {
            cte_name: p.cte_name.into(),
            columns: p.columns.to_vec(),
            init_exprs: p.init_exprs.to_vec(),
            step_exprs: p.step_exprs.to_vec(),
            condition: p.condition.clone(),
            max_depth: *p.max_depth,
            distinct: *p.distinct,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
