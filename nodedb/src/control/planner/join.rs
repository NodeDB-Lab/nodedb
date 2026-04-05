//! Join conversion helpers for the plan converter.
//!
//! Handles `LogicalPlan::Join` conversion to `PhysicalPlan::HashJoin`.

use datafusion::logical_expr::{Join, JoinType};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::QueryOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::search::extract_table_name;

/// Convert a DataFusion `Join` logical plan node into a `HashJoin` physical task.
///
/// Only `INNER JOIN` with column-equality `ON` conditions is supported. The
/// resulting task is routed to the left collection's vShard.
pub(super) fn convert_join(join: &Join, tenant_id: TenantId) -> crate::Result<Vec<PhysicalTask>> {
    // Extract collection names from left and right inputs.
    let left_collection =
        extract_table_name(&join.left).ok_or_else(|| crate::Error::PlanError {
            detail: "JOIN left side must be a table scan".into(),
        })?;
    let right_collection =
        extract_table_name(&join.right).ok_or_else(|| crate::Error::PlanError {
            detail: "JOIN right side must be a table scan".into(),
        })?;

    // Map DataFusion JoinType to our string representation.
    let join_type_str = match join.join_type {
        JoinType::Inner => "inner",
        JoinType::Left => "left",
        JoinType::Right => "right",
        JoinType::Full => "full",
        JoinType::LeftSemi | JoinType::RightSemi => "semi",
        JoinType::LeftAnti | JoinType::RightAnti => "anti",
        other => {
            return Err(crate::Error::PlanError {
                detail: format!("{other:?} JOIN is not supported"),
            });
        }
    };

    // Extract join keys from ON clause.
    let mut on_keys = Vec::with_capacity(join.on.len());
    for (left_expr, right_expr) in &join.on {
        let left_col = match left_expr {
            Expr::Column(col) => col.name.clone(),
            _ => {
                return Err(crate::Error::PlanError {
                    detail: "JOIN ON must be column = column".into(),
                });
            }
        };
        let right_col = match right_expr {
            Expr::Column(col) => col.name.clone(),
            _ => {
                return Err(crate::Error::PlanError {
                    detail: "JOIN ON must be column = column".into(),
                });
            }
        };
        on_keys.push((left_col, right_col));
    }

    // If no equi-join keys (cross join, theta join) or if the ON clause
    // has non-column expressions that couldn't be parsed as equi-keys,
    // fall back to nested loop join.
    if on_keys.is_empty() {
        let vshard = VShardId::from_collection(&left_collection);

        // Serialize any filter expression as join condition for NLJ.
        let condition = if let Some(filter) = &join.filter {
            let filters = super::extract::expr_to_scan_filters(filter);
            zerompk::to_msgpack_vec(&filters).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("join condition serialization: {e}"),
            })?
        } else {
            Vec::new()
        };

        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type: join_type_str.to_string(),
                limit: 1000,
            }),
        }]);
    }

    let left_vshard = VShardId::from_collection(&left_collection);
    let right_vshard = VShardId::from_collection(&right_collection);

    // Co-located join: both collections on the same vShard → single-core hash join.
    // This is the fastest path: zero bridge overhead, no data movement.
    if left_vshard == right_vshard {
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: left_vshard,
            plan: PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                on: on_keys,
                join_type: join_type_str.to_string(),
                limit: 1000,
            }),
        }]);
    }

    // Cross-shard join: collections on different vShards.
    // Default strategy: HashJoin on the left collection's core (pulls right side).
    // Future: the CBO will select broadcast vs shuffle based on table statistics.
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: left_vshard,
        plan: PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            on: on_keys,
            join_type: join_type_str.to_string(),
            limit: 1000,
        }),
    }])
}
