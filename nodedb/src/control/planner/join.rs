//! Join conversion helpers for the plan converter.
//!
//! Extracted from `converter.rs` to keep that file under the 500-line limit.
//! Handles `LogicalPlan::Join` conversion to `PhysicalPlan::HashJoin`.

use datafusion::logical_expr::{Join, JoinType};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
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

    if on_keys.is_empty() {
        return Err(crate::Error::PlanError {
            detail: "JOIN requires at least one ON condition".into(),
        });
    }

    // Route to the left collection's vShard.
    let vshard = VShardId::from_collection(&left_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::HashJoin {
            left_collection,
            right_collection,
            on: on_keys,
            join_type: join_type_str.to_string(),
            limit: 1000,
        },
    }])
}
