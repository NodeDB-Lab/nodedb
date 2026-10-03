// SPDX-License-Identifier: Apache-2.0

//! Plan-time constant folding for catalog-dependent expressions.
//!
//! `fold_catalog_exprs_in_plan` walks a `SqlPlan` and replaces
//! `Cast { expr: Literal(String(s)), to_type: "regclass" }` and
//! `Cast { expr: Literal(String(s)), to_type: "regtype" }` nodes with
//! their resolved OID literals using the `SqlCatalog` trait.  This keeps
//! the data-plane evaluator pure (no catalog/session context) while still
//! supporting the `'name'::regclass` / `'name'::regtype` PostgreSQL idiom.

use crate::types::{CtePlan, LateralLoopPlan, LateralTopKPlan, MergePlan};
use nodedb_types::DatabaseId;

use super::filter::fold_filter;
use super::leaf::fold_leaf;
use crate::catalog::SqlCatalog;
use crate::types::{MergePlanAction, SqlExpr, SqlPlan};

use crate::planner::catalog_expr_fold::fold_expr;
use crate::planner::catalog_plan_shapes::{
    fold_aggregates, fold_projection, fold_sort_keys, fold_windows,
};
use crate::planner::catalog_plan_validate::validate_catalog_exprs;

/// Walk every `Filter` in `plan` and fold catalog-dependent cast expressions
/// to their constant OID equivalents.
///
/// Mutates the plan in-place (via owned `SqlPlan`). The caller owns the plan
/// after `plan_sql` returns; this pass runs between planning and physical
/// conversion where the catalog is still available.
pub fn fold_catalog_exprs_in_plan(
    plan: SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<SqlPlan> {
    validate_catalog_exprs(&plan, catalog, database_id, tenant_id)?;
    Ok(walk_plan(plan, catalog, database_id, tenant_id))
}

fn walk_plan(
    plan: SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> SqlPlan {
    match plan {
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            mut filters,
            mut projection,
            mut sort_keys,
            limit,
            offset,
            distinct,
            mut window_functions,
            temporal,
        } => {
            for f in &mut filters {
                fold_filter(f, catalog, database_id, tenant_id);
            }
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            fold_sort_keys(&mut sort_keys, catalog, database_id, tenant_id);
            fold_windows(&mut window_functions, catalog, database_id, tenant_id);
            SqlPlan::Scan {
                collection,
                alias,
                engine,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
                window_functions,
                temporal,
            }
        }

        SqlPlan::Union { inputs, distinct } => SqlPlan::Union {
            inputs: inputs
                .into_iter()
                .map(|input| walk_plan(input, catalog, database_id, tenant_id))
                .collect(),
            distinct,
        },

        SqlPlan::Intersect { left, right, all } => SqlPlan::Intersect {
            left: Box::new(walk_plan(*left, catalog, database_id, tenant_id)),
            right: Box::new(walk_plan(*right, catalog, database_id, tenant_id)),
            all,
        },

        SqlPlan::Except { left, right, all } => SqlPlan::Except {
            left: Box::new(walk_plan(*left, catalog, database_id, tenant_id)),
            right: Box::new(walk_plan(*right, catalog, database_id, tenant_id)),
            all,
        },

        SqlPlan::Cte(CtePlan { definitions, outer }) => SqlPlan::Cte(CtePlan {
            definitions: definitions
                .into_iter()
                .map(|(name, plan)| (name, walk_plan(plan, catalog, database_id, tenant_id)))
                .collect(),
            outer: Box::new(walk_plan(*outer, catalog, database_id, tenant_id)),
        }),

        // The post-processing tail wraps a body that keeps its own filters. A
        // wrapper that stopped the walk left every catalog cast inside the body
        // unfolded (`attrelid = 'x'::regclass` stays an unevaluated cast), and
        // an unfolded cast matches no row — the query then succeeds with zero
        // rows instead of failing.
        SqlPlan::Subquery {
            input,
            mut filters,
            mut projection,
            mut window_functions,
            mut sort_keys,
            offset,
            distinct,
            limit,
        } => {
            for f in &mut filters {
                fold_filter(f, catalog, database_id, tenant_id);
            }
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            fold_windows(&mut window_functions, catalog, database_id, tenant_id);
            fold_sort_keys(&mut sort_keys, catalog, database_id, tenant_id);
            SqlPlan::Subquery {
                input: Box::new(walk_plan(*input, catalog, database_id, tenant_id)),
                filters,
                projection,
                window_functions,
                sort_keys,
                offset,
                distinct,
                limit,
            }
        }

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            mut projection,
            mut filters,
        } => {
            for f in &mut filters {
                fold_filter(f, catalog, database_id, tenant_id);
            }
            let condition = condition.map(|e| fold_expr(e, catalog, database_id, tenant_id));
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            SqlPlan::Join {
                left: Box::new(walk_plan(*left, catalog, database_id, tenant_id)),
                right: Box::new(walk_plan(*right, catalog, database_id, tenant_id)),
                on,
                join_type,
                condition,
                limit,
                projection,
                filters,
            }
        }

        SqlPlan::UpdateFrom {
            collection,
            engine,
            source,
            target_join_col,
            source_join_col,
            mut assignments,
            mut target_filters,
            returning,
        } => {
            for (_, expr) in &mut assignments {
                let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                *expr = fold_expr(owned, catalog, database_id, tenant_id);
            }
            for filter in &mut target_filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            SqlPlan::UpdateFrom {
                collection,
                engine,
                source: Box::new(walk_plan(*source, catalog, database_id, tenant_id)),
                target_join_col,
                source_join_col,
                assignments,
                target_filters,
                returning,
            }
        }

        SqlPlan::InsertSelect {
            target,
            source,
            limit,
            column_map,
        } => SqlPlan::InsertSelect {
            target,
            source: Box::new(walk_plan(*source, catalog, database_id, tenant_id)),
            limit,
            column_map,
        },

        SqlPlan::Aggregate {
            input,
            mut group_by,
            group_by_aliases,
            output_order,
            mut aggregates,
            mut having,
            limit,
            grouping_sets,
            mut sort_keys,
        } => {
            for expr in &mut group_by {
                let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                *expr = fold_expr(owned, catalog, database_id, tenant_id);
            }
            fold_aggregates(&mut aggregates, catalog, database_id, tenant_id);
            for filter in &mut having {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            fold_sort_keys(&mut sort_keys, catalog, database_id, tenant_id);
            SqlPlan::Aggregate {
                input: Box::new(walk_plan(*input, catalog, database_id, tenant_id)),
                group_by,
                group_by_aliases,
                output_order,
                aggregates,
                having,
                limit,
                grouping_sets,
                sort_keys,
            }
        }

        SqlPlan::LateralTopK(LateralTopKPlan {
            outer,
            outer_alias,
            inner_collection,
            mut inner_filters,
            mut inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            mut projection,
            left_join,
        }) => {
            for filter in &mut inner_filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            fold_sort_keys(&mut inner_order_by, catalog, database_id, tenant_id);
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            SqlPlan::LateralTopK(LateralTopKPlan {
                outer: Box::new(walk_plan(*outer, catalog, database_id, tenant_id)),
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join,
            })
        }

        SqlPlan::LateralLoop(LateralLoopPlan {
            outer,
            outer_alias,
            inner,
            correlation_predicates,
            lateral_alias,
            mut projection,
            outer_row_cap,
            left_join,
        }) => {
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            SqlPlan::LateralLoop(LateralLoopPlan {
                outer: Box::new(walk_plan(*outer, catalog, database_id, tenant_id)),
                outer_alias,
                inner: Box::new(walk_plan(*inner, catalog, database_id, tenant_id)),
                correlation_predicates,
                lateral_alias,
                projection,
                outer_row_cap,
                left_join,
            })
        }

        SqlPlan::Merge(MergePlan {
            target,
            engine,
            source,
            target_join_col,
            source_join_col,
            source_alias,
            mut clauses,
            returning,
        }) => {
            for clause in &mut clauses {
                for filter in &mut clause.extra_predicate {
                    fold_filter(filter, catalog, database_id, tenant_id);
                }
                match &mut clause.action {
                    MergePlanAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                            *expr = fold_expr(owned, catalog, database_id, tenant_id);
                        }
                    }
                    MergePlanAction::Insert { values, .. } => {
                        for expr in values {
                            let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                            *expr = fold_expr(owned, catalog, database_id, tenant_id);
                        }
                    }
                    MergePlanAction::Delete | MergePlanAction::DoNothing => {}
                }
            }
            SqlPlan::Merge(MergePlan {
                target,
                engine,
                source: Box::new(walk_plan(*source, catalog, database_id, tenant_id)),
                target_join_col,
                source_join_col,
                source_alias,
                clauses,
                returning,
            })
        }

        // Leaf plans: `fold_leaf` folds the expressions each one carries.
        mut other => {
            fold_leaf(&mut other, catalog, database_id, tenant_id);
            other
        }
    }
}
