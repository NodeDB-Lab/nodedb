// SPDX-License-Identifier: Apache-2.0

//! Plan-time constant folding for catalog-dependent expressions.
//!
//! `fold_catalog_exprs_in_plan` walks a `SqlPlan` and replaces
//! `Cast { expr: Literal(String(s)), to_type: "regclass" }` and
//! `Cast { expr: Literal(String(s)), to_type: "regtype" }` nodes with
//! their resolved OID literals using the `SqlCatalog` trait.  This keeps
//! the data-plane evaluator pure (no catalog/session context) while still
//! supporting the `'name'::regclass` / `'name'::regtype` PostgreSQL idiom.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::types::{Filter, FilterExpr, MergePlanAction, SqlExpr, SqlPlan};

use super::catalog_expr_fold::fold_expr;
use super::catalog_plan_shapes::{fold_aggregates, fold_projection, fold_sort_keys, fold_windows};
use super::catalog_plan_validate::validate_catalog_exprs;

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

        SqlPlan::Cte { definitions, outer } => SqlPlan::Cte {
            definitions: definitions
                .into_iter()
                .map(|(name, plan)| (name, walk_plan(plan, catalog, database_id, tenant_id)))
                .collect(),
            outer: Box::new(walk_plan(*outer, catalog, database_id, tenant_id)),
        },

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
            fold_sort_keys(&mut sort_keys, catalog, database_id, tenant_id);
            // Window specs carry their own exprs (args, PARTITION BY,
            // ORDER BY) — a wrapper that skips them leaves catalog casts
            // inside window exprs unfolded, which then match no row.
            fold_windows(&mut window_functions, catalog, database_id, tenant_id);
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

        mut plan @ (SqlPlan::PointGet { .. } | SqlPlan::RangeScan { .. }) => {
            match &mut plan {
                SqlPlan::PointGet { projection, .. } | SqlPlan::RangeScan { projection, .. } => {
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                _ => unreachable!(),
            }
            plan
        }

        mut plan @ (SqlPlan::DocumentIndexLookup { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::Delete { .. }) => {
            match &mut plan {
                SqlPlan::DocumentIndexLookup {
                    filters,
                    projection,
                    sort_keys,
                    window_functions,
                    ..
                } => {
                    for filter in filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    fold_projection(projection, catalog, database_id, tenant_id);
                    fold_sort_keys(sort_keys, catalog, database_id, tenant_id);
                    fold_windows(window_functions, catalog, database_id, tenant_id);
                }
                SqlPlan::Delete { filters, .. } => {
                    for filter in filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                }
                SqlPlan::Update {
                    assignments,
                    filters,
                    ..
                } => {
                    for (_, expr) in assignments {
                        let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                        *expr = fold_expr(owned, catalog, database_id, tenant_id);
                    }
                    for filter in filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                }
                _ => unreachable!(),
            }
            plan
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

        SqlPlan::LateralTopK {
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
        } => {
            for filter in &mut inner_filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            fold_sort_keys(&mut inner_order_by, catalog, database_id, tenant_id);
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            SqlPlan::LateralTopK {
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
            }
        }

        SqlPlan::LateralLoop {
            outer,
            outer_alias,
            inner,
            correlation_predicates,
            lateral_alias,
            mut projection,
            outer_row_cap,
            left_join,
        } => {
            fold_projection(&mut projection, catalog, database_id, tenant_id);
            SqlPlan::LateralLoop {
                outer: Box::new(walk_plan(*outer, catalog, database_id, tenant_id)),
                outer_alias,
                inner: Box::new(walk_plan(*inner, catalog, database_id, tenant_id)),
                correlation_predicates,
                lateral_alias,
                projection,
                outer_row_cap,
                left_join,
            }
        }

        SqlPlan::Merge {
            target,
            engine,
            source,
            target_join_col,
            source_join_col,
            source_alias,
            mut clauses,
            returning,
        } => {
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
            SqlPlan::Merge {
                target,
                engine,
                source: Box::new(walk_plan(*source, catalog, database_id, tenant_id)),
                target_join_col,
                source_join_col,
                source_alias,
                clauses,
                returning,
            }
        }

        mut plan @ (SqlPlan::VectorSearch { .. }
        | SqlPlan::TextSearch { .. }
        | SqlPlan::SpatialScan { .. }
        | SqlPlan::RecursiveScan { .. }) => {
            match &mut plan {
                SqlPlan::VectorSearch {
                    filters,
                    projection,
                    ..
                } => {
                    for filter in filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                SqlPlan::TextSearch {
                    filters,
                    projection,
                    ..
                } => {
                    for filter in filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                SqlPlan::SpatialScan {
                    attribute_filters,
                    projection,
                    ..
                } => {
                    for filter in attribute_filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                SqlPlan::RecursiveScan {
                    base_filters,
                    recursive_filters,
                    projection,
                    ..
                } => {
                    for filter in base_filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    for filter in recursive_filters {
                        fold_filter(filter, catalog, database_id, tenant_id);
                    }
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                _ => unreachable!(),
            }
            plan
        }

        mut plan @ (SqlPlan::MultiVectorSearch { .. }
        | SqlPlan::SparseSearch { .. }
        | SqlPlan::HybridSearch { .. }
        | SqlPlan::HybridSearchTriple { .. }) => {
            match &mut plan {
                SqlPlan::MultiVectorSearch { projection, .. }
                | SqlPlan::SparseSearch { projection, .. }
                | SqlPlan::HybridSearch { projection, .. }
                | SqlPlan::HybridSearchTriple { projection, .. } => {
                    fold_projection(projection, catalog, database_id, tenant_id);
                }
                _ => unreachable!(),
            }
            plan
        }

        // Plan variants without expression-bearing filters pass through unchanged.
        other => other,
    }
}

fn fold_filter(
    filter: &mut Filter,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    fold_filter_expr(&mut filter.expr, catalog, database_id, tenant_id);
}

fn fold_filter_expr(
    expr: &mut FilterExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    match expr {
        FilterExpr::Expr(sql_expr) => {
            let owned = std::mem::replace(sql_expr, SqlExpr::Wildcard);
            *sql_expr = fold_expr(owned, catalog, database_id, tenant_id);
        }
        FilterExpr::And(children) | FilterExpr::Or(children) => {
            for child in children {
                fold_filter_expr(&mut child.expr, catalog, database_id, tenant_id);
            }
        }
        FilterExpr::Not(child) => {
            fold_filter_expr(&mut child.expr, catalog, database_id, tenant_id);
        }
        // Simple comparison, InList, Between, IsNull, IsNotNull — no sub-expressions to fold.
        FilterExpr::Comparison { .. }
        | FilterExpr::InList { .. }
        | FilterExpr::Between { .. }
        | FilterExpr::IsNull { .. }
        | FilterExpr::IsNotNull { .. } => {}
    }
}
