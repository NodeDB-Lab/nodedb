// SPDX-License-Identifier: Apache-2.0

//! Catalog-dependent expression validation across nested SQL plan shapes.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::types::{FilterExpr, MergePlanAction, SqlPlan};

use super::catalog_expr_fold::validate_expr;
use super::catalog_plan_shapes::{
    validate_aggregates, validate_projection, validate_sort_keys, validate_windows,
};

pub(super) fn validate_catalog_exprs(
    plan: &SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    match plan {
        SqlPlan::Scan {
            filters,
            projection,
            sort_keys,
            window_functions,
            ..
        }
        | SqlPlan::DocumentIndexLookup {
            filters,
            projection,
            sort_keys,
            window_functions,
            ..
        } => {
            validate_filters(filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
            validate_sort_keys(sort_keys, catalog, database_id, tenant_id)?;
            validate_windows(window_functions, catalog, database_id, tenant_id)?;
        }
        SqlPlan::PointGet { projection, .. } | SqlPlan::RangeScan { projection, .. } => {
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Update {
            assignments,
            filters,
            ..
        } => {
            for (_, expr) in assignments {
                validate_expr(expr, catalog, database_id, tenant_id)?;
            }
            validate_filters(filters, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Delete { filters, .. } => {
            validate_filters(filters, catalog, database_id, tenant_id)?
        }
        SqlPlan::UpdateFrom {
            source,
            assignments,
            target_filters,
            ..
        } => {
            validate_catalog_exprs(source, catalog, database_id, tenant_id)?;
            for (_, expr) in assignments {
                validate_expr(expr, catalog, database_id, tenant_id)?;
            }
            validate_filters(target_filters, catalog, database_id, tenant_id)?;
        }
        SqlPlan::InsertSelect { source, .. } => {
            validate_catalog_exprs(source, catalog, database_id, tenant_id)?
        }
        SqlPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            sort_keys,
            ..
        } => {
            validate_catalog_exprs(input, catalog, database_id, tenant_id)?;
            for expr in group_by {
                validate_expr(expr, catalog, database_id, tenant_id)?;
            }
            validate_aggregates(aggregates, catalog, database_id, tenant_id)?;
            validate_filters(having, catalog, database_id, tenant_id)?;
            validate_sort_keys(sort_keys, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Union { inputs, .. } => {
            for input in inputs {
                validate_catalog_exprs(input, catalog, database_id, tenant_id)?;
            }
        }
        SqlPlan::Intersect { left, right, .. } | SqlPlan::Except { left, right, .. } => {
            validate_catalog_exprs(left, catalog, database_id, tenant_id)?;
            validate_catalog_exprs(right, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Cte { definitions, outer } => {
            for (_, definition) in definitions {
                validate_catalog_exprs(definition, catalog, database_id, tenant_id)?;
            }
            validate_catalog_exprs(outer, catalog, database_id, tenant_id)?;
        }
        // The post-processing tail's body keeps its own expressions; stopping
        // the walk at the wrapper skips validating all of them.
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            sort_keys,
            window_functions,
            ..
        } => {
            validate_catalog_exprs(input, catalog, database_id, tenant_id)?;
            validate_filters(filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
            validate_sort_keys(sort_keys, catalog, database_id, tenant_id)?;
            validate_windows(window_functions, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Join {
            left,
            right,
            condition,
            projection,
            filters,
            ..
        } => {
            validate_catalog_exprs(left, catalog, database_id, tenant_id)?;
            validate_catalog_exprs(right, catalog, database_id, tenant_id)?;
            if let Some(condition) = condition {
                validate_expr(condition, catalog, database_id, tenant_id)?;
            }
            validate_projection(projection, catalog, database_id, tenant_id)?;
            validate_filters(filters, catalog, database_id, tenant_id)?;
        }
        SqlPlan::LateralTopK {
            outer,
            inner_filters,
            inner_order_by,
            projection,
            ..
        } => {
            validate_catalog_exprs(outer, catalog, database_id, tenant_id)?;
            validate_filters(inner_filters, catalog, database_id, tenant_id)?;
            validate_sort_keys(inner_order_by, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::LateralLoop {
            outer,
            inner,
            projection,
            ..
        } => {
            validate_catalog_exprs(outer, catalog, database_id, tenant_id)?;
            validate_catalog_exprs(inner, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::Merge {
            source, clauses, ..
        } => {
            validate_catalog_exprs(source, catalog, database_id, tenant_id)?;
            for clause in clauses {
                validate_filters(&clause.extra_predicate, catalog, database_id, tenant_id)?;
                match &clause.action {
                    MergePlanAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            validate_expr(expr, catalog, database_id, tenant_id)?;
                        }
                    }
                    MergePlanAction::Insert { values, .. } => {
                        for expr in values {
                            validate_expr(expr, catalog, database_id, tenant_id)?;
                        }
                    }
                    MergePlanAction::Delete | MergePlanAction::DoNothing => {}
                }
            }
        }
        SqlPlan::VectorSearch {
            filters,
            projection,
            ..
        } => {
            validate_filters(filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::MultiVectorSearch { projection, .. }
        | SqlPlan::SparseSearch { projection, .. }
        | SqlPlan::HybridSearch { projection, .. }
        | SqlPlan::HybridSearchTriple { projection, .. } => {
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::TextSearch {
            filters,
            projection,
            ..
        } => {
            validate_filters(filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::SpatialScan {
            attribute_filters,
            projection,
            ..
        } => {
            validate_filters(attribute_filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        SqlPlan::RecursiveScan {
            base_filters,
            recursive_filters,
            projection,
            ..
        } => {
            validate_filters(base_filters, catalog, database_id, tenant_id)?;
            validate_filters(recursive_filters, catalog, database_id, tenant_id)?;
            validate_projection(projection, catalog, database_id, tenant_id)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_filters(
    filters: &[crate::types::Filter],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    for filter in filters {
        validate_filter_expr(&filter.expr, catalog, database_id, tenant_id)?;
    }
    Ok(())
}

fn validate_filter_expr(
    expr: &FilterExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    match expr {
        FilterExpr::Expr(expr) => validate_expr(expr, catalog, database_id, tenant_id),
        FilterExpr::And(children) | FilterExpr::Or(children) => {
            validate_filters(children, catalog, database_id, tenant_id)
        }
        FilterExpr::Not(child) => {
            validate_filter_expr(&child.expr, catalog, database_id, tenant_id)
        }
        _ => Ok(()),
    }
}
