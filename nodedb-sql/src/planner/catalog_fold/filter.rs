// SPDX-License-Identifier: Apache-2.0

//! Catalog folding within filter trees.

use crate::catalog::SqlCatalog;
use crate::planner::catalog_expr_fold::fold_expr;
use crate::types::{Filter, FilterExpr, SqlExpr};
use nodedb_types::DatabaseId;

pub(super) fn fold_filter(
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
