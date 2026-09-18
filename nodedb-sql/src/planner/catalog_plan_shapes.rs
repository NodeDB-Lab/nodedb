// SPDX-License-Identifier: Apache-2.0

//! Catalog folding for reusable projection, ordering, aggregate, and window shapes.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::types::{AggregateExpr, Projection, SortKey, SqlExpr, WindowSpec};

use super::catalog_expr_fold::{fold_expr, validate_expr};

pub(super) fn validate_projection(
    projection: &[Projection],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    for item in projection {
        match item {
            Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                validate_expr(expr, catalog, database_id, tenant_id)?;
            }
            Projection::Column(_) | Projection::Star | Projection::QualifiedStar(_) => {}
        }
    }
    Ok(())
}

pub(super) fn validate_sort_keys(
    sort_keys: &[SortKey],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    for key in sort_keys {
        validate_expr(&key.expr, catalog, database_id, tenant_id)?;
    }
    Ok(())
}

pub(super) fn validate_aggregates(
    aggregates: &[AggregateExpr],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    for aggregate in aggregates {
        for arg in &aggregate.args {
            validate_expr(arg, catalog, database_id, tenant_id)?;
        }
    }
    Ok(())
}

pub(super) fn validate_windows(
    windows: &[WindowSpec],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    for window in windows {
        for expr in window.args.iter().chain(&window.partition_by) {
            validate_expr(expr, catalog, database_id, tenant_id)?;
        }
        validate_sort_keys(&window.order_by, catalog, database_id, tenant_id)?;
    }
    Ok(())
}

pub(super) fn fold_projection(
    projection: &mut [Projection],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    // The fold resolves catalog casts (`regclass`, `regtype`) only; a
    // sequence accessor in a Control-Plane-computed item stays a call.
    for item in projection {
        match item {
            Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                *expr = fold_expr(owned, catalog, database_id, tenant_id);
            }
            Projection::Column(_) | Projection::Star | Projection::QualifiedStar(_) => {}
        }
    }
}

pub(super) fn fold_sort_keys(
    sort_keys: &mut [SortKey],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    for key in sort_keys {
        let owned = std::mem::replace(&mut key.expr, SqlExpr::Wildcard);
        key.expr = fold_expr(owned, catalog, database_id, tenant_id);
    }
}

pub(super) fn fold_aggregates(
    aggregates: &mut [AggregateExpr],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    for aggregate in aggregates {
        for arg in &mut aggregate.args {
            let owned = std::mem::replace(arg, SqlExpr::Wildcard);
            *arg = fold_expr(owned, catalog, database_id, tenant_id);
        }
    }
}

pub(super) fn fold_windows(
    windows: &mut [WindowSpec],
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    for window in windows {
        for expr in window.args.iter_mut().chain(&mut window.partition_by) {
            let owned = std::mem::replace(expr, SqlExpr::Wildcard);
            *expr = fold_expr(owned, catalog, database_id, tenant_id);
        }
        fold_sort_keys(&mut window.order_by, catalog, database_id, tenant_id);
    }
}
