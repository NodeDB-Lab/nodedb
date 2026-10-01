// SPDX-License-Identifier: Apache-2.0

//! Catalog folding within leaf read and write plans.

use super::filter::fold_filter;
use crate::catalog::SqlCatalog;
use crate::planner::catalog_expr_fold::fold_expr;
use crate::planner::catalog_plan_shapes::{fold_projection, fold_sort_keys, fold_windows};
use crate::types::{
    DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, RangeScanPlan,
    RecursiveScanPlan, SqlExpr, SqlPlan, VectorPrimaryDeletePlan, VectorPrimaryUpdatePlan,
};
use nodedb_types::DatabaseId;

pub(super) fn fold_leaf(
    plan: &mut SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) {
    match plan {
        SqlPlan::PointGet { projection, .. }
        | SqlPlan::RangeScan(RangeScanPlan { projection, .. }) => {
            fold_projection(projection, catalog, database_id, tenant_id);
        }
        SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            filters,
            projection,
            sort_keys,
            window_functions,
            ..
        }) => {
            for filter in filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            fold_projection(projection, catalog, database_id, tenant_id);
            fold_sort_keys(sort_keys, catalog, database_id, tenant_id);
            fold_windows(window_functions, catalog, database_id, tenant_id);
        }
        SqlPlan::Delete { filters, .. }
        | SqlPlan::VectorPrimaryDelete(VectorPrimaryDeletePlan { filters, .. }) => {
            for filter in filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
        }
        SqlPlan::Update {
            assignments,
            filters,
            ..
        }
        | SqlPlan::VectorPrimaryUpdate(VectorPrimaryUpdatePlan {
            assignments,
            filters,
            ..
        }) => {
            for (_, expr) in assignments {
                let owned = std::mem::replace(expr, SqlExpr::Wildcard);
                *expr = fold_expr(owned, catalog, database_id, tenant_id);
            }
            for filter in filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
        }
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
        SqlPlan::RecursiveScan(RecursiveScanPlan {
            base_filters,
            recursive_filters,
            projection,
            ..
        }) => {
            for filter in base_filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            for filter in recursive_filters {
                fold_filter(filter, catalog, database_id, tenant_id);
            }
            fold_projection(projection, catalog, database_id, tenant_id);
        }
        SqlPlan::MultiVectorSearch { projection, .. }
        | SqlPlan::SparseSearch { projection, .. }
        | SqlPlan::HybridSearch(HybridSearchPlan { projection, .. })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { projection, .. }) => {
            fold_projection(projection, catalog, database_id, tenant_id);
        }
        _ => {}
    }
}
