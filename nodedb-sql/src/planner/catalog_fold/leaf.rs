// SPDX-License-Identifier: Apache-2.0

//! Catalog folding within leaf read and write plans.

use super::filter::fold_filter;
use crate::catalog::SqlCatalog;
use crate::planner::catalog_expr_fold::fold_expr;
use crate::planner::catalog_plan_shapes::{fold_projection, fold_sort_keys, fold_windows};
use crate::types::{
    DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, RangeScanPlan,
    RecursiveScanPlan, SqlExpr, SqlPlan, TextSearchPlan, VectorPrimaryDeletePlan,
    VectorPrimaryUpdatePlan,
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
        SqlPlan::TextSearch(TextSearchPlan {
            filters,
            projection,
            ..
        })
        | SqlPlan::HybridSearch(HybridSearchPlan {
            filters,
            projection,
            ..
        })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
            filters,
            projection,
            ..
        }) => {
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
        | SqlPlan::SparseSearch { projection, .. } => {
            fold_projection(projection, catalog, database_id, tenant_id);
        }
        // Composite plans: `walk_plan` folds them before reaching a leaf.
        SqlPlan::Scan { .. }
        | SqlPlan::Union { .. }
        | SqlPlan::Intersect { .. }
        | SqlPlan::Except { .. }
        | SqlPlan::Cte(_)
        | SqlPlan::Subquery { .. }
        | SqlPlan::Join { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Aggregate { .. }
        | SqlPlan::LateralTopK(_)
        | SqlPlan::LateralLoop(_)
        | SqlPlan::Merge(_) => {}
        // Plans whose expressions this pass does not fold.
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Insert(_)
        | SqlPlan::KvInsert(_)
        | SqlPlan::Upsert(_)
        | SqlPlan::Truncate { .. }
        | SqlPlan::TimeseriesScan(_)
        | SqlPlan::TimeseriesIngest(_)
        | SqlPlan::RecursiveValue(_)
        | SqlPlan::CreateArray(_)
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray(_)
        | SqlPlan::InsertArray(_)
        | SqlPlan::DeleteArray(_)
        | SqlPlan::ArraySlice(_)
        | SqlPlan::ArrayProject(_)
        | SqlPlan::ArrayAgg(_)
        | SqlPlan::ArrayElementwise(_)
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::VectorPrimaryInsert(_)
        | SqlPlan::VectorPrimaryTruncate(_)
        | SqlPlan::CreateIndex(_)
        | SqlPlan::DropIndex(_) => {}
    }
}
