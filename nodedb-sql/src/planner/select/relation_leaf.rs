// SPDX-License-Identifier: Apache-2.0

//! Refusal of an index or tier read over a statement relation.
//!
//! A search leaf reads a stored index by collection name. CTE inlining
//! replaces a `Scan` with the CTE's body, but a search leaf has no body to
//! take. Left alone, the leaf reads a collection named after the CTE: none, or
//! a stored one the CTE shadows, whose rows the statement never asked for.

use crate::error::{Result, SqlError};
use crate::types::{
    DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, RangeScanPlan,
    RecursiveScanPlan, SqlCatalog, SqlPlan, TextSearchPlan, TimeseriesScanPlan,
};

/// Refuse `plan` when its by-name leaf names a CTE or derived table of
/// `catalog`, the catalog the leaf's FROM resolved against.
pub(super) fn refuse_leaf_over_statement_relation(
    plan: &SqlPlan,
    catalog: &dyn SqlCatalog,
) -> Result<()> {
    let Some(collection) = by_name_leaf(plan) else {
        return Ok(());
    };
    if !catalog.is_statement_relation(collection) {
        return Ok(());
    }
    Err(SqlError::Unsupported {
        detail: format!(
            "{} reads a stored index by collection name, and '{collection}' is a CTE or derived \
             table; name the stored collection in FROM and write its predicates beside the search",
            plan_leaf_name(plan)
        ),
    })
}

/// The name a refusal gives the leaf, looking through a computed tail.
fn plan_leaf_name(plan: &SqlPlan) -> &'static str {
    match plan {
        SqlPlan::Subquery { input, .. } => plan_leaf_name(input),
        other => other.variant_name(),
    }
}

/// The collection `plan` reads by name with no body to inline, looking through
/// a computed tail. A `Scan` inlines, and every other plan holds no such leaf
/// of its own: a nested SELECT is checked where it plans.
fn by_name_leaf(plan: &SqlPlan) -> Option<&str> {
    match plan {
        SqlPlan::Subquery { input, .. } => by_name_leaf(input),
        SqlPlan::VectorSearch { collection, .. }
        | SqlPlan::MultiVectorSearch { collection, .. }
        | SqlPlan::SparseSearch { collection, .. }
        | SqlPlan::SpatialScan { collection, .. }
        | SqlPlan::PointGet { collection, .. }
        | SqlPlan::TextSearch(TextSearchPlan { collection, .. })
        | SqlPlan::HybridSearch(HybridSearchPlan { collection, .. })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { collection, .. })
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { collection, .. })
        | SqlPlan::RangeScan(RangeScanPlan { collection, .. })
        | SqlPlan::TimeseriesScan(TimeseriesScanPlan { collection, .. })
        | SqlPlan::RecursiveScan(RecursiveScanPlan { collection, .. }) => Some(collection),
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Scan { .. }
        | SqlPlan::Insert(_)
        | SqlPlan::KvInsert(_)
        | SqlPlan::Upsert(_)
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::Join { .. }
        | SqlPlan::Aggregate { .. }
        | SqlPlan::TimeseriesIngest(_)
        | SqlPlan::Union { .. }
        | SqlPlan::Intersect { .. }
        | SqlPlan::Except { .. }
        | SqlPlan::RecursiveValue(_)
        | SqlPlan::Cte(_)
        | SqlPlan::CreateArray(_)
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray(_)
        | SqlPlan::InsertArray(_)
        | SqlPlan::DeleteArray(_)
        | SqlPlan::ArraySlice(_)
        | SqlPlan::ArrayAgg(_)
        | SqlPlan::ArrayElementwise(_)
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::Merge(_)
        | SqlPlan::LateralTopK(_)
        | SqlPlan::LateralLoop(_)
        | SqlPlan::VectorPrimaryInsert(_)
        | SqlPlan::VectorPrimaryDelete(_)
        | SqlPlan::VectorPrimaryTruncate(_)
        | SqlPlan::VectorPrimaryUpdate(_)
        | SqlPlan::CreateIndex(_)
        | SqlPlan::DropIndex(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Projection, SqlValue};

    fn multi_vector() -> SqlPlan {
        SqlPlan::MultiVectorSearch {
            collection: "c".to_string(),
            query_vector: vec![0.1],
            top_k: 1,
            ef_search: 2,
            projection: Vec::<Projection>::new(),
            temporal: crate::TemporalScope::default(),
        }
    }

    fn recursive_scan() -> SqlPlan {
        SqlPlan::RecursiveScan(RecursiveScanPlan {
            collection: "c".to_string(),
            base_filters: Vec::new(),
            recursive_filters: Vec::new(),
            join_link: None,
            max_iterations: 1,
            distinct: false,
            limit: 1,
            projection: Vec::new(),
        })
    }

    fn point_get() -> SqlPlan {
        SqlPlan::PointGet {
            collection: "c".to_string(),
            alias: None,
            engine: crate::types::EngineType::DocumentSchemaless,
            key_column: "id".to_string(),
            key_value: SqlValue::Int(1),
            projection: Vec::new(),
        }
    }

    /// These leaves have no SQL shape over a CTE through the test catalog, so
    /// the walk is checked on the plans themselves.
    #[test]
    fn each_family_reports_its_collection() {
        for plan in [multi_vector(), recursive_scan(), point_get()] {
            assert_eq!(by_name_leaf(&plan), Some("c"), "{plan:?}");
        }
    }

    #[test]
    fn a_computed_tail_does_not_hide_the_leaf() {
        let tail = SqlPlan::Subquery {
            input: Box::new(multi_vector()),
            filters: Vec::new(),
            projection: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        };
        assert_eq!(by_name_leaf(&tail), Some("c"));
        assert_eq!(plan_leaf_name(&tail), "MultiVectorSearch");
    }

    /// A `Scan` takes the CTE's body, so it is no by-name leaf.
    #[test]
    fn a_scan_is_not_a_by_name_leaf() {
        let scan = SqlPlan::Scan {
            collection: "c".to_string(),
            alias: None,
            engine: crate::types::EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        };
        assert_eq!(by_name_leaf(&scan), None);
    }
}
