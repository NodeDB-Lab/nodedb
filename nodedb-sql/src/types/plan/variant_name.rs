// SPDX-License-Identifier: Apache-2.0

//! Stable variant names for [`SqlPlan`], used in diagnostics.
//!
//! The match is exhaustive on purpose: a new plan variant must be named
//! here, so an error message can never fall back to "unknown".

use super::variants::SqlPlan;

impl SqlPlan {
    /// The plan variant's name, for error messages and tracing.
    pub fn variant_name(&self) -> &'static str {
        match self {
            SqlPlan::ConstantResult { .. } => "ConstantResult",
            SqlPlan::Scan { .. } => "Scan",
            SqlPlan::PointGet { .. } => "PointGet",
            SqlPlan::DocumentIndexLookup { .. } => "DocumentIndexLookup",
            SqlPlan::RangeScan { .. } => "RangeScan",
            SqlPlan::Insert { .. } => "Insert",
            SqlPlan::KvInsert { .. } => "KvInsert",
            SqlPlan::Upsert { .. } => "Upsert",
            SqlPlan::InsertSelect { .. } => "InsertSelect",
            SqlPlan::Update { .. } => "Update",
            SqlPlan::UpdateFrom { .. } => "UpdateFrom",
            SqlPlan::Delete { .. } => "Delete",
            SqlPlan::Truncate { .. } => "Truncate",
            SqlPlan::Join { .. } => "Join",
            SqlPlan::Aggregate { .. } => "Aggregate",
            SqlPlan::TimeseriesScan { .. } => "TimeseriesScan",
            SqlPlan::TimeseriesIngest { .. } => "TimeseriesIngest",
            SqlPlan::VectorSearch { .. } => "VectorSearch",
            SqlPlan::MultiVectorSearch { .. } => "MultiVectorSearch",
            SqlPlan::SparseSearch { .. } => "SparseSearch",
            SqlPlan::TextSearch { .. } => "TextSearch",
            SqlPlan::HybridSearch { .. } => "HybridSearch",
            SqlPlan::HybridSearchTriple { .. } => "HybridSearchTriple",
            SqlPlan::SpatialScan { .. } => "SpatialScan",
            SqlPlan::Union { .. } => "Union",
            SqlPlan::Intersect { .. } => "Intersect",
            SqlPlan::Except { .. } => "Except",
            SqlPlan::RecursiveScan { .. } => "RecursiveScan",
            SqlPlan::RecursiveValue { .. } => "RecursiveValue",
            SqlPlan::Cte { .. } => "Cte",
            SqlPlan::Subquery { .. } => "Subquery",
            SqlPlan::CreateArray { .. } => "CreateArray",
            SqlPlan::DropArray { .. } => "DropArray",
            SqlPlan::AlterArray { .. } => "AlterArray",
            SqlPlan::InsertArray { .. } => "InsertArray",
            SqlPlan::DeleteArray { .. } => "DeleteArray",
            SqlPlan::ArraySlice { .. } => "ArraySlice",
            SqlPlan::ArrayProject { .. } => "ArrayProject",
            SqlPlan::ArrayAgg { .. } => "ArrayAgg",
            SqlPlan::ArrayElementwise { .. } => "ArrayElementwise",
            SqlPlan::ArrayFlush { .. } => "ArrayFlush",
            SqlPlan::ArrayCompact { .. } => "ArrayCompact",
            SqlPlan::Merge { .. } => "Merge",
            SqlPlan::LateralTopK { .. } => "LateralTopK",
            SqlPlan::LateralLoop { .. } => "LateralLoop",
            SqlPlan::VectorPrimaryInsert { .. } => "VectorPrimaryInsert",
            SqlPlan::VectorPrimaryDelete { .. } => "VectorPrimaryDelete",
            SqlPlan::VectorPrimaryUpdate { .. } => "VectorPrimaryUpdate",
            SqlPlan::CreateIndex { .. } => "CreateIndex",
            SqlPlan::DropIndex { .. } => "DropIndex",
        }
    }
}
