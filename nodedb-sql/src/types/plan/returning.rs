// SPDX-License-Identifier: Apache-2.0

//! Resolved write-response projection retained by each DML plan.

use crate::types::query::Projection;
use nodedb_types::ReturningSpec;

/// A write response's complete output list and required stored columns.
#[derive(Debug, Clone)]
pub struct ResolvedReturning {
    pub projection: Vec<Projection>,
    pub spec: ReturningSpec,
}

use super::variants::*;

impl SqlPlan {
    /// The response clause owned by this write plan.
    pub fn returning(&self) -> Option<&ResolvedReturning> {
        self.returning_slot().and_then(Option::as_ref)
    }

    /// The response slot and target collection of this write plan.
    pub fn returning_target(&self) -> Option<&str> {
        match self {
            Self::Insert(InsertPlan { collection, .. }) => Some(collection),
            Self::KvInsert(KvInsertPlan { collection, .. }) => Some(collection),
            Self::Upsert(UpsertPlan { collection, .. }) => Some(collection),
            Self::TimeseriesIngest(TimeseriesIngestPlan { collection, .. }) => Some(collection),
            Self::VectorPrimaryInsert(VectorPrimaryInsertPlan { collection, .. }) => {
                Some(collection)
            }
            Self::VectorPrimaryDelete(VectorPrimaryDeletePlan { collection, .. }) => {
                Some(collection)
            }
            Self::VectorPrimaryUpdate(VectorPrimaryUpdatePlan { collection, .. }) => {
                Some(collection)
            }
            Self::Merge(MergePlan { target, .. }) => Some(target),
            Self::Update { collection, .. } => Some(collection),
            Self::InsertSelect { target, .. } => Some(target),
            Self::UpdateFrom { collection, .. } => Some(collection),
            Self::Delete { collection, .. } => Some(collection),
            Self::ConstantResult { .. }
            | Self::Scan { .. }
            | Self::PointGet { .. }
            | Self::DocumentIndexLookup { .. }
            | Self::RangeScan { .. }
            | Self::Truncate { .. }
            | Self::Join { .. }
            | Self::Aggregate { .. }
            | Self::TimeseriesScan { .. }
            | Self::VectorSearch { .. }
            | Self::MultiVectorSearch { .. }
            | Self::SparseSearch { .. }
            | Self::TextSearch { .. }
            | Self::HybridSearch { .. }
            | Self::HybridSearchTriple { .. }
            | Self::SpatialScan { .. }
            | Self::Union { .. }
            | Self::Intersect { .. }
            | Self::Except { .. }
            | Self::RecursiveScan { .. }
            | Self::RecursiveValue { .. }
            | Self::Cte { .. }
            | Self::Subquery { .. }
            | Self::CreateArray { .. }
            | Self::DropArray { .. }
            | Self::AlterArray { .. }
            | Self::InsertArray { .. }
            | Self::DeleteArray { .. }
            | Self::ArraySlice { .. }
            | Self::ArrayAgg { .. }
            | Self::ArrayElementwise { .. }
            | Self::ArrayFlush { .. }
            | Self::ArrayCompact { .. }
            | Self::LateralTopK { .. }
            | Self::LateralLoop { .. }
            | Self::VectorPrimaryTruncate { .. }
            | Self::CreateIndex { .. }
            | Self::DropIndex { .. } => None,
        }
    }

    pub fn returning_slot(&self) -> Option<&Option<ResolvedReturning>> {
        match self {
            Self::Insert(InsertPlan { returning, .. }) => Some(returning),
            Self::KvInsert(KvInsertPlan { returning, .. }) => Some(returning),
            Self::Upsert(UpsertPlan { returning, .. }) => Some(returning),
            Self::TimeseriesIngest(TimeseriesIngestPlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryInsert(VectorPrimaryInsertPlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryDelete(VectorPrimaryDeletePlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryUpdate(VectorPrimaryUpdatePlan { returning, .. }) => Some(returning),
            Self::Merge(MergePlan { returning, .. }) => Some(returning),
            Self::Update { returning, .. } => Some(returning),
            Self::InsertSelect { returning, .. } => Some(returning),
            Self::UpdateFrom { returning, .. } => Some(returning),
            Self::Delete { returning, .. } => Some(returning),
            Self::ConstantResult { .. }
            | Self::Scan { .. }
            | Self::PointGet { .. }
            | Self::DocumentIndexLookup { .. }
            | Self::RangeScan { .. }
            | Self::Truncate { .. }
            | Self::Join { .. }
            | Self::Aggregate { .. }
            | Self::TimeseriesScan { .. }
            | Self::VectorSearch { .. }
            | Self::MultiVectorSearch { .. }
            | Self::SparseSearch { .. }
            | Self::TextSearch { .. }
            | Self::HybridSearch { .. }
            | Self::HybridSearchTriple { .. }
            | Self::SpatialScan { .. }
            | Self::Union { .. }
            | Self::Intersect { .. }
            | Self::Except { .. }
            | Self::RecursiveScan { .. }
            | Self::RecursiveValue { .. }
            | Self::Cte { .. }
            | Self::Subquery { .. }
            | Self::CreateArray { .. }
            | Self::DropArray { .. }
            | Self::AlterArray { .. }
            | Self::InsertArray { .. }
            | Self::DeleteArray { .. }
            | Self::ArraySlice { .. }
            | Self::ArrayAgg { .. }
            | Self::ArrayElementwise { .. }
            | Self::ArrayFlush { .. }
            | Self::ArrayCompact { .. }
            | Self::LateralTopK { .. }
            | Self::LateralLoop { .. }
            | Self::VectorPrimaryTruncate { .. }
            | Self::CreateIndex { .. }
            | Self::DropIndex { .. } => None,
        }
    }

    pub fn returning_slot_mut(&mut self) -> Option<&mut Option<ResolvedReturning>> {
        match self {
            Self::Insert(InsertPlan { returning, .. }) => Some(returning),
            Self::KvInsert(KvInsertPlan { returning, .. }) => Some(returning),
            Self::Upsert(UpsertPlan { returning, .. }) => Some(returning),
            Self::TimeseriesIngest(TimeseriesIngestPlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryInsert(VectorPrimaryInsertPlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryDelete(VectorPrimaryDeletePlan { returning, .. }) => Some(returning),
            Self::VectorPrimaryUpdate(VectorPrimaryUpdatePlan { returning, .. }) => Some(returning),
            Self::Merge(MergePlan { returning, .. }) => Some(returning),
            Self::Update { returning, .. } => Some(returning),
            Self::InsertSelect { returning, .. } => Some(returning),
            Self::UpdateFrom { returning, .. } => Some(returning),
            Self::Delete { returning, .. } => Some(returning),
            Self::ConstantResult { .. }
            | Self::Scan { .. }
            | Self::PointGet { .. }
            | Self::DocumentIndexLookup { .. }
            | Self::RangeScan { .. }
            | Self::Truncate { .. }
            | Self::Join { .. }
            | Self::Aggregate { .. }
            | Self::TimeseriesScan { .. }
            | Self::VectorSearch { .. }
            | Self::MultiVectorSearch { .. }
            | Self::SparseSearch { .. }
            | Self::TextSearch { .. }
            | Self::HybridSearch { .. }
            | Self::HybridSearchTriple { .. }
            | Self::SpatialScan { .. }
            | Self::Union { .. }
            | Self::Intersect { .. }
            | Self::Except { .. }
            | Self::RecursiveScan { .. }
            | Self::RecursiveValue { .. }
            | Self::Cte { .. }
            | Self::Subquery { .. }
            | Self::CreateArray { .. }
            | Self::DropArray { .. }
            | Self::AlterArray { .. }
            | Self::InsertArray { .. }
            | Self::DeleteArray { .. }
            | Self::ArraySlice { .. }
            | Self::ArrayAgg { .. }
            | Self::ArrayElementwise { .. }
            | Self::ArrayFlush { .. }
            | Self::ArrayCompact { .. }
            | Self::LateralTopK { .. }
            | Self::LateralLoop { .. }
            | Self::VectorPrimaryTruncate { .. }
            | Self::CreateIndex { .. }
            | Self::DropIndex { .. } => None,
        }
    }
}
