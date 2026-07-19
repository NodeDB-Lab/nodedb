// SPDX-License-Identifier: Apache-2.0

use crate::types::query::EngineType;

use super::SqlPlan;

/// Whether a logical plan may be lowered once and reused from the physical-plan cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheEligibility {
    /// Lowering depends only on schema/catalog descriptors tracked by the cache.
    Cacheable,
    /// Lowering consults mutable row identity and must run for every execution.
    DataDependent,
}

impl PlanCacheEligibility {
    /// Whether the lowered physical tasks may be admitted to the plan cache.
    pub fn is_cacheable(self) -> bool {
        self == Self::Cacheable
    }

    fn combine(self, other: Self) -> Self {
        if self == Self::DataDependent || other == Self::DataDependent {
            Self::DataDependent
        } else {
            Self::Cacheable
        }
    }
}

impl SqlPlan {
    /// Classify dependencies that are not represented by descriptor versions.
    ///
    /// Document point operations resolve primary-key bytes to a surrogate while
    /// lowering. That binding can appear after an earlier miss without any
    /// schema-version change, so those physical tasks cannot be cached.
    pub fn cache_eligibility(&self) -> PlanCacheEligibility {
        use PlanCacheEligibility::{Cacheable, DataDependent};

        match self {
            Self::PointGet {
                engine: EngineType::DocumentSchemaless | EngineType::DocumentStrict,
                ..
            } => DataDependent,
            Self::Update {
                engine,
                target_keys,
                ..
            }
            | Self::Delete {
                engine,
                target_keys,
                ..
            } if !target_keys.is_empty()
                && matches!(
                    engine,
                    EngineType::DocumentSchemaless | EngineType::DocumentStrict
                ) =>
            {
                DataDependent
            }
            Self::InsertSelect { source, .. }
            | Self::UpdateFrom { source, .. }
            | Self::Aggregate { input: source, .. }
            | Self::Merge { source, .. } => source.cache_eligibility(),
            Self::Join { left, right, .. }
            | Self::Intersect { left, right, .. }
            | Self::Except { left, right, .. } => {
                left.cache_eligibility().combine(right.cache_eligibility())
            }
            Self::Union { inputs, .. } => inputs.iter().fold(Cacheable, |eligibility, input| {
                eligibility.combine(input.cache_eligibility())
            }),
            Self::Cte { definitions, outer } => definitions
                .iter()
                .fold(outer.cache_eligibility(), |eligibility, (_, plan)| {
                    eligibility.combine(plan.cache_eligibility())
                }),
            Self::LateralTopK { outer, .. } => outer.cache_eligibility(),
            Self::LateralLoop { outer, inner, .. } => {
                outer.cache_eligibility().combine(inner.cache_eligibility())
            }
            Self::ConstantResult { .. }
            | Self::Scan { .. }
            | Self::PointGet { .. }
            | Self::DocumentIndexLookup { .. }
            | Self::RangeScan { .. }
            | Self::Insert { .. }
            | Self::KvInsert { .. }
            | Self::Upsert { .. }
            | Self::Update { .. }
            | Self::Delete { .. }
            | Self::Truncate { .. }
            | Self::TimeseriesScan { .. }
            | Self::TimeseriesIngest { .. }
            | Self::VectorSearch { .. }
            | Self::MultiVectorSearch { .. }
            | Self::SparseSearch { .. }
            | Self::TextSearch { .. }
            | Self::HybridSearch { .. }
            | Self::HybridSearchTriple { .. }
            | Self::SpatialScan { .. }
            | Self::RecursiveScan { .. }
            | Self::RecursiveValue { .. }
            | Self::CreateArray { .. }
            | Self::DropArray { .. }
            | Self::AlterArray { .. }
            | Self::InsertArray { .. }
            | Self::DeleteArray { .. }
            | Self::ArraySlice { .. }
            | Self::ArrayProject { .. }
            | Self::ArrayAgg { .. }
            | Self::ArrayElementwise { .. }
            | Self::ArrayFlush { .. }
            | Self::ArrayCompact { .. }
            | Self::VectorPrimaryInsert { .. }
            | Self::CreateIndex { .. }
            | Self::DropIndex { .. } => Cacheable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types_expr::SqlValue;

    fn point_get(engine: EngineType) -> SqlPlan {
        SqlPlan::PointGet {
            collection: "docs".into(),
            alias: None,
            engine,
            key_column: "id".into(),
            key_value: SqlValue::String("k".into()),
            projection: Vec::new(),
        }
    }

    fn update(engine: EngineType, target_keys: Vec<SqlValue>) -> SqlPlan {
        SqlPlan::Update {
            collection: "docs".into(),
            engine,
            assignments: Vec::new(),
            filters: Vec::new(),
            target_keys,
            returning: false,
        }
    }

    fn delete(engine: EngineType, target_keys: Vec<SqlValue>) -> SqlPlan {
        SqlPlan::Delete {
            collection: "docs".into(),
            engine,
            filters: Vec::new(),
            target_keys,
        }
    }

    #[test]
    fn document_point_get_is_data_dependent() {
        assert_eq!(
            point_get(EngineType::DocumentStrict).cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }

    #[test]
    fn key_value_point_get_is_cacheable() {
        assert_eq!(
            point_get(EngineType::KeyValue).cache_eligibility(),
            PlanCacheEligibility::Cacheable
        );
    }

    #[test]
    fn document_point_update_is_data_dependent() {
        assert_eq!(
            update(
                EngineType::DocumentSchemaless,
                vec![SqlValue::String("k".into())]
            )
            .cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }

    #[test]
    fn document_predicate_update_is_cacheable() {
        assert_eq!(
            update(EngineType::DocumentStrict, Vec::new()).cache_eligibility(),
            PlanCacheEligibility::Cacheable
        );
    }

    #[test]
    fn document_point_delete_is_data_dependent() {
        assert_eq!(
            delete(
                EngineType::DocumentStrict,
                vec![SqlValue::String("k".into())]
            )
            .cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }

    #[test]
    fn nested_point_dependency_propagates() {
        let plan = SqlPlan::Cte {
            definitions: vec![("selected".into(), point_get(EngineType::DocumentStrict))],
            outer: Box::new(point_get(EngineType::KeyValue)),
        };
        assert_eq!(
            plan.cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }
}
