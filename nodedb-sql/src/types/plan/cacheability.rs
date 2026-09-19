// SPDX-License-Identifier: Apache-2.0

use crate::types::query::EngineType;

use super::SqlPlan;
use super::expr_scan::projection_is_cp_computed;

/// Whether a logical plan may be lowered once and reused from the physical-plan cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheEligibility {
    /// Lowering depends only on schema/catalog descriptors tracked by the cache.
    Cacheable,
    /// Lowering consults mutable row identity, or the plan holds a volatile
    /// expression, and must run for every execution.
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
    ///
    /// A plan holding a volatile call is `DataDependent` for the same reason:
    /// the call was evaluated while the plan was built, so caching it would
    /// replay one execution's value into every later one.
    pub fn cache_eligibility(&self) -> PlanCacheEligibility {
        use PlanCacheEligibility::{Cacheable, DataDependent};

        match self {
            Self::ConstantResult { volatile: true, .. } => DataDependent,
            // A Control-Plane-computed projection entry holds a sequence
            // accessor, which allocates per execution: the lowered tasks
            // must not replay one execution's rows for another.
            Self::Scan { projection, .. }
            | Self::PointGet { projection, .. }
            | Self::DocumentIndexLookup { projection, .. }
            | Self::RangeScan { projection, .. }
            | Self::Join { projection, .. }
            | Self::TimeseriesScan { projection, .. }
            | Self::VectorSearch { projection, .. }
            | Self::MultiVectorSearch { projection, .. }
            | Self::SparseSearch { projection, .. }
            | Self::TextSearch { projection, .. }
            | Self::HybridSearch { projection, .. }
            | Self::HybridSearchTriple { projection, .. }
            | Self::SpatialScan { projection, .. }
            | Self::RecursiveScan { projection, .. }
            | Self::Subquery { projection, .. }
            | Self::LateralTopK { projection, .. }
            | Self::LateralLoop { projection, .. }
                if projection_is_cp_computed(projection) =>
            {
                DataDependent
            }
            Self::Insert {
                volatile_defaults: true,
                ..
            }
            | Self::Upsert {
                volatile_defaults: true,
                ..
            }
            | Self::TimeseriesIngest {
                volatile_defaults: true,
                ..
            }
            | Self::KvInsert {
                volatile_defaults: true,
                ..
            }
            | Self::VectorPrimaryInsert {
                volatile_defaults: true,
                ..
            } => DataDependent,
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
            // A point-key delete or update binds its surrogates while the plan
            // is lowered, from the catalog state of that moment.
            Self::VectorPrimaryDelete { target_keys, .. }
            | Self::VectorPrimaryUpdate { target_keys, .. }
                if !target_keys.is_empty() =>
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
            Self::Subquery { input, .. } => input.cache_eligibility(),
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
            | Self::VectorPrimaryDelete { .. }
            | Self::VectorPrimaryUpdate { .. }
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
    fn cp_computed_projection_is_data_dependent() {
        use crate::types::query::Projection;
        use crate::types_expr::SqlExpr;
        let cp = Projection::CpComputed {
            expr: SqlExpr::Function {
                name: "nextval".into(),
                args: vec![SqlExpr::Literal(SqlValue::String("s".into()))],
                distinct: false,
            },
            alias: "nextval".into(),
        };
        let scan = |projection: Vec<Projection>| SqlPlan::Scan {
            collection: "docs".into(),
            alias: None,
            engine: EngineType::KeyValue,
            filters: Vec::new(),
            projection,
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::temporal::TemporalScope::default(),
        };
        assert_eq!(
            scan(vec![Projection::Column("id".into()), cp.clone()]).cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
        assert_eq!(
            scan(vec![Projection::Column("id".into())]).cache_eligibility(),
            PlanCacheEligibility::Cacheable
        );
        let wrapped = SqlPlan::Subquery {
            input: Box::new(scan(Vec::new())),
            filters: Vec::new(),
            projection: vec![cp],
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        };
        assert_eq!(
            wrapped.cache_eligibility(),
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
