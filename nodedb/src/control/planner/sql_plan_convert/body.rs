// SPDX-License-Identifier: BUSL-1.1

//! Derived-table body lowering: one `SqlPlan` body to ONE physical relation,
//! for a post-processor or an input-sourced aggregate.

use nodedb_sql::types::SqlPlan;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, QueryOp, SetOpKind};

use super::convert::{ConvertContext, convert_one};

/// Lower a derived-table body to ONE physical relation for a post-processor
/// or an input-sourced aggregate.
///
/// A set-operation body lowers to a coordinator-resolved `QueryOp::SetOp`
/// whose branches recurse through this function, so nested set operations
/// (`(a UNION b) INTERSECT c`) stay one relation. Every other body lowers
/// through `convert_one` and must yield exactly one task.
///
/// A sharded relation (the body itself, or any set-operation branch) is
/// wrapped in `Exchange{Gather}` so its gather runs exactly once over the
/// full union before the enclosing tail or merge observes it. `SetOp` is
/// coordinator-local, so it is never wrapped itself.
pub(super) fn convert_body_to_single_plan(
    input: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<PhysicalPlan> {
    match input {
        SqlPlan::Union { inputs, distinct } => {
            let op = if *distinct {
                SetOpKind::UnionDistinct
            } else {
                SetOpKind::UnionAll
            };
            let inputs = inputs
                .iter()
                .map(|branch| convert_body_to_single_plan(branch, tenant_id, ctx))
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(PhysicalPlan::Query(QueryOp::SetOp { inputs, op }))
        }
        SqlPlan::Intersect { left, right, all } => {
            let op = if *all {
                SetOpKind::IntersectAll
            } else {
                SetOpKind::Intersect
            };
            convert_binary_set_op(left, right, op, tenant_id, ctx)
        }
        SqlPlan::Except { left, right, all } => {
            let op = if *all {
                SetOpKind::ExceptAll
            } else {
                SetOpKind::Except
            };
            convert_binary_set_op(left, right, op, tenant_id, ctx)
        }
        // Every other body lowers through the ordinary converter. Listed in
        // full so a new `SqlPlan` variant forces a decision here.
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Scan { .. }
        | SqlPlan::PointGet { .. }
        | SqlPlan::DocumentIndexLookup { .. }
        | SqlPlan::RangeScan { .. }
        | SqlPlan::Insert { .. }
        | SqlPlan::KvInsert { .. }
        | SqlPlan::Upsert { .. }
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::VectorPrimaryTruncate { .. }
        | SqlPlan::Join { .. }
        | SqlPlan::Aggregate { .. }
        | SqlPlan::TimeseriesScan { .. }
        | SqlPlan::TimeseriesIngest { .. }
        | SqlPlan::VectorSearch { .. }
        | SqlPlan::MultiVectorSearch { .. }
        | SqlPlan::SparseSearch { .. }
        | SqlPlan::TextSearch { .. }
        | SqlPlan::HybridSearch { .. }
        | SqlPlan::HybridSearchTriple { .. }
        | SqlPlan::SpatialScan { .. }
        | SqlPlan::RecursiveScan { .. }
        | SqlPlan::RecursiveValue { .. }
        | SqlPlan::Cte { .. }
        | SqlPlan::Subquery { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::ArraySlice { .. }
        | SqlPlan::ArrayProject { .. }
        | SqlPlan::ArrayAgg { .. }
        | SqlPlan::ArrayElementwise { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::Merge { .. }
        | SqlPlan::LateralTopK { .. }
        | SqlPlan::LateralLoop { .. }
        | SqlPlan::VectorPrimaryInsert { .. }
        | SqlPlan::VectorPrimaryDelete { .. }
        | SqlPlan::VectorPrimaryUpdate { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. } => {
            let mut tasks = convert_one(input, tenant_id, ctx)?;
            let plan = match (tasks.len(), tasks.pop()) {
                (1, Some(task)) => task.plan,
                (n, _) => {
                    return Err(crate::Error::PlanError {
                        detail: format!(
                            "derived-table body lowers to {n} physical tasks; the body must \
                             produce a single relation"
                        ),
                    });
                }
            };
            Ok(gather_if_sharded(plan))
        }
    }
}

/// Lower the two sides of `INTERSECT` / `EXCEPT` to a two-branch `SetOp`.
fn convert_binary_set_op(
    left: &SqlPlan,
    right: &SqlPlan,
    op: SetOpKind,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<PhysicalPlan> {
    let inputs = vec![
        convert_body_to_single_plan(left, tenant_id, ctx)?,
        convert_body_to_single_plan(right, tenant_id, ctx)?,
    ];
    Ok(PhysicalPlan::Query(QueryOp::SetOp { inputs, op }))
}

/// Wrap a sharded relation in `Exchange{Gather}` so its gather runs exactly
/// once over the full union. The enclosing node is coordinator-local, so the
/// top-level `convert()` wrap loop does not gather it.
fn gather_if_sharded(plan: PhysicalPlan) -> PhysicalPlan {
    if !plan.is_sharded_source() {
        return plan;
    }
    let as_aggregate = matches!(
        &plan,
        PhysicalPlan::Query(QueryOp::Aggregate { .. })
            | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
    );
    PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
        child: Box::new(plan),
        mode: ExchangeMode::Gather { as_aggregate },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::planner::sql_plan_convert::PlanningPurpose;
    use nodedb_sql::types::SqlValue;

    fn ctx() -> ConvertContext {
        ConvertContext {
            purpose: PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn constant(x: i64) -> SqlPlan {
        SqlPlan::ConstantResult {
            columns: vec!["x".into()],
            values: vec![SqlValue::Int(x)],
            volatile: false,
        }
    }

    #[test]
    fn union_all_body_lowers_to_one_set_op() {
        let body = SqlPlan::Union {
            inputs: vec![constant(1), constant(2)],
            distinct: false,
        };
        let plan = convert_body_to_single_plan(&body, TenantId::new(1), &ctx())
            .expect("union body lowers");
        match plan {
            PhysicalPlan::Query(QueryOp::SetOp { inputs, op }) => {
                assert_eq!(op, SetOpKind::UnionAll);
                assert_eq!(inputs.len(), 2);
                for input in &inputs {
                    assert!(matches!(
                        input,
                        PhysicalPlan::Query(QueryOp::ProviderScan { provider: None, .. })
                    ));
                }
            }
            other => panic!("expected SetOp, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_ops_stay_one_relation() {
        let body = SqlPlan::Intersect {
            left: Box::new(SqlPlan::Union {
                inputs: vec![constant(1), constant(2)],
                distinct: true,
            }),
            right: Box::new(constant(2)),
            all: false,
        };
        let plan = convert_body_to_single_plan(&body, TenantId::new(1), &ctx())
            .expect("nested set-op body lowers");
        let PhysicalPlan::Query(QueryOp::SetOp { inputs, op }) = plan else {
            panic!("expected SetOp");
        };
        assert_eq!(op, SetOpKind::Intersect);
        assert!(matches!(
            &inputs[0],
            PhysicalPlan::Query(QueryOp::SetOp {
                op: SetOpKind::UnionDistinct,
                ..
            })
        ));
        assert!(!PhysicalPlan::Query(QueryOp::SetOp { inputs, op }).is_sharded_source());
    }
}
