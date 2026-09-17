// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_AGG → PhysicalPlan::Array(ArrayOp::Aggregate) or ClusterArray(Agg).

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::ArrayId;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::ArrayReducerAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{load_entry, map_reducer};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_agg(
    name: &str,
    attr: &str,
    reducer: ArrayReducerAst,
    group_by_dim: Option<&str>,
    temporal: TemporalScope,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let entry = load_entry(name, tenant_id, ctx)?;
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let attr_idx = schema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_AGG: array '{name}' has no attr '{attr}'"),
        })? as u32;

    let group_by_dim_idx: i32 = match group_by_dim {
        None => -1,
        Some(dim) => schema
            .dims
            .iter()
            .position(|d| d.name == dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
            })? as i32,
    };

    let (system_as_of, valid_at_ms) =
        super::helpers::resolve_array_temporal(temporal, "ARRAY_AGG")?;
    let mapped = map_reducer(reducer);
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);

    let plan = if ctx.cluster_enabled {
        // Encode the reducer for the wire. The coordinator decodes it
        // and sends per-shard requests via ArrayShardAggReq.
        let reducer_msgpack =
            zerompk::to_msgpack_vec(&mapped).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array reducer encode: {e}"),
            })?;
        PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id: aid,
            attr_idx,
            reducer_msgpack,
            group_by_dim: group_by_dim_idx,
            slice_hilbert_ranges: vec![],
            prefix_bits: entry.prefix_bits,
            system_as_of,
            valid_at_ms,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: aid,
            attr_idx,
            reducer: mapped,
            group_by_dim: group_by_dim_idx,
            cell_filter: None,
            return_partial: false,
            hilbert_range: None,
            system_as_of,
            valid_at_ms,
        })
    };

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::array_catalog::ArrayCatalogEntry;
    use crate::control::planner::sql_plan_convert::convert::ConvertContext;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn make_ctx(cluster_enabled: bool) -> ConvertContext {
        let schema = ArraySchemaBuilder::new("sales")
            .dim(DimSpec::new(
                "month".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(1), DomainBound::Int64(12)),
            ))
            .attr(AttrSpec::new(
                "revenue".to_string(),
                AttrType::Float64,
                true,
            ))
            .tile_extents(vec![1])
            .build()
            .expect("build");
        let schema_bytes = zerompk::to_msgpack_vec(&schema).expect("encode");
        let handle = crate::control::array_catalog::ArrayCatalog::handle();
        {
            let mut cat = handle.write().expect("lock");
            cat.register(ArrayCatalogEntry {
                array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "sales"),
                name: "sales".into(),
                schema_msgpack: schema_bytes,
                schema_hash: 0,
                created_at_ms: 0,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            })
            .expect("register");
        }
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: Some(handle),
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled,
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

    #[test]
    fn single_node_emits_local_agg() {
        let ctx = make_ctx(false);
        let tasks = convert_agg(
            "sales",
            "revenue",
            ArrayReducerAst::Sum,
            None,
            TemporalScope::default(),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::Array(ArrayOp::Aggregate { .. })
            ),
            "expected local Array variant"
        );
    }

    #[test]
    fn cluster_mode_emits_cluster_agg() {
        let ctx = make_ctx(true);
        let tasks = convert_agg(
            "sales",
            "revenue",
            ArrayReducerAst::Sum,
            None,
            TemporalScope::default(),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::ClusterArray(ClusterArrayOp::Agg { .. })
            ),
            "expected ClusterArray variant"
        );
    }

    #[test]
    fn all_versions_aggregate_is_rejected() {
        // AS OF SYSTEM TIME NULL (AllVersions) is meaningless for an aggregate —
        // summing across every historical version is undefined. The planner must
        // reject it with a typed PlanError rather than silently degrading to a
        // live read. This is why `ArrayOp::Aggregate` / `ClusterArrayOp::Agg`
        // carry `Option<i64>` (Current | AsOf) and not `SystemTimeScope`: the
        // AllVersions state is made unrepresentable by rejecting it here.
        // Audit-log reads go through NDARRAY_SLICE instead.
        let ctx = make_ctx(false);
        let temporal = TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            ..TemporalScope::default()
        };
        let err = convert_agg(
            "sales",
            "revenue",
            ArrayReducerAst::Sum,
            None,
            temporal,
            TenantId::new(1),
            &ctx,
        )
        .expect_err("AllVersions aggregate must be rejected");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("not meaningful")),
            "expected typed PlanError about AllVersions aggregate, got {err:?}"
        );
    }
}
