// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_AGG → PhysicalPlan::Array(ArrayOp::Aggregate) or ClusterArray(Agg).

use nodedb_array::types::ArrayId;
use nodedb_sql::ArrayAggVisitArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{decode_schema, load_entry, map_reducer};
use super::region::resolve_region;
use super::tile_keys::compute_slice_tile_keys;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_agg(
    args: ArrayAggVisitArgs<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let ArrayAggVisitArgs {
        name,
        attr,
        reducer,
        group_by_dim,
        slice,
        zone,
        temporal,
        physical,
    } = args;
    let entry = load_entry(name, tenant_id, ctx)?;
    let schema = decode_schema(&entry)?;

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

    let (system_time, valid_at_ms) =
        super::helpers::resolve_array_temporal(*temporal, "ARRAY_AGG")?;
    let region = resolve_region(name, slice, zone, &schema, &ctx.crs_context())?;
    let region_msgpack = match &region {
        Some(region) => {
            zerompk::to_msgpack_vec(region).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array region encode: {e}"),
            })?
        }
        None => Vec::new(),
    };
    let mapped = map_reducer(reducer);
    // A malformed histogram spec is refused here, before any shard runs it.
    mapped.engine().map_err(|e| crate::Error::PlanError {
        detail: format!("ARRAY_AGG: {e}"),
    })?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = ctx.collection_key(name).vshard();

    let plan = if ctx.cluster_enabled {
        // Encode the reducer for the wire. The coordinator decodes it
        // and sends per-shard requests via ArrayShardAggReq.
        let reducer_msgpack =
            zerompk::to_msgpack_vec(&mapped).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array reducer encode: {e}"),
            })?;
        // The fan-out covers the shards that own a tile of the region's box.
        let box_ranges = region
            .as_ref()
            .map(|r| r.slice.dim_ranges.as_slice())
            .unwrap_or_default();
        PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id: aid,
            attr_idx,
            reducer_msgpack,
            group_by_dim: group_by_dim_idx,
            slice_tile_keys: compute_slice_tile_keys(&schema, box_ranges, entry.prefix_bits),
            prefix_bits: entry.prefix_bits,
            system_time,
            valid_at_ms,
            region_msgpack,
            physical,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: aid,
            attr_idx,
            reducer: mapped,
            group_by_dim: group_by_dim_idx,
            cell_filter: None,
            return_partial: false,
            shard_bucket: None,
            system_time,
            valid_at_ms,
            region_msgpack,
            physical,
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
    use nodedb_array::query::region::ArrayRegion;
    use nodedb_array::query::slice::DimRange;
    use nodedb_sql::temporal::TemporalScope;
    use nodedb_sql::types_array::{
        ArrayCoordLiteral, ArrayReducerAst, ArraySliceAst, NamedDimRange,
    };

    /// `SUM(revenue)` over `sales` within `slice`.
    fn sum_args<'a>(
        slice: &'a ArraySliceAst,
        temporal: &'a TemporalScope,
    ) -> ArrayAggVisitArgs<'a> {
        ArrayAggVisitArgs {
            name: "sales",
            attr: "revenue",
            reducer: ArrayReducerAst::Sum,
            group_by_dim: None,
            slice,
            zone: None,
            temporal,
            physical: false,
        }
    }
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
                modification_hlc: nodedb_types::Hlc::ZERO,
                incarnation: nodedb_types::Hlc::ZERO,
            })
            .expect("register");
        }
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: Some(handle),
            credentials: None,
            wal: None,
            surrogate_assigner:
                crate::control::planner::sql_plan_convert::test_support::test_assigner(),
            cluster_enabled,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            prefetched: Default::default(),
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    #[test]
    fn single_node_emits_local_agg() {
        let ctx = make_ctx(false);
        let tasks = convert_agg(
            sum_args(&ArraySliceAst::default(), &TemporalScope::default()),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Array(ArrayOp::Aggregate { region_msgpack, .. }) => {
                assert!(region_msgpack.is_empty(), "no region reads the whole array");
            }
            other => panic!("expected local Array variant, got {other:?}"),
        }
    }

    #[test]
    fn cluster_mode_emits_cluster_agg() {
        let ctx = make_ctx(true);
        let tasks = convert_agg(
            sum_args(&ArraySliceAst::default(), &TemporalScope::default()),
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

    /// A box rides to every shard as the encoded region, and the fan-out
    /// covers only the tiles inside it.
    #[test]
    fn a_box_encodes_the_region_and_narrows_the_fan_out() {
        let ctx = make_ctx(true);
        let slice = ArraySliceAst {
            dim_ranges: vec![NamedDimRange {
                dim: "month".into(),
                lo: ArrayCoordLiteral::Int64(3),
                hi: ArrayCoordLiteral::Int64(4),
            }],
        };
        let tasks = convert_agg(
            sum_args(&slice, &TemporalScope::default()),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
        let PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            region_msgpack,
            slice_tile_keys,
            ..
        }) = &tasks[0].plan
        else {
            panic!("expected ClusterArray variant, got {:?}", tasks[0].plan);
        };
        let region: ArrayRegion = zerompk::from_msgpack(region_msgpack).expect("region decodes");
        assert_eq!(
            region.slice.dim_ranges,
            vec![Some(DimRange::new(
                DomainBound::Int64(3),
                DomainBound::Int64(4)
            ))]
        );
        assert!(region.zone.is_none());
        assert!(
            (1..=2).contains(&slice_tile_keys.len()),
            "two one-month tiles fan out to at most two buckets: {slice_tile_keys:?}"
        );
    }

    #[test]
    fn a_box_on_an_unknown_dim_is_refused() {
        let ctx = make_ctx(false);
        let slice = ArraySliceAst {
            dim_ranges: vec![NamedDimRange {
                dim: "year".into(),
                lo: ArrayCoordLiteral::Int64(1),
                hi: ArrayCoordLiteral::Int64(2),
            }],
        };
        let err = convert_agg(
            sum_args(&slice, &TemporalScope::default()),
            TenantId::new(1),
            &ctx,
        )
        .expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("no dim 'year'")),
            "{err:?}"
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
        // Audit-log reads go through ARRAY_SLICE instead.
        let ctx = make_ctx(false);
        let temporal = TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            ..TemporalScope::default()
        };
        let err = convert_agg(
            sum_args(&ArraySliceAst::default(), &temporal),
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
