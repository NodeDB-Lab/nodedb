// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_SLICE → PhysicalPlan::Array(ArrayOp::Slice) or ClusterArray(Slice).

use nodedb_array::types::ArrayId;
use nodedb_sql::ArraySliceVisitArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{decode_schema, load_entry, resolve_attr_indices, resolve_slice};
use super::region::lower_zone;
use super::tile_keys::compute_slice_tile_keys;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_slice(
    args: ArraySliceVisitArgs<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let ArraySliceVisitArgs {
        name,
        slice: slice_ast,
        attr_projection,
        limit,
        zone,
        temporal,
        physical,
    } = args;
    let entry = load_entry(name, tenant_id, ctx)?;
    let schema = decode_schema(&entry)?;

    let mut slice = resolve_slice("ARRAY_SLICE", name, slice_ast, &schema)?;
    // A zone narrows the box before it is encoded, so pruning and the
    // cluster fan-out both see the zone's extent.
    let zone_msgpack = match zone {
        None => Vec::new(),
        Some(zone) => {
            let lowered = lower_zone(
                "ARRAY_SLICE",
                name,
                zone,
                &schema,
                &mut slice,
                &ctx.crs_context(),
            )?;
            zerompk::to_msgpack_vec(&lowered).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array slice zone encode: {e}"),
            })?
        }
    };
    let slice_msgpack =
        zerompk::to_msgpack_vec(&slice).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array slice encode: {e}"),
        })?;

    let (system_time, valid_at_ms) = super::helpers::resolve_array_temporal_scope(*temporal)?;
    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = ctx.collection_key(name).vshard();

    let plan = if ctx.cluster_enabled {
        // In cluster mode emit a ClusterArray variant. The routing loop
        // intercepts it and calls ArrayCoordinator::coord_slice instead
        // of sending to the local SPSC bridge.
        //
        // The coordinator fans out only to the shards that own a tile the
        // slice covers. An empty list fans out to every shard.
        let slice_tile_keys =
            compute_slice_tile_keys(&schema, &slice.dim_ranges, entry.prefix_bits);
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            slice_tile_keys,
            prefix_bits: entry.prefix_bits,
            system_time,
            valid_at_ms,
            zone_msgpack,
            physical,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            cell_filter: None,
            shard_bucket: None,
            system_time,
            valid_at_ms,
            zone_msgpack,
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
    use nodedb_array::schema::{
        ArraySchema, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType,
    };
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_sql::temporal::TemporalScope;
    use nodedb_sql::types_array::ArraySliceAst;

    fn make_schema() -> (Vec<u8>, ArraySchema) {
        let schema = ArraySchemaBuilder::new("test_arr")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
            ))
            .attr(AttrSpec::new("val".to_string(), AttrType::Float64, true))
            .tile_extents(vec![10])
            .build()
            .expect("schema build");
        let bytes = zerompk::to_msgpack_vec(&schema).expect("encode");
        (bytes, schema)
    }

    fn make_ctx(
        cluster_enabled: bool,
    ) -> (
        ConvertContext,
        crate::control::array_catalog::ArrayCatalogHandle,
    ) {
        use crate::control::array_catalog::ArrayCatalog;
        let handle = ArrayCatalog::handle();
        let (schema_msgpack, _schema) = make_schema();
        let entry = ArrayCatalogEntry {
            array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "test_arr"),
            name: "test_arr".into(),
            schema_msgpack,
            schema_hash: 0,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: nodedb_types::Hlc::ZERO,
            incarnation: nodedb_types::Hlc::ZERO,
        };
        {
            let mut cat = handle.write().expect("write lock");
            cat.register(entry).expect("register");
        }
        let ctx = ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: Some(handle.clone()),
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
        };
        (ctx, handle)
    }

    fn whole_slice(ctx: &ConvertContext) -> Vec<PhysicalTask> {
        let slice_ast = ArraySliceAst { dim_ranges: vec![] };
        convert_slice(
            ArraySliceVisitArgs {
                name: "test_arr",
                slice: &slice_ast,
                attr_projection: &[],
                limit: 100,
                zone: None,
                temporal: &TemporalScope::default(),
                physical: false,
            },
            TenantId::new(1),
            ctx,
        )
        .expect("convert")
    }

    #[test]
    fn single_node_emits_local_array() {
        let (ctx, _handle) = make_ctx(false);
        let tasks = whole_slice(&ctx);
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(&tasks[0].plan, PhysicalPlan::Array(ArrayOp::Slice { .. })),
            "expected local Array variant, got: {:?}",
            tasks[0].plan
        );
    }

    #[test]
    fn cluster_mode_emits_cluster_array() {
        let (ctx, _handle) = make_ctx(true);
        let tasks = whole_slice(&ctx);
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::ClusterArray(ClusterArrayOp::Slice { .. })
            ),
            "expected ClusterArray variant, got: {:?}",
            tasks[0].plan
        );
    }
}
