// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_SLICE → PhysicalPlan::Array(ArrayOp::Slice) or ClusterArray(Slice).

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::DimSpec;
use nodedb_array::tile::{tile_prefix, tiles_per_dim};
use nodedb_array::types::ArrayId;
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::ArraySliceAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{coerce_bound, load_entry, resolve_attr_indices};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_slice(
    name: &str,
    slice_ast: &ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
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

    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &slice_ast.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("ARRAY_SLICE: array '{name}' has no dim '{}'", r.dim),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = coerce_bound(&r.lo, dtype, &r.dim)?;
        let hi = coerce_bound(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(lo, hi));
    }
    let slice = Slice::new(dim_ranges);
    let slice_msgpack =
        zerompk::to_msgpack_vec(&slice).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array slice encode: {e}"),
        })?;

    let (system_time, valid_at_ms) =
        super::helpers::resolve_array_temporal_scope(temporal, "ARRAY_SLICE")?;
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
        let slice_hilbert_ranges =
            compute_slice_hilbert_ranges(&schema, &slice.dim_ranges, entry.prefix_bits);
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            slice_hilbert_ranges,
            prefix_bits: entry.prefix_bits,
            system_time,
            valid_at_ms,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            cell_filter: None,
            hilbert_range: None,
            system_time,
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

/// The most tiles a slice's shard fan-out enumerates. A slice covering more
/// tiles fans out to every shard.
const MAX_FAN_OUT_TILES: u128 = 4096;

/// The Hilbert ranges a slice's shard fan-out covers: one `(key, key)` pair
/// per routing bucket, each holding the key of one tile in that bucket.
///
/// Every tile whose cells can match the slice is enumerated, so every shard
/// that holds such a cell, or will hold one a later write lands there, is
/// covered. A transaction's read of the slice then validates on each of
/// those shards. A cell routes by its tile's key (`cell_tile_prefix`), the
/// same key enumerated here.
///
/// Returns an empty vec, which fans out to every shard, when the slice
/// covers more than [`MAX_FAN_OUT_TILES`] tiles, when the schema has no
/// dimension, or when a tile key fails to encode.
fn compute_slice_hilbert_ranges(
    schema: &ArraySchema,
    dim_ranges: &[Option<DimRange>],
    prefix_bits: u8,
) -> Vec<(u64, u64)> {
    if schema.dims.is_empty() || prefix_bits == 0 || prefix_bits > 16 {
        return Vec::new();
    }
    let spans: Vec<(u64, u64)> = schema
        .dims
        .iter()
        .zip(schema.tile_extents.iter())
        .enumerate()
        .map(|(i, (dim, extent))| {
            dim_tile_span(dim, *extent, dim_ranges.get(i).and_then(Option::as_ref))
        })
        .collect();
    let tiles: u128 = spans
        .iter()
        .map(|(lo, hi)| u128::from(hi - lo) + 1)
        .fold(1u128, |acc, n| acc.saturating_mul(n));
    if tiles > MAX_FAN_OUT_TILES {
        return Vec::new();
    }

    let shift = 64 - u32::from(prefix_bits);
    let mut buckets: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    let mut indices: Vec<u64> = spans.iter().map(|(lo, _)| *lo).collect();
    loop {
        let Ok(key) = tile_prefix(schema, &indices) else {
            return Vec::new();
        };
        buckets.entry(key >> shift).or_insert(key);
        // Advance the odometer over the per-dimension tile spans.
        let mut dim = 0;
        loop {
            if dim == indices.len() {
                return buckets.into_values().map(|key| (key, key)).collect();
            }
            if indices[dim] < spans[dim].1 {
                indices[dim] += 1;
                break;
            }
            indices[dim] = spans[dim].0;
            dim += 1;
        }
    }
}

/// The inclusive tile index span along `dim` that `range` covers, the whole
/// dimension when `range` is `None`. A string dimension tiles by hash, so a
/// range on it covers every tile. A bound outside the domain is clamped to
/// the domain's tiles.
fn dim_tile_span(dim: &DimSpec, extent: u64, range: Option<&DimRange>) -> (u64, u64) {
    let last = u64::try_from(tiles_per_dim(dim, extent).saturating_sub(1)).unwrap_or(u64::MAX);
    let extent = u128::from(extent.max(1));
    let index = |bound: &DomainBound| -> Option<u64> {
        let offset: u128 = match (bound, &dim.domain.lo) {
            (DomainBound::Int64(v), DomainBound::Int64(lo))
            | (DomainBound::TimestampMs(v), DomainBound::TimestampMs(lo)) => {
                ((*v as i128) - (*lo as i128)).max(0) as u128
            }
            (DomainBound::Float64(v), DomainBound::Float64(lo)) if v.is_finite() => {
                (v - lo).max(0.0) as u128
            }
            _ => return None,
        };
        Some(u64::try_from(offset / extent).unwrap_or(u64::MAX).min(last))
    };
    match range.map(|r| (index(&r.lo), index(&r.hi))) {
        Some((Some(lo), Some(hi))) => (lo.min(hi), lo.max(hi)),
        _ => (0, last),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::array_catalog::ArrayCatalogEntry;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

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

    /// Every cell a slice can match, or a later write can land in its box,
    /// routes to a shard the slice fans out to.
    #[test]
    fn the_fan_out_covers_the_shard_of_every_cell_in_the_box() {
        use nodedb_array::types::coord::value::CoordValue;
        let schema = ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "chr".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(9)),
            ))
            .dim(DimSpec::new(
                "pos".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .attr(AttrSpec::new("qual".to_string(), AttrType::Float64, true))
            .tile_extents(vec![1, 100])
            .build()
            .expect("schema build");
        let box_ranges = vec![
            Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(2))),
            Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(99))),
        ];
        let ranges = compute_slice_hilbert_ranges(&schema, &box_ranges, 8);
        assert!(!ranges.is_empty(), "a three-tile slice fans out by tile");
        let fanned = nodedb_cluster::distributed_array::array_vshards_for_slice(&ranges, 8, 1024)
            .expect("fan-out");
        let mut owners = std::collections::BTreeSet::new();
        for chr in 0..=2 {
            for pos in [0, 10, 20, 50, 99] {
                let coord = [CoordValue::Int64(chr), CoordValue::Int64(pos)];
                let key = nodedb_array::tile::cell_tile_prefix(&schema, &coord).expect("route");
                let owner = nodedb_cluster::distributed_array::array_vshard_for_tile(key, 8)
                    .expect("owner");
                assert!(
                    fanned.contains(&owner),
                    "cell ({chr}, {pos}) routes to vShard {owner}, outside the fan-out {fanned:?}"
                );
                owners.insert(owner);
            }
        }
        assert!(owners.len() > 1, "the slice's tiles spread over shards");
    }

    #[test]
    fn a_slice_of_more_tiles_than_the_cap_fans_out_to_every_shard() {
        let (_, schema) = make_schema();
        let wide = ArraySchemaBuilder::new("wide")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("val".to_string(), AttrType::Float64, true))
            .tile_extents(vec![1])
            .build()
            .expect("schema build");
        assert!(compute_slice_hilbert_ranges(&wide, &[None], 8).is_empty());
        assert!(!compute_slice_hilbert_ranges(&schema, &[None], 8).is_empty());
    }

    #[test]
    fn single_node_emits_local_array() {
        let (ctx, _handle) = make_ctx(false);
        let slice_ast = ArraySliceAst { dim_ranges: vec![] };
        let tasks = convert_slice(
            "test_arr",
            &slice_ast,
            &[],
            100,
            TemporalScope::default(),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
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
        let slice_ast = ArraySliceAst { dim_ranges: vec![] };
        let tasks = convert_slice(
            "test_arr",
            &slice_ast,
            &[],
            100,
            TemporalScope::default(),
            TenantId::new(1),
            &ctx,
        )
        .expect("convert");
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
