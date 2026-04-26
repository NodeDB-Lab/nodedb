//! NDARRAY_SLICE → PhysicalPlan::Array(ArrayOp::Slice) or ClusterArray(Slice).

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::layout::tile_id_for_cell;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_sql::types_array::ArraySliceAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{ArrayOp, ClusterArrayOp};
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::helpers::{coerce_bound, load_entry, resolve_attr_indices};

pub(crate) fn convert_slice(
    name: &str,
    slice_ast: &ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let entry = load_entry(name, ctx)?;
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
                detail: format!("NDARRAY_SLICE: array '{name}' has no dim '{}'", r.dim),
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

    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);

    let plan = if ctx.cluster_enabled {
        // In cluster mode emit a ClusterArray variant. The routing loop
        // intercepts it and calls ArrayCoordinator::coord_slice instead
        // of sending to the local SPSC bridge.
        //
        // Compute a conservative Hilbert bounding range from the slice's
        // dim-range corners so the coordinator only fans out to the shards
        // whose Hilbert buckets overlap the query range, rather than all
        // 1024 shards (the "empty ranges = unbounded" fallback).
        let slice_hilbert_ranges =
            compute_slice_hilbert_ranges(&schema, &slice.dim_ranges, entry.prefix_bits);
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            slice_hilbert_ranges,
            prefix_bits: entry.prefix_bits,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            cell_filter: None,
            hilbert_range: None,
        })
    };

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan,
        post_set_op: PostSetOp::None,
    }])
}

/// Compute a conservative Hilbert bounding range for a slice predicate.
///
/// For each constrained dimension, uses the lo and hi bounds as the range
/// endpoints. Generates all 2^num_dims corner coordinates of the bounding
/// box and computes the Hilbert prefix for each. Returns a single
/// `(min_prefix, max_prefix)` range that conservatively covers all cells
/// that could match the slice.
///
/// Falls back to an empty vec (unbounded = contact all shards) if the
/// schema has more than 16 dimensions (2^16 corners would be excessive)
/// or if any Hilbert encoding fails.
///
/// The result may over-estimate the shard set — the shard-side slice
/// filter is always applied and never produces false positives.
fn compute_slice_hilbert_ranges(
    schema: &ArraySchema,
    dim_ranges: &[Option<DimRange>],
    _prefix_bits: u8,
) -> Vec<(u64, u64)> {
    use nodedb_array::types::domain::DomainBound;

    let ndim = schema.dims.len();
    if ndim == 0 || ndim > 16 {
        return Vec::new(); // fallback: unbounded
    }

    // Build lo/hi bound for each dim. Unconstrained dims use the schema domain.
    let mut lo_vals: Vec<CoordValue> = Vec::with_capacity(ndim);
    let mut hi_vals: Vec<CoordValue> = Vec::with_capacity(ndim);

    for (i, dr_opt) in dim_ranges.iter().enumerate() {
        let dim = &schema.dims[i];
        let (lo_bound, hi_bound) = if let Some(dr) = dr_opt {
            (dr.lo.clone(), dr.hi.clone())
        } else {
            (dim.domain.lo.clone(), dim.domain.hi.clone())
        };

        let lo_cv = match lo_bound {
            DomainBound::Int64(v) => CoordValue::Int64(v),
            DomainBound::Float64(v) => CoordValue::Float64(v),
            DomainBound::TimestampMs(v) => CoordValue::TimestampMs(v),
            DomainBound::String(v) => CoordValue::String(v),
        };
        let hi_cv = match hi_bound {
            DomainBound::Int64(v) => CoordValue::Int64(v),
            DomainBound::Float64(v) => CoordValue::Float64(v),
            DomainBound::TimestampMs(v) => CoordValue::TimestampMs(v),
            DomainBound::String(v) => CoordValue::String(v),
        };

        lo_vals.push(lo_cv);
        hi_vals.push(hi_cv);
    }

    // Generate all 2^ndim corners and compute their Hilbert prefix.
    let num_corners = 1usize << ndim;
    let mut min_prefix = u64::MAX;
    let mut max_prefix = 0u64;
    let mut found_any = false;

    for mask in 0..num_corners {
        let corner: Vec<CoordValue> = (0..ndim)
            .map(|i| {
                if (mask >> i) & 1 == 1 {
                    hi_vals[i].clone()
                } else {
                    lo_vals[i].clone()
                }
            })
            .collect();

        // Use the tile prefix (computed from tile indices = coord / tile_extents),
        // not the cell prefix — tiles store and the per-shard filter compares
        // against tile-level Hilbert keys.
        if let Ok(tile_id) = tile_id_for_cell(schema, &corner, 0) {
            min_prefix = min_prefix.min(tile_id.hilbert_prefix);
            max_prefix = max_prefix.max(tile_id.hilbert_prefix);
            found_any = true;
        }
    }

    if found_any {
        vec![(min_prefix, max_prefix)]
    } else {
        Vec::new() // fallback: unbounded
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
        };
        {
            let mut cat = handle.write().expect("write lock");
            cat.register(entry).expect("register");
        }
        let ctx = ConvertContext {
            retention_registry: None,
            array_catalog: Some(handle.clone()),
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled,
        };
        (ctx, handle)
    }

    #[test]
    fn single_node_emits_local_array() {
        let (ctx, _handle) = make_ctx(false);
        let slice_ast = ArraySliceAst { dim_ranges: vec![] };
        let tasks = convert_slice("test_arr", &slice_ast, &[], 100, TenantId::new(1), &ctx)
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
        let tasks = convert_slice("test_arr", &slice_ast, &[], 100, TenantId::new(1), &ctx)
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
