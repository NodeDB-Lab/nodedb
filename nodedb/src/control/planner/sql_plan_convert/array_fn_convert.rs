//! `SqlPlan::NdArray*` → `PhysicalTask` lowering for the read/maint
//! function surface (Tier 6 sub-pass 3). Sub-pass 2 array DDL/DML
//! lowering is in [`super::array_convert`]; the two are kept side by
//! side but split so each file stays under the 500-line limit.
//!
//! Each converter:
//! 1. Resolves the array's catalog entry to obtain its typed schema.
//! 2. Maps SQL-surface name strings (attrs, dims) to positional `u32`
//!    indices required by [`ArrayOp`].
//! 3. Coerces literal coord values against each dim's declared dtype.
//! 4. zerompk-encodes nested payloads (e.g. `nodedb_array::query::Slice`)
//!    matching the `slice_msgpack` wire contract.
//!
//! Shape-mismatch errors surface as `crate::Error::PlanError` so the
//! pgwire layer turns them into a structured response.

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::types_array::{
    ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst, ArraySliceAst,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{ArrayBinaryOp, ArrayOp, ArrayReducer};
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::convert::ConvertContext;

pub(super) fn convert_slice(
    name: &str,
    slice_ast: &ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;

    // Build per-dim DimRange in schema dim order.
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
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_project(
    name: &str,
    attr_projection: &[String],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;
    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;
    if attr_indices.is_empty() {
        return Err(crate::Error::PlanError {
            detail: format!("NDARRAY_PROJECT: array '{name}': attr list must not be empty"),
        });
    }
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Project {
            array_id: aid,
            attr_indices,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_agg(
    name: &str,
    attr: &str,
    reducer: ArrayReducerAst,
    group_by_dim: Option<&str>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;
    let attr_idx = schema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("NDARRAY_AGG: array '{name}' has no attr '{attr}'"),
        })? as u32;
    let group_by_dim_idx: i32 = match group_by_dim {
        None => -1,
        Some(dim) => schema
            .dims
            .iter()
            .position(|d| d.name == dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("NDARRAY_AGG: array '{name}' has no dim '{dim}'"),
            })? as i32,
    };
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: aid,
            attr_idx,
            reducer: map_reducer(reducer),
            group_by_dim: group_by_dim_idx,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_elementwise(
    left_name: &str,
    right_name: &str,
    op: ArrayBinaryOpAst,
    attr: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let lschema = load_schema(left_name, ctx)?;
    let rschema = load_schema(right_name, ctx)?;
    if lschema.dims.len() != rschema.dims.len() || lschema.attrs.len() != rschema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "NDARRAY_ELEMENTWISE: arrays '{left_name}' and '{right_name}' have different shapes"
            ),
        });
    }
    let attr_idx = lschema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("NDARRAY_ELEMENTWISE: array '{left_name}' has no attr '{attr}'"),
        })? as u32;
    if !rschema.attrs.iter().any(|a| a.name == attr) {
        return Err(crate::Error::PlanError {
            detail: format!("NDARRAY_ELEMENTWISE: array '{right_name}' has no attr '{attr}'"),
        });
    }
    let left = ArrayId::new(tenant_id, left_name);
    let right = ArrayId::new(tenant_id, right_name);
    let vshard = VShardId::from_collection(left_name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Elementwise {
            left,
            right,
            op: map_binary_op(op),
            attr_idx,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_flush(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // Existence check via schema load — surfaces "not found" before
    // dispatch.
    let _ = load_schema(name, ctx)?;
    let wal = ctx.wal.as_ref().ok_or_else(|| crate::Error::PlanError {
        detail: "NDARRAY_FLUSH: no WAL wired into convert context".into(),
    })?;
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    let wal_lsn = wal.next_lsn().as_u64();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Flush {
            array_id: aid,
            wal_lsn,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_compact(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let _ = load_schema(name, ctx)?;
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Compact { array_id: aid }),
        post_set_op: PostSetOp::None,
    }])
}

// ── Helpers ─────────────────────────────────────────────────────────

fn load_schema(name: &str, ctx: &ConvertContext) -> crate::Result<ArraySchema> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("NDARRAY_*: no array catalog wired into convert context for '{name}'"),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name(name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("NDARRAY_*: array '{name}' not found"),
            })?
    };
    zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array schema decode: {e}"),
    })
}

fn resolve_attr_indices(
    name: &str,
    attrs: &[String],
    schema: &ArraySchema,
) -> crate::Result<Vec<u32>> {
    let mut out = Vec::with_capacity(attrs.len());
    for a in attrs {
        let idx = schema
            .attrs
            .iter()
            .position(|s| &s.name == a)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("NDARRAY_*: array '{name}' has no attr '{a}'"),
            })?;
        out.push(idx as u32);
    }
    Ok(out)
}

fn coerce_bound(
    lit: &ArrayCoordLiteral,
    dtype: EngineDimType,
    dim: &str,
) -> crate::Result<DomainBound> {
    match (lit, dtype) {
        (ArrayCoordLiteral::Int64(v), EngineDimType::Int64) => Ok(DomainBound::Int64(*v)),
        (ArrayCoordLiteral::Int64(v), EngineDimType::TimestampMs) => {
            Ok(DomainBound::TimestampMs(*v))
        }
        (ArrayCoordLiteral::Int64(v), EngineDimType::Float64) => {
            Ok(DomainBound::Float64(*v as f64))
        }
        (ArrayCoordLiteral::Float64(v), EngineDimType::Float64) => Ok(DomainBound::Float64(*v)),
        (ArrayCoordLiteral::String(v), EngineDimType::String) => Ok(DomainBound::String(v.clone())),
        (got, want) => Err(crate::Error::PlanError {
            detail: format!(
                "NDARRAY_SLICE bound for dim `{dim}`: got {got:?}, expected dim type {want:?}"
            ),
        }),
    }
}

fn map_reducer(r: ArrayReducerAst) -> ArrayReducer {
    match r {
        ArrayReducerAst::Sum => ArrayReducer::Sum,
        ArrayReducerAst::Count => ArrayReducer::Count,
        ArrayReducerAst::Min => ArrayReducer::Min,
        ArrayReducerAst::Max => ArrayReducer::Max,
        ArrayReducerAst::Mean => ArrayReducer::Mean,
    }
}

fn map_binary_op(o: ArrayBinaryOpAst) -> ArrayBinaryOp {
    match o {
        ArrayBinaryOpAst::Add => ArrayBinaryOp::Add,
        ArrayBinaryOpAst::Sub => ArrayBinaryOp::Sub,
        ArrayBinaryOpAst::Mul => ArrayBinaryOp::Mul,
        ArrayBinaryOpAst::Div => ArrayBinaryOp::Div,
    }
}

// Silence "unused import" when EngineAttrType isn't referenced — kept
// in scope for future schema-validation extensions on this module.
const _UNUSED_ATTR_TYPE: Option<EngineAttrType> = None;
