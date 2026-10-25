// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for array fn → PhysicalTask lowering.

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::{ArraySchema, DimType as EngineDimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::{
    ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst, ArraySliceAst,
};

use crate::control::array_catalog::ArrayCatalogEntry;
use nodedb_physical::physical_plan::{ArrayBinaryOp, ArrayReducer};

use super::super::convert::ConvertContext;

/// Load the full catalog entry for an array by name.
///
/// Used by converters that need both the schema *and* catalog metadata
/// (e.g. `prefix_bits` for cluster routing).
pub(super) fn load_entry(
    name: &str,
    tenant_id: nodedb_types::TenantId,
    ctx: &ConvertContext,
) -> crate::Result<ArrayCatalogEntry> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_*: no array catalog wired into convert context for '{name}'"),
        })?;
    let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
        detail: "array catalog lock poisoned".into(),
    })?;
    cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_*: array '{name}' not found"),
        })
}

/// The schema a catalog entry holds.
pub(super) fn decode_schema(entry: &ArrayCatalogEntry) -> crate::Result<ArraySchema> {
    zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array schema decode: {e}"),
    })
}

pub(super) fn resolve_attr_indices(
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
                detail: format!("ARRAY_*: array '{name}' has no attr '{a}'"),
            })?;
        out.push(idx as u32);
    }
    Ok(out)
}

/// The engine [`Slice`] for a named-dim slice predicate over array `name`.
/// Each range is coerced to its dim's type. A dim no range names stays
/// unconstrained. `context` prefixes every error.
pub(crate) fn resolve_slice(
    context: &str,
    name: &str,
    ast: &ArraySliceAst,
    schema: &ArraySchema,
) -> crate::Result<Slice> {
    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &ast.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("{context}: array '{name}' has no dim '{}'", r.dim),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = coerce_bound(&r.lo, dtype, &r.dim)?;
        let hi = coerce_bound(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(lo, hi));
    }
    Ok(Slice::new(dim_ranges))
}

pub(crate) fn coerce_bound(
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
                "ARRAY_SLICE bound for dim `{dim}`: got {got:?}, expected dim type {want:?}"
            ),
        }),
    }
}

pub(super) fn map_reducer(r: ArrayReducerAst) -> ArrayReducer {
    match r {
        ArrayReducerAst::Sum => ArrayReducer::Sum,
        ArrayReducerAst::Count => ArrayReducer::Count,
        ArrayReducerAst::Min => ArrayReducer::Min,
        ArrayReducerAst::Max => ArrayReducer::Max,
        ArrayReducerAst::Mean => ArrayReducer::Mean,
        ArrayReducerAst::Median => ArrayReducer::Median,
        ArrayReducerAst::Percentile(p) => ArrayReducer::Percentile { p },
        ArrayReducerAst::StdDevPop => ArrayReducer::StdDevPop,
        ArrayReducerAst::StdDevSamp => ArrayReducer::StdDevSamp,
        ArrayReducerAst::VarPop => ArrayReducer::VarPop,
        ArrayReducerAst::VarSamp => ArrayReducer::VarSamp,
        ArrayReducerAst::Mode => ArrayReducer::Mode,
        ArrayReducerAst::Histogram { bins, lo, hi } => ArrayReducer::Histogram { bins, lo, hi },
    }
}

pub(super) fn map_binary_op(o: ArrayBinaryOpAst) -> ArrayBinaryOp {
    match o {
        ArrayBinaryOpAst::Add => ArrayBinaryOp::Add,
        ArrayBinaryOpAst::Sub => ArrayBinaryOp::Sub,
        ArrayBinaryOpAst::Mul => ArrayBinaryOp::Mul,
        ArrayBinaryOpAst::Div => ArrayBinaryOp::Div,
    }
}

/// Resolve a [`TemporalScope`] into the `(system_time, valid_at_ms)` pair
/// expected by `ArrayOp::Aggregate` / `ArrayOp::Elementwise` (and their
/// `ClusterArrayOp` fan-outs).
///
/// With neither clause: `SystemTimeScope::Current` and no valid-time
/// filter, the Data Plane's live-state path.
///
/// `AS OF SYSTEM TIME` passes its scope through. The statement's cut pin
/// (`system_cut::pin_statement_cuts`) later turns `AsOf` into `Pinned`, and
/// every serving node waits for the pinned cut's marker before it reads.
/// `valid_at_ms` stays `None` unless `AS OF VALID TIME` is also present.
///
/// When `AS OF VALID TIME` is given: `valid_at_ms = Some(v)`. Only the
/// `ValidTime::At(v)` (point-in-time) form is supported by the array engine;
/// a range predicate (`ValidTime::Range`) is rejected here with a typed error
/// because array cells store a single valid-time point, not an interval.
///
/// `AS OF SYSTEM TIME NULL` (AllVersions) is rejected for aggregates because
/// summing across all historical versions is not meaningful. Use
/// `ARRAY_SLICE` for audit-log reads instead.
pub(super) fn resolve_array_temporal(
    temporal: TemporalScope,
    context: &str,
) -> crate::Result<(nodedb_types::SystemTimeScope, Option<i64>)> {
    let system_time = match temporal.system_time {
        scope @ (nodedb_types::SystemTimeScope::Current
        | nodedb_types::SystemTimeScope::AsOf(_)
        | nodedb_types::SystemTimeScope::Pinned { .. }) => scope,
        nodedb_types::SystemTimeScope::AllVersions => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "{context}: AS OF SYSTEM TIME NULL is not meaningful for {context}; \
                     use a point-in-time AS OF SYSTEM TIME <ms>, or ARRAY_SLICE for the audit log"
                ),
            });
        }
    };
    let valid_at_ms =
        crate::control::planner::sql_plan_convert::valid_time_point(temporal.valid_time, "array")?;
    Ok((system_time, valid_at_ms))
}

/// Resolve a [`TemporalScope`] into the `(system_time, valid_at_ms)` pair
/// expected by `ArrayOp::Slice` (and `ClusterArrayOp::Slice`).
///
/// Unlike `resolve_array_temporal` (used for AGG), this variant passes
/// `SystemTimeScope::AllVersions` through unchanged so that
/// `AS OF SYSTEM TIME NULL` triggers the audit-log scan path in the
/// Data Plane handler.
///
/// `ValidTime::Range` is still rejected — array cells carry a single
/// valid-time point, not an interval, so range predicates are nonsensical.
pub(super) fn resolve_array_temporal_scope(
    temporal: TemporalScope,
) -> crate::Result<(nodedb_types::SystemTimeScope, Option<i64>)> {
    let system_time = temporal.system_time; // all three variants pass through
    let valid_at_ms =
        crate::control::planner::sql_plan_convert::valid_time_point(temporal.valid_time, "array")?;
    Ok((system_time, valid_at_ms))
}
