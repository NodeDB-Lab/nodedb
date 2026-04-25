//! Shared helpers for array fn → PhysicalTask lowering.

use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::types_array::{ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst};

use crate::bridge::physical_plan::{ArrayBinaryOp, ArrayReducer};

use super::super::convert::ConvertContext;

pub(super) fn load_schema(name: &str, ctx: &ConvertContext) -> crate::Result<ArraySchema> {
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
                detail: format!("NDARRAY_*: array '{name}' has no attr '{a}'"),
            })?;
        out.push(idx as u32);
    }
    Ok(out)
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
                "NDARRAY_SLICE bound for dim `{dim}`: got {got:?}, expected dim type {want:?}"
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

const _UNUSED_ATTR_TYPE: Option<EngineAttrType> = None;
