// SPDX-License-Identifier: Apache-2.0

//! Per-column value encoding for the Binary Tuple encoder.
//!
//! Every value either encodes in full or returns an error. A coercion
//! source that does not convert to the column type is an error, never a
//! zeroed or empty field.

use nodedb_types::columnar::{ColumnDef, ColumnType};
use nodedb_types::value::Value;

use crate::error::StrictError;

/// Encode a fixed-size value into `dst`.
///
/// Handles both native Value types and SQL coercion sources.
pub(super) fn encode_fixed(
    dst: &mut [u8],
    col: &ColumnDef,
    value: &Value,
) -> Result<(), StrictError> {
    match (&col.column_type, value) {
        (ColumnType::Int64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Float64, Value::Float(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Float64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&(*v as f64).to_le_bytes());
        }
        (ColumnType::Bool, Value::Bool(v)) => {
            dst[0] = u8::from(*v);
        }
        (ColumnType::Timestamp, Value::NaiveDateTime(dt))
        | (ColumnType::Timestamptz | ColumnType::SystemTimestamp, Value::DateTime(dt)) => {
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (
            ColumnType::Timestamp
            | ColumnType::Timestamptz
            | ColumnType::SystemTimestamp
            | ColumnType::Duration,
            Value::Integer(micros),
        ) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        (ColumnType::Timestamp | ColumnType::Timestamptz, Value::String(s)) => {
            let dt = nodedb_types::NdbDateTime::parse(s).ok_or_else(|| {
                invalid_value(
                    col,
                    format!("'{s}' does not parse as an ISO 8601 timestamp"),
                )
            })?;
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (ColumnType::Ulid, Value::Ulid(s) | Value::String(s)) => {
            let id = ulid::Ulid::from_string(s)
                .map_err(|e| invalid_value(col, format!("'{s}' does not parse as a ULID: {e}")))?;
            dst[..16].copy_from_slice(&id.to_bytes());
        }
        (ColumnType::Duration, Value::Duration(duration)) => {
            dst[..8].copy_from_slice(&duration.micros.to_le_bytes());
        }
        (ColumnType::Duration, Value::String(s)) => {
            let duration = nodedb_types::NdbDuration::parse(s)
                .ok_or_else(|| invalid_value(col, format!("'{s}' does not parse as a duration")))?;
            dst[..8].copy_from_slice(&duration.micros.to_le_bytes());
        }
        (ColumnType::Decimal { .. }, Value::Decimal(d)) => {
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::String(s)) => {
            let d: rust_decimal::Decimal = s.parse().map_err(|e| {
                invalid_value(col, format!("'{s}' does not parse as a decimal: {e}"))
            })?;
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::Float(f)) => {
            let d = rust_decimal::Decimal::try_from(*f).map_err(|e| {
                invalid_value(col, format!("{f} has no decimal representation: {e}"))
            })?;
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::Integer(i)) => {
            dst[..16].copy_from_slice(&rust_decimal::Decimal::from(*i).serialize());
        }
        (ColumnType::Uuid, Value::Uuid(s) | Value::String(s)) => {
            let parsed = uuid::Uuid::parse_str(s)
                .map_err(|e| invalid_value(col, format!("'{s}' does not parse as a UUID: {e}")))?;
            dst[..16].copy_from_slice(parsed.as_bytes());
        }
        (ColumnType::Vector(dim), Value::Array(arr)) => {
            check_vector_len(col, *dim, arr.len())?;
            for (i, v) in arr.iter().enumerate() {
                let f = match v {
                    Value::Float(f) => *f as f32,
                    Value::Integer(n) => *n as f32,
                    other => {
                        return Err(invalid_value(
                            col,
                            format!("element {i} is {other:?}, not a number"),
                        ));
                    }
                };
                dst[i * 4..(i + 1) * 4].copy_from_slice(&f.to_le_bytes());
            }
        }
        (ColumnType::Vector(dim), Value::Bytes(b)) => {
            if b.len() % 4 != 0 {
                return Err(invalid_value(
                    col,
                    format!("{} bytes is not a whole number of f32 values", b.len()),
                ));
            }
            check_vector_len(col, *dim, b.len() / 4)?;
            dst[..b.len()].copy_from_slice(b);
        }
        _ => return Err(type_mismatch(col)),
    }
    Ok(())
}

/// Encode a variable-length value, appending to `var_data`.
///
/// Handles both native Value types and SQL coercion sources.
pub(super) fn encode_variable(
    var_data: &mut Vec<u8>,
    col: &ColumnDef,
    value: &Value,
) -> Result<(), StrictError> {
    match (&col.column_type, value) {
        (ColumnType::String, Value::String(s)) | (ColumnType::SparseVector, Value::String(s)) => {
            // Sparse vector text is a `'{id: weight}'` literal parsed at
            // index-build time.
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::Bytes | ColumnType::SparseVector, Value::Bytes(b)) => {
            var_data.extend_from_slice(b);
        }
        // EWKB, so the cell keeps its SRID, Z and M. A stored SRID is
        // checked for syntax only.
        (ColumnType::Geometry, value) => append_geometry(var_data, col, value)?,
        (ColumnType::Json, Value::String(s)) => {
            // A JSON text stores as the value it spells. Any other text
            // stores as a JSON string.
            let parsed = sonic_rs::from_str::<serde_json::Value>(s)
                .ok()
                .map(Value::from);
            append_json(var_data, col, parsed.as_ref().unwrap_or(value))?;
        }
        (ColumnType::Json, value) => {
            append_json(var_data, col, value)?;
        }
        // Typed variable columns use tagged NodeDB MessagePack so their Value
        // variant survives storage. Coercion inputs are converted first.
        (ColumnType::Array, Value::Array(_))
        | (ColumnType::Set, Value::Set(_))
        | (ColumnType::Regex, Value::Regex(_))
        | (ColumnType::Range, Value::Range { .. })
        | (ColumnType::Record, Value::Record { .. }) => {
            append_msgpack(var_data, col, value)?;
        }
        (ColumnType::Set, Value::Array(items)) => {
            append_msgpack(var_data, col, &Value::Set(items.clone()))?;
        }
        (ColumnType::Regex, Value::String(pattern)) => {
            append_msgpack(var_data, col, &Value::Regex(pattern.clone()))?;
        }
        (ColumnType::Record, Value::String(reference)) => {
            let (table, id) = reference
                .split_once(':')
                .filter(|(table, id)| !table.is_empty() && !id.is_empty())
                .ok_or_else(|| {
                    invalid_value(col, format!("'{reference}' is not a 'table:id' reference"))
                })?;
            append_msgpack(
                var_data,
                col,
                &Value::Record {
                    table: table.to_owned(),
                    id: id.to_owned(),
                },
            )?;
        }
        _ => return Err(type_mismatch(col)),
    }
    Ok(())
}

/// Append the untagged MessagePack form a JSON column stores.
fn append_json(var_data: &mut Vec<u8>, col: &ColumnDef, value: &Value) -> Result<(), StrictError> {
    let bytes = nodedb_types::value_to_msgpack(value)
        .map_err(|e| invalid_value(col, format!("value does not serialize to MessagePack: {e}")))?;
    var_data.extend_from_slice(&bytes);
    Ok(())
}

/// Append a lossless NodeDB MessagePack representation.
fn append_msgpack(
    var_data: &mut Vec<u8>,
    col: &ColumnDef,
    value: &Value,
) -> Result<(), StrictError> {
    let bytes = zerompk::to_msgpack_vec(value)
        .map_err(|e| invalid_value(col, format!("value does not serialize to MessagePack: {e}")))?;
    var_data.extend_from_slice(&bytes);
    Ok(())
}

/// Error unless a vector holds exactly `dim` elements.
fn check_vector_len(col: &ColumnDef, dim: u32, len: usize) -> Result<(), StrictError> {
    if usize::try_from(dim).is_ok_and(|d| d == len) {
        Ok(())
    } else {
        Err(invalid_value(
            col,
            format!("expected {dim} elements, got {len}"),
        ))
    }
}

/// Append the EWKB of the geometry `value` holds, or refuse the value.
fn append_geometry(
    var_data: &mut Vec<u8>,
    col: &ColumnDef,
    value: &Value,
) -> Result<(), StrictError> {
    let ewkb = nodedb_spatial::ewkb_of_value(value, nodedb_spatial::SridCheck::Syntax).map_err(
        |e| match e {
            nodedb_spatial::SpatialError::NotGeometryValue { .. } => type_mismatch(col),
            e => invalid_value(col, e.to_string()),
        },
    )?;
    var_data.extend_from_slice(&ewkb);
    Ok(())
}

fn type_mismatch(col: &ColumnDef) -> StrictError {
    StrictError::TypeMismatch {
        column: col.name.clone(),
        expected: col.column_type,
    }
}

fn invalid_value(col: &ColumnDef, detail: String) -> StrictError {
    StrictError::InvalidValue {
        column: col.name.clone(),
        expected: col.column_type,
        detail,
    }
}
