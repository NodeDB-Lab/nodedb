//! Engine-boundary conversion between `nodedb_types::Value` (the wire
//! and SQL-surface carrier) and the array engine's typed
//! `CoordValue` / `CellValue`.
//!
//! Every match is exhaustive on the source enum so adding a new variant
//! fails to compile until handled here explicitly.
//!
//! Read, aggregate, and elementwise handlers call every helper here,
//! so no module-wide dead-code allowance is needed.

use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::attr_spec::AttrType;
use nodedb_array::schema::dim_spec::DimType;
use nodedb_array::tile::sparse_tile::{RowKind, SparseTile};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::{ArrayCell, NodeDbError, Value};

/// Convert one coordinate scalar from the engine's typed representation
/// to the generic wire `Value`.
pub fn coord_value_to_value(c: &CoordValue) -> Value {
    match c {
        CoordValue::Int64(v) => Value::Integer(*v),
        CoordValue::Float64(v) => Value::Float(*v),
        CoordValue::TimestampMs(v) => Value::Integer(*v),
        CoordValue::String(v) => Value::String(v.clone()),
    }
}

/// Coerce a generic `Value` to a typed `CoordValue`, rejecting any
/// combination that the target dimension type cannot represent.
#[allow(dead_code)] // consumed by SQL DML extractors
pub fn value_to_coord_value(v: &Value, expected: DimType) -> Result<CoordValue, NodeDbError> {
    match (v, expected) {
        (Value::Integer(i), DimType::Int64) => Ok(CoordValue::Int64(*i)),
        (Value::Integer(i), DimType::TimestampMs) => Ok(CoordValue::TimestampMs(*i)),
        (Value::Integer(i), DimType::Float64) => Ok(CoordValue::Float64(*i as f64)),
        (Value::Float(f), DimType::Float64) => Ok(CoordValue::Float64(*f)),
        (Value::String(s), DimType::String) => Ok(CoordValue::String(s.clone())),
        // Every other Value variant is explicitly unsupported for coords.
        (
            Value::Null
            | Value::Bool(_)
            | Value::Integer(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Array(_)
            | Value::Object(_)
            | Value::Uuid(_)
            | Value::Ulid(_)
            | Value::DateTime(_)
            | Value::Duration(_)
            | Value::Decimal(_)
            | Value::Geometry(_)
            | Value::Set(_)
            | Value::Regex(_)
            | Value::Range { .. }
            | Value::Record { .. }
            | Value::NdArrayCell(_),
            _,
        ) => Err(NodeDbError::codec(format!(
            "coord value {} not convertible to {:?}",
            value_kind(v),
            expected
        ))),
    }
}

/// Convert one attribute value from the engine's typed representation
/// to the generic wire `Value`.
pub fn cell_value_to_value(c: &CellValue) -> Value {
    match c {
        CellValue::Int64(v) => Value::Integer(*v),
        CellValue::Float64(v) => Value::Float(*v),
        CellValue::String(v) => Value::String(v.clone()),
        CellValue::Bytes(v) => Value::Bytes(v.clone()),
        CellValue::Null => Value::Null,
    }
}

/// Coerce a generic `Value` to a typed `CellValue`, rejecting any
/// combination the target attribute type cannot represent. `Null` maps
/// to `CellValue::Null` regardless of the declared attribute type;
/// nullability is enforced elsewhere (schema validation).
#[allow(dead_code)] // consumed by SQL DML extractors
pub fn value_to_cell_value(v: &Value, expected: AttrType) -> Result<CellValue, NodeDbError> {
    match (v, expected) {
        (Value::Null, _) => Ok(CellValue::Null),
        (Value::Integer(i), AttrType::Int64) => Ok(CellValue::Int64(*i)),
        (Value::Integer(i), AttrType::Float64) => Ok(CellValue::Float64(*i as f64)),
        (Value::Float(f), AttrType::Float64) => Ok(CellValue::Float64(*f)),
        (Value::String(s), AttrType::String) => Ok(CellValue::String(s.clone())),
        (Value::Bytes(b), AttrType::Bytes) => Ok(CellValue::Bytes(b.clone())),
        (
            Value::Bool(_)
            | Value::Integer(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Array(_)
            | Value::Object(_)
            | Value::Uuid(_)
            | Value::Ulid(_)
            | Value::DateTime(_)
            | Value::Duration(_)
            | Value::Decimal(_)
            | Value::Geometry(_)
            | Value::Set(_)
            | Value::Regex(_)
            | Value::Range { .. }
            | Value::Record { .. }
            | Value::NdArrayCell(_),
            _,
        ) => Err(NodeDbError::codec(format!(
            "attr value {} not convertible to {:?}",
            value_kind(v),
            expected
        ))),
    }
}

/// Materialize a sparse tile back into the `ArrayCell` wire carrier.
/// Coordinates are reconstructed from each dim's dictionary + index
/// stream; attributes are taken in column order.
pub fn sparse_tile_to_array_cells(schema: &ArraySchema, tile: &SparseTile) -> Vec<ArrayCell> {
    let arity = schema.arity();
    let mut cells = Vec::with_capacity(tile.nnz() as usize);
    let mut live_idx = 0usize;
    for row in 0..tile.row_count() {
        // Sentinel rows carry no attribute payload — skip them.
        let kind = match tile.row_kind(row) {
            Ok(k) => k,
            Err(_) => break,
        };
        if kind != RowKind::Live {
            continue;
        }
        let attr_row = live_idx;
        live_idx += 1;
        let mut coords = Vec::with_capacity(arity);
        for dim in 0..arity {
            let dict = &tile.dim_dicts[dim];
            let idx = dict.indices[row] as usize;
            coords.push(coord_value_to_value(&dict.values[idx]));
        }
        let mut attrs = Vec::with_capacity(schema.attrs.len());
        for col in &tile.attr_cols {
            attrs.push(cell_value_to_value(&col[attr_row]));
        }
        cells.push(ArrayCell { coords, attrs });
    }
    cells
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Integer(_) => "integer",
        Value::Float(_) => "float",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Uuid(_) => "uuid",
        Value::Ulid(_) => "ulid",
        Value::DateTime(_) => "datetime",
        Value::Duration(_) => "duration",
        Value::Decimal(_) => "decimal",
        Value::Geometry(_) => "geometry",
        Value::Set(_) => "set",
        Value::Regex(_) => "regex",
        Value::Range { .. } => "range",
        Value::Record { .. } => "record",
        Value::NdArrayCell(_) => "ndarray_cell",
    }
}
