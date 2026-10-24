// SPDX-License-Identifier: BUSL-1.1

//! Engine-boundary conversion from the array engine's typed `CoordValue` /
//! `CellValue` to `nodedb_types::Value`, the wire and SQL-surface carrier.
//!
//! Every match is exhaustive on the source enum so adding a new variant
//! fails to compile until handled here explicitly. The opposite direction
//! runs on the Control Plane, where a literal narrows to the attr's
//! declared type before any cell reaches the Data Plane.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::sparse_tile::{RowKind, SparseTile};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::cell_value::widen_f32;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::{ArrayCell, Value};

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

/// Convert one attribute value from the engine's typed representation
/// to the generic wire `Value`. A narrow integer widens to `Integer`. A
/// `Float32` widens through `widen_f32`, so `0.1` stored as `FLOAT32` reads
/// as `0.1` over JSON, the native protocol, and pgwire.
pub fn cell_value_to_value(c: &CellValue) -> Value {
    match c {
        CellValue::Int64(v) => Value::Integer(*v),
        CellValue::Float64(v) => Value::Float(*v),
        CellValue::String(v) => Value::String(v.clone()),
        CellValue::Bytes(v) => Value::Bytes(v.clone()),
        CellValue::Null => Value::Null,
        CellValue::UInt8(v) => Value::Integer(i64::from(*v)),
        CellValue::Int8(v) => Value::Integer(i64::from(*v)),
        CellValue::Int16(v) => Value::Integer(i64::from(*v)),
        CellValue::UInt16(v) => Value::Integer(i64::from(*v)),
        CellValue::Int32(v) => Value::Integer(i64::from(*v)),
        CellValue::UInt32(v) => Value::Integer(i64::from(*v)),
        CellValue::Float32(v) => Value::Float(widen_f32(*v)),
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
        cells.push(ArrayCell {
            coords,
            attrs,
            system_time: None,
        });
    }
    cells
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrow_cells_widen_to_wire_values() {
        assert_eq!(
            cell_value_to_value(&CellValue::UInt8(255)),
            Value::Integer(255)
        );
        assert_eq!(
            cell_value_to_value(&CellValue::Int16(-2)),
            Value::Integer(-2)
        );
        assert_eq!(
            cell_value_to_value(&CellValue::UInt32(u32::MAX)),
            Value::Integer(4_294_967_295)
        );
        assert_eq!(
            cell_value_to_value(&CellValue::Float32(0.5)),
            Value::Float(0.5)
        );
        assert_eq!(
            cell_value_to_value(&CellValue::Float32(0.1)),
            Value::Float(0.1)
        );
        assert_eq!(
            cell_value_to_value(&CellValue::Float32(f32::MAX)),
            Value::Float(3.4028235e38)
        );
    }
}
