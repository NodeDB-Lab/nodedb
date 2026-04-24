//! Shared helper: materialize a row value from a DecodedColumn.

use crate::reader::DecodedColumn;

/// Extract a single row value from a DecodedColumn.
pub(super) fn extract_row_value(
    col: &DecodedColumn,
    row_idx: usize,
    col_type: &nodedb_types::columnar::ColumnType,
) -> nodedb_types::value::Value {
    use nodedb_types::value::Value;

    match col {
        DecodedColumn::Int64 { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Integer(values[row_idx])
            }
        }
        DecodedColumn::Float64 { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Float(values[row_idx])
            }
        }
        DecodedColumn::Timestamp { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Integer(values[row_idx]) // Timestamp stored as micros.
            }
        }
        DecodedColumn::Bool { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Bool(values[row_idx])
            }
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            if !valid[row_idx] {
                return Value::Null;
            }
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            let bytes = &data[start..end];

            match col_type {
                nodedb_types::columnar::ColumnType::String => {
                    Value::String(String::from_utf8_lossy(bytes).into_owned())
                }
                nodedb_types::columnar::ColumnType::Bytes
                | nodedb_types::columnar::ColumnType::Geometry => Value::Bytes(bytes.to_vec()),
                _ => Value::Bytes(bytes.to_vec()),
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            if !valid[row_idx] {
                return Value::Null;
            }
            let id = ids[row_idx] as usize;
            if let Some(s) = dictionary.get(id) {
                Value::String(s.clone())
            } else {
                Value::Null
            }
        }
    }
}
