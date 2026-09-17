// SPDX-License-Identifier: Apache-2.0

//! Shared helper: materialize a row value from a DecodedColumn.

use nodedb_types::value_from_msgpack;

use crate::error::ColumnarError;
use crate::reader::DecodedColumn;

/// Extract a single row value from a `DecodedColumn`.
///
/// Returns `Err(ColumnarError::MsgpackDeserialize)` if a `Json` column
/// contains bytes that cannot be decoded as MessagePack — this indicates
/// segment corruption rather than a missing value, so `Value::Null` would
/// silently hide the problem.
pub(crate) fn extract_row_value(
    col: &DecodedColumn,
    row_idx: usize,
    col_type: &nodedb_types::columnar::ColumnType,
    col_name: &str,
) -> Result<nodedb_types::value::Value, ColumnarError> {
    use nodedb_types::value::Value;

    let v = match col {
        // The reader infers a column's physical kind from its codec, so a
        // time column decodes as `Int64`. The declared type decides the cell:
        // an instant column yields the variant its declared type names;
        // every other type yields the integer stored.
        DecodedColumn::Int64 { values, valid } | DecodedColumn::Timestamp { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                col_type.time_cell(values[row_idx])
            }
        }
        DecodedColumn::Float64 { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Float(values[row_idx])
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
                Value::Null
            } else {
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let bytes = &data[start..end];

                match col_type {
                    nodedb_types::columnar::ColumnType::String
                    | nodedb_types::columnar::ColumnType::SparseVector => {
                        Value::String(String::from_utf8_lossy(bytes).into_owned())
                    }
                    nodedb_types::columnar::ColumnType::Json => {
                        // MessagePack-encoded JSON; decode back to structured Value.
                        // An empty byte slice means the JSON value was NULL at write time.
                        if bytes.is_empty() {
                            Value::Null
                        } else {
                            value_from_msgpack(bytes).map_err(|e| {
                                ColumnarError::MsgpackDeserialize {
                                    column: col_name.to_string(),
                                    source: e,
                                }
                            })?
                        }
                    }
                    nodedb_types::columnar::ColumnType::Bytes
                    | nodedb_types::columnar::ColumnType::Geometry => Value::Bytes(bytes.to_vec()),
                    _ => Value::Bytes(bytes.to_vec()),
                }
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                let id = ids[row_idx] as usize;
                if let Some(s) = dictionary.get(id) {
                    Value::String(s.clone())
                } else {
                    Value::Null
                }
            }
        }
    };
    Ok(v)
}
