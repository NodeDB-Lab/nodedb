// SPDX-License-Identifier: BUSL-1.1

//! Timeseries cells as `Value`s, and the identity of a stored row.
//!
//! A RESTORE decodes a backup's rows with these readers, and its resolve
//! decodes the destination's rows with the same readers. So a restored row
//! and a stored copy of it decode to the same object, and carry the same
//! identity.

use std::collections::HashMap;

use nodedb_types::datetime::NdbDateTimeError;
use nodedb_types::value::Value;

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};

/// Extract one cell from a memtable column as a `Value`.
///
/// A time column yields the value its kind denotes — a typed instant for a
/// declared `TIMESTAMP` / `TIMESTAMPTZ` key, the integer stored for a
/// `BIGINT` key — so the decoded row carries the cell a client INSERT
/// sends.
pub fn memtable_cell(
    mt: &ColumnarMemtable,
    col_idx: usize,
    ty: ColumnType,
    idx: usize,
) -> Result<Value, NdbDateTimeError> {
    let value = match ty {
        ColumnType::Timestamp(kind) => {
            return kind.cell_value(mt.column(col_idx).as_timestamps()[idx]);
        }
        ColumnType::Int64 => Value::Integer(mt.column(col_idx).as_i64()[idx]),
        ColumnType::Float64 => {
            let v = mt.column(col_idx).as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol | ColumnType::Text => match mt.column(col_idx) {
            ColumnData::Symbol(ids) => mt
                .symbol_dict(col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    };
    Ok(value)
}

/// Extract one cell from a flushed-segment column as a `Value`. The partition
/// schema carries each time column's kind, so the cell is typed the same way
/// a memtable cell is.
pub fn partition_cell(
    data: &ColumnData,
    ty: ColumnType,
    col_idx: usize,
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> Result<Value, NdbDateTimeError> {
    let value = match ty {
        ColumnType::Timestamp(kind) => {
            return kind.cell_value(data.as_timestamps()[idx]);
        }
        ColumnType::Int64 => Value::Integer(data.as_i64()[idx]),
        ColumnType::Float64 => {
            let v = data.as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol | ColumnType::Text => match data {
            ColumnData::Symbol(ids) => sym_dicts
                .get(&col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    };
    Ok(value)
}

/// Insert a cell, skipping nulls: a row carries only its present fields, as
/// an ingest that names no field writes none.
pub fn insert_non_null(map: &mut HashMap<String, Value>, name: &str, value: Value) {
    if !matches!(value, Value::Null) {
        map.insert(name.to_string(), value);
    }
}

/// The identity of the row `fields`: its present cells, sorted by column
/// name, as native-`Value` msgpack. Two rows with the same series tags,
/// timestamp, `_ts_system` and values have the same identity.
pub fn row_identity(fields: &HashMap<String, Value>) -> zerompk::Result<Vec<u8>> {
    let mut cells: Vec<(&String, &Value)> = fields
        .iter()
        .filter(|(_, value)| !matches!(value, Value::Null))
        .collect();
    cells.sort_by(|a, b| a.0.cmp(b.0));
    let pairs: Vec<Value> = cells
        .into_iter()
        .map(|(name, value)| Value::Array(vec![Value::String(name.clone()), value.clone()]))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Array(pairs))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The identity ignores field order and absent cells.
    #[test]
    fn the_identity_is_order_free() {
        let a: HashMap<String, Value> = [
            ("host".to_string(), Value::String("h1".into())),
            ("v".to_string(), Value::Float(1.5)),
            ("_ts_system".to_string(), Value::Integer(7)),
        ]
        .into_iter()
        .collect();
        let mut b = HashMap::new();
        b.insert("_ts_system".to_string(), Value::Integer(7));
        b.insert("v".to_string(), Value::Float(1.5));
        b.insert("host".to_string(), Value::String("h1".into()));
        b.insert("gone".to_string(), Value::Null);
        assert_eq!(row_identity(&a).expect("a"), row_identity(&b).expect("b"));
        b.insert("_ts_system".to_string(), Value::Integer(8));
        assert_ne!(row_identity(&a).expect("a"), row_identity(&b).expect("b"));
    }
}
