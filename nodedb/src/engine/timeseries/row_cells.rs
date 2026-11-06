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

/// A flushed-partition cell that does not read as its declared type.
#[derive(Debug, thiserror::Error)]
pub enum PartitionCellError {
    /// The stored millisecond count does not fit the column's instant kind.
    #[error(transparent)]
    Instant(#[from] NdbDateTimeError),
    /// The decoded column does not hold the data its schema type declares.
    #[error("the column holds {stored} data, declared {declared:?}")]
    TypeMismatch {
        stored: &'static str,
        declared: ColumnType,
    },
    /// The decoded column is shorter than the partition.
    #[error("the column has no row {row}")]
    RowOutOfRange { row: usize },
}

/// Extract one cell from a flushed partition as a `Value`, with no panic on
/// corrupt data. The partition schema carries each time column's kind, so the
/// cell is typed the same way a memtable cell is.
///
/// `Err` when the column does not match its declared type or holds no row
/// `idx`. A symbol id the dictionary does not hold reads as NULL. RESTORE and
/// its resolve both read here, so a backup row and a stored copy of it decode
/// to one object.
pub fn read_partition_cell(
    data: &ColumnData,
    ty: ColumnType,
    col_idx: usize,
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> Result<Value, PartitionCellError> {
    let out_of_range = PartitionCellError::RowOutOfRange { row: idx };
    let value = match (ty, data) {
        (ColumnType::Timestamp(kind), ColumnData::Timestamp(v)) => {
            return Ok(kind.cell_value(*v.get(idx).ok_or(out_of_range)?)?);
        }
        (ColumnType::Int64, ColumnData::Int64(v)) => {
            Value::Integer(*v.get(idx).ok_or(out_of_range)?)
        }
        (ColumnType::Float64, ColumnData::Float64(v)) => {
            let v = *v.get(idx).ok_or(out_of_range)?;
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        (ColumnType::Symbol | ColumnType::Text, ColumnData::Symbol(ids)) => {
            let id = *ids.get(idx).ok_or(out_of_range)?;
            sym_dicts
                .get(&col_idx)
                .and_then(|dict| dict.get(id))
                .map_or(Value::Null, |s| Value::String(s.to_string()))
        }
        (
            ColumnType::Symbol | ColumnType::Text,
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            },
        ) => {
            let id = *ids.get(idx).ok_or(out_of_range)?;
            if valid.get(idx).copied().unwrap_or(false) {
                dictionary
                    .get(id as usize)
                    .map_or(Value::Null, |s| Value::String(s.clone()))
            } else {
                Value::Null
            }
        }
        (declared, stored) => {
            return Err(PartitionCellError::TypeMismatch {
                stored: stored_kind(stored),
                declared,
            });
        }
    };
    Ok(value)
}

/// The name of the data shape a column holds.
fn stored_kind(data: &ColumnData) -> &'static str {
    match data {
        ColumnData::Timestamp(_) => "timestamp",
        ColumnData::Float64(_) => "float64",
        ColumnData::Int64(_) => "int64",
        ColumnData::Symbol(_) => "symbol",
        ColumnData::DictEncoded { .. } => "dictionary-encoded",
    }
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
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);

    /// A column shorter than the partition, a real corrupt state, is an error
    /// for every column type. Indexing it panics a Data Plane core.
    #[test]
    fn a_row_past_the_column_is_an_error() {
        let dicts = HashMap::new();
        let short = [
            (ColumnData::Timestamp(vec![1]), MILLIS),
            (ColumnData::Int64(vec![1]), ColumnType::Int64),
            (ColumnData::Float64(vec![1.0]), ColumnType::Float64),
            (ColumnData::Symbol(vec![0]), ColumnType::Symbol),
        ];
        for (data, ty) in &short {
            assert!(matches!(
                read_partition_cell(data, *ty, 0, &dicts, 1),
                Err(PartitionCellError::RowOutOfRange { row: 1 })
            ));
        }
    }

    /// A column of another type than its schema declares is an error. The
    /// `as_*` accessors reach `unreachable!` on it.
    #[test]
    fn a_column_of_the_wrong_type_is_an_error() {
        let dicts = HashMap::new();
        let floats = ColumnData::Float64(vec![1.0]);
        for ty in [
            MILLIS,
            ColumnType::Int64,
            ColumnType::Symbol,
            ColumnType::Text,
        ] {
            assert!(matches!(
                read_partition_cell(&floats, ty, 0, &dicts, 0),
                Err(PartitionCellError::TypeMismatch { .. })
            ));
        }
    }

    /// A healthy column decodes to its typed value. A NaN float and a symbol
    /// the dictionary does not hold read as NULL.
    #[test]
    fn a_healthy_column_decodes_to_its_value() {
        let dicts = HashMap::new();
        let cases = [
            (ColumnData::Timestamp(vec![7]), MILLIS, Value::Integer(7)),
            (
                ColumnData::Int64(vec![7]),
                ColumnType::Int64,
                Value::Integer(7),
            ),
            (
                ColumnData::Float64(vec![1.5]),
                ColumnType::Float64,
                Value::Float(1.5),
            ),
            (
                ColumnData::Float64(vec![f64::NAN]),
                ColumnType::Float64,
                Value::Null,
            ),
            (ColumnData::Symbol(vec![9]), ColumnType::Symbol, Value::Null),
        ];
        for (data, ty, expected) in &cases {
            assert_eq!(
                &read_partition_cell(data, *ty, 0, &dicts, 0).expect("reads"),
                expected
            );
        }
    }

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
