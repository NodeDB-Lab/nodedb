// SPDX-License-Identifier: BUSL-1.1

//! One typed read of a timeseries cell, for the timeseries row emitters.
//!
//! A column whose stored data does not match its declared type, a row past
//! the end of its column, and a symbol id its dictionary does not hold are
//! all errors. None of them reads as NULL. A NULL float is stored as NaN, a
//! NULL symbol as [`NULL_SYMBOL_ID`], and a NULL decimal, UUID or boolean as
//! its empty cell.

use nodedb_types::timeseries::SymbolDictionary;

use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, NULL_SYMBOL_ID, TimeKind,
};

/// One timeseries cell, read as its declared column type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::data::executor) enum TsCell<'a> {
    Null,
    /// A time cell: its kind and the stored millisecond count.
    Time(TimeKind, i64),
    Float(f64),
    Int(i64),
    Symbol(&'a str),
    Decimal(rust_decimal::Decimal),
    /// A UUID's 16 bytes.
    Uuid([u8; 16]),
    Bool(bool),
}

/// A timeseries cell that does not read as its declared type.
#[derive(Debug, thiserror::Error)]
pub(in crate::data::executor) enum TsCellError {
    #[error("column '{column}' holds {stored} data, declared {declared:?}")]
    TypeMismatch {
        column: String,
        stored: &'static str,
        declared: ColumnType,
    },
    #[error("column '{column}' has no row {row}")]
    RowOutOfRange { column: String, row: usize },
    #[error("column '{column}' row {row} holds symbol {id}, which its dictionary does not hold")]
    UnknownSymbol { column: String, row: usize, id: u32 },
}

impl From<TsCellError> for crate::Error {
    fn from(e: TsCellError) -> Self {
        crate::Error::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

/// Read row `row` of `data`, a column named `column` declared `declared`.
/// `dict` is the column's symbol dictionary, when it has one.
pub(in crate::data::executor) fn read_ts_cell<'a>(
    data: &'a ColumnData,
    declared: ColumnType,
    column: &str,
    dict: Option<&'a SymbolDictionary>,
    row: usize,
) -> Result<TsCell<'a>, TsCellError> {
    let out_of_range = || TsCellError::RowOutOfRange {
        column: column.to_string(),
        row,
    };
    match (declared, data) {
        (ColumnType::Timestamp(kind), ColumnData::Timestamp(v)) => {
            let millis = *v.get(row).ok_or_else(out_of_range)?;
            Ok(TsCell::Time(kind, millis))
        }
        (ColumnType::Float64, ColumnData::Float64(v)) => {
            let f = *v.get(row).ok_or_else(out_of_range)?;
            Ok(if f.is_nan() {
                TsCell::Null
            } else {
                TsCell::Float(f)
            })
        }
        (ColumnType::Int64, ColumnData::Int64(v)) => {
            Ok(TsCell::Int(*v.get(row).ok_or_else(out_of_range)?))
        }
        (ColumnType::Symbol | ColumnType::Text, ColumnData::Symbol(ids)) => {
            let id = *ids.get(row).ok_or_else(out_of_range)?;
            if id == NULL_SYMBOL_ID {
                return Ok(TsCell::Null);
            }
            dict.and_then(|d| d.get(id))
                .map(TsCell::Symbol)
                .ok_or_else(|| TsCellError::UnknownSymbol {
                    column: column.to_string(),
                    row,
                    id,
                })
        }
        (ColumnType::Decimal(_), ColumnData::Decimal(v)) => Ok(v
            .get(row)
            .ok_or_else(out_of_range)?
            .map_or(TsCell::Null, TsCell::Decimal)),
        (ColumnType::Uuid, ColumnData::Uuid(v)) => Ok(v
            .get(row)
            .ok_or_else(out_of_range)?
            .map_or(TsCell::Null, TsCell::Uuid)),
        (ColumnType::Bool, ColumnData::Bool(v)) => Ok(v
            .get(row)
            .ok_or_else(out_of_range)?
            .map_or(TsCell::Null, TsCell::Bool)),
        (declared, stored) => Err(TsCellError::TypeMismatch {
            column: column.to_string(),
            stored: stored_kind(stored),
            declared,
        }),
    }
}

/// The typed value of a decimal, UUID or boolean cell. `None` for any other
/// cell, which its emitter writes in its own shape.
pub(in crate::data::executor) fn exact_cell_value(cell: TsCell<'_>) -> Option<nodedb_types::Value> {
    use crate::engine::timeseries::row_cells::{bool_value, decimal_value, uuid_value};
    match cell {
        TsCell::Decimal(d) => Some(decimal_value(Some(d))),
        TsCell::Uuid(bytes) => Some(uuid_value(Some(bytes))),
        TsCell::Bool(b) => Some(bool_value(Some(b))),
        TsCell::Null | TsCell::Time(..) | TsCell::Float(_) | TsCell::Int(_) | TsCell::Symbol(_) => {
            None
        }
    }
}

/// The name of the data shape a column holds.
fn stored_kind(data: &ColumnData) -> &'static str {
    match data {
        ColumnData::Timestamp(_) => "timestamp",
        ColumnData::Float64(_) => "float64",
        ColumnData::Int64(_) => "int64",
        ColumnData::Symbol(_) => "symbol",
        ColumnData::DictEncoded { .. } => "dictionary-encoded",
        ColumnData::Decimal(_) => "decimal",
        ColumnData::Uuid(_) => "uuid",
        ColumnData::Bool(_) => "bool",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_column_of_the_wrong_shape_is_an_error() {
        let data = ColumnData::Float64(vec![1.0]);
        assert!(matches!(
            read_ts_cell(&data, ColumnType::Int64, "n", None, 0),
            Err(TsCellError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn a_row_past_the_column_is_an_error() {
        let data = ColumnData::Int64(vec![1]);
        assert!(matches!(
            read_ts_cell(&data, ColumnType::Int64, "n", None, 1),
            Err(TsCellError::RowOutOfRange { row: 1, .. })
        ));
    }

    #[test]
    fn a_symbol_the_dictionary_lacks_is_an_error_and_the_null_id_is_null() {
        let data = ColumnData::Symbol(vec![7, NULL_SYMBOL_ID]);
        assert!(matches!(
            read_ts_cell(&data, ColumnType::Symbol, "tag", None, 0),
            Err(TsCellError::UnknownSymbol { id: 7, .. })
        ));
        assert_eq!(
            read_ts_cell(&data, ColumnType::Symbol, "tag", None, 1).expect("null"),
            TsCell::Null
        );
    }

    #[test]
    fn a_nan_float_is_null() {
        let data = ColumnData::Float64(vec![f64::NAN, 2.5]);
        assert_eq!(
            read_ts_cell(&data, ColumnType::Float64, "v", None, 0).expect("null"),
            TsCell::Null
        );
        assert_eq!(
            read_ts_cell(&data, ColumnType::Float64, "v", None, 1).expect("float"),
            TsCell::Float(2.5)
        );
    }
}
