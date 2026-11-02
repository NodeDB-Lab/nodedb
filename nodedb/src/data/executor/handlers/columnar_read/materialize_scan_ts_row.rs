// SPDX-License-Identifier: BUSL-1.1

//! The values a timeseries materialize-scan row emits.
//!
//! Every timeseries row stores `_ts_system`: the system-time ordinal of the
//! batch that wrote it. A bitemporal collection (one with `_ts_valid_from`)
//! shows it as the ordinal's millisecond. Every other collection drops it:
//! there it is the engine's version stamp, not a declared column. Every row
//! drops `_ts_series`, the series id the engine stamps.

use std::collections::HashMap;

use nodedb_types::columnar::schema::{TS_SERIES, TS_VALID_FROM};
use nodedb_types::value::Value;

use crate::data::executor::handlers::timeseries::cell_read::TsCell;
use crate::engine::timeseries::columnar_memtable::ColumnType;

/// How an emitted row carries `_ts_system`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowShape {
    /// Index of the `_ts_system` column, `None` when the schema has none.
    pub(super) system_idx: Option<usize>,
    /// True when the schema carries `_ts_valid_from`. A bitemporal row
    /// shows `_ts_system` as a millisecond. Every other row drops it.
    pub(super) bitemporal: bool,
    /// Index of the `_ts_series` column, which every row drops.
    pub(super) series_idx: Option<usize>,
}

impl RowShape {
    pub(super) fn of(columns: &[(String, ColumnType)], system_idx: Option<usize>) -> Self {
        Self {
            system_idx,
            bitemporal: columns.iter().any(|(name, _)| name == TS_VALID_FROM),
            series_idx: columns.iter().position(|(name, _)| name == TS_SERIES),
        }
    }

    /// The value an emitted row carries for column `col_idx`, `None` when
    /// the row drops the column. `cell` reads the column's cell.
    pub(super) fn emitted_value<'c>(
        self,
        col_idx: usize,
        col_name: &str,
        cell: impl FnOnce() -> crate::Result<TsCell<'c>>,
        stamp: impl FnOnce() -> crate::Result<i64>,
    ) -> crate::Result<Option<Value>> {
        if self.series_idx == Some(col_idx) {
            return Ok(None);
        }
        if self.system_idx != Some(col_idx) {
            return cell_to_value(cell()?, col_name).map(Some);
        }
        if !self.bitemporal {
            return Ok(None);
        }
        Ok(Some(Value::Integer(nodedb_types::ordinal_to_ms(stamp()?))))
    }
}

/// Serialize one row map to msgpack bytes.
pub(super) fn encode_row_map(
    map: HashMap<String, Value>,
    row_idx: usize,
) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_msgpack(&Value::Object(map)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("timeseries materialize scan row {row_idx}: {e}"),
    })
}

/// The error for a stored millisecond count that no instant can carry.
fn instant_read_error(column: &str, e: nodedb_types::NdbDateTimeError) -> crate::Error {
    crate::Error::Internal {
        detail: format!("timeseries column {column}: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Column-to-Value converters
// ---------------------------------------------------------------------------

/// The `Value` a timeseries cell of column `column` emits.
fn cell_to_value(cell: TsCell<'_>, column: &str) -> crate::Result<Value> {
    Ok(match cell {
        TsCell::Null => Value::Null,
        TsCell::Time(kind, millis) => kind
            .cell_value(millis)
            .map_err(|e| instant_read_error(column, e))?,
        TsCell::Float(f) => Value::Float(f),
        TsCell::Int(n) => Value::Integer(n),
        TsCell::Symbol(s) => Value::String(s.to_string()),
    })
}

/// The system-time ordinal a `_ts_system` cell holds. `None` for any cell
/// other than a time or an integer.
pub(super) fn system_time(cell: TsCell<'_>) -> Option<i64> {
    match cell {
        TsCell::Time(_, millis) | TsCell::Int(millis) => Some(millis),
        TsCell::Null | TsCell::Float(_) | TsCell::Symbol(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_UNTIL};

    fn columns(names: &[&str]) -> Vec<(String, ColumnType)> {
        names
            .iter()
            .map(|n| ((*n).to_string(), ColumnType::Int64))
            .collect()
    }

    fn emit(shape: RowShape, col_idx: usize, ordinal: i64) -> Option<Value> {
        shape
            .emitted_value(col_idx, "c", || Ok(TsCell::Int(ordinal)), || Ok(ordinal))
            .expect("emitted value")
    }

    #[test]
    fn plain_rows_drop_the_system_stamp() {
        let cols = columns(&["ts", "v", TS_SYSTEM]);
        let shape = RowShape::of(&cols, Some(2));
        assert!(!shape.bitemporal);
        let ordinal = nodedb_types::ms_to_ordinal_upper(1_000) - 7;
        assert_eq!(emit(shape, 2, ordinal), None);
        assert_eq!(emit(shape, 1, 42), Some(Value::Integer(42)));
    }

    #[test]
    fn bitemporal_rows_show_the_stamp_as_milliseconds() {
        let cols = columns(&["ts", TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL]);
        let shape = RowShape::of(&cols, Some(1));
        assert!(shape.bitemporal);
        let ordinal = nodedb_types::ms_to_ordinal_upper(1_000) - 7;
        assert_eq!(emit(shape, 1, ordinal), Some(Value::Integer(1_000)));
    }
}
