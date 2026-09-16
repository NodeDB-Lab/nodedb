// SPDX-License-Identifier: Apache-2.0

//! Read-only access methods on `ColumnData`: validity checks, value extraction.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;
use nodedb_types::value_from_msgpack;

use super::types::ColumnData;

impl ColumnData {
    /// Get the validity bitmap, or generate an all-true one for non-nullable columns.
    ///
    /// For segment writing — we always need a validity slice for the block encoder.
    /// Non-nullable columns return a freshly generated all-true vec (cheap, happens
    /// once per flush, not per row).
    pub(crate) fn validity_or_all_true(&self) -> std::borrow::Cow<'_, [bool]> {
        let valid_opt = match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Bool { valid, .. }
            | Self::Timestamp { valid, .. }
            | Self::Decimal { valid, .. }
            | Self::Uuid { valid, .. }
            | Self::String { valid, .. }
            | Self::Bytes { valid, .. }
            | Self::Json { valid, .. }
            | Self::Geometry { valid, .. }
            | Self::Vector { valid, .. }
            | Self::DictEncoded { valid, .. } => valid,
        };
        match valid_opt {
            Some(v) => std::borrow::Cow::Borrowed(v.as_slice()),
            None => std::borrow::Cow::Owned(vec![true; self.len()]),
        }
    }

    /// Check if a row is null (valid bitmap says false).
    ///
    /// Returns false (not null) for non-nullable columns (no bitmap).
    #[inline]
    pub(super) fn is_null(&self, row: usize) -> bool {
        let valid_opt = match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Bool { valid, .. }
            | Self::Timestamp { valid, .. }
            | Self::Decimal { valid, .. }
            | Self::Uuid { valid, .. }
            | Self::String { valid, .. }
            | Self::Bytes { valid, .. }
            | Self::Json { valid, .. }
            | Self::Geometry { valid, .. }
            | Self::Vector { valid, .. }
            | Self::DictEncoded { valid, .. } => valid,
        };
        valid_opt.as_ref().is_some_and(|v| !v[row])
    }

    /// Extract a single row's value as `nodedb_types::Value`.
    ///
    /// A time cell is typed by the column's declared type: an instant column
    /// yields `Value::NaiveDateTime` (`Timestamp`) or `Value::DateTime`
    /// (`Timestamptz`) from the stored epoch microseconds, and every other
    /// declared type backed by time storage (`SystemTimestamp`, `Duration`)
    /// yields the integer stored.
    pub(crate) fn get_value(&self, row: usize, declared: &ColumnType) -> Value {
        if self.is_null(row) {
            return Value::Null;
        }
        match self {
            Self::Int64 { values, .. } => Value::Integer(values[row]),
            Self::Float64 { values, .. } => Value::Float(values[row]),
            Self::Bool { values, .. } => Value::Bool(values[row]),
            Self::Timestamp { values, .. } => declared.time_cell(values[row]),
            Self::Decimal { values, .. } => {
                Value::Decimal(rust_decimal::Decimal::deserialize(values[row]))
            }
            Self::Uuid { values, .. } => {
                Value::Uuid(uuid::Uuid::from_bytes(values[row]).to_string())
            }
            Self::String { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = std::str::from_utf8(&data[start..end])
                    .unwrap_or("")
                    .to_string();
                Value::String(s)
            }
            Self::Bytes { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                Value::Bytes(data[start..end].to_vec())
            }
            Self::Json { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let slice = &data[start..end];
                if slice.is_empty() {
                    Value::Null
                } else {
                    value_from_msgpack(slice).unwrap_or(Value::Null)
                }
            }
            Self::Geometry { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = std::str::from_utf8(&data[start..end])
                    .unwrap_or("")
                    .to_string();
                Value::String(s)
            }
            Self::Vector { data, dim, .. } => {
                let d = *dim as usize;
                let start = row * d;
                let floats: Vec<Value> = data[start..start + d]
                    .iter()
                    .map(|&f| Value::Float(f as f64))
                    .collect();
                Value::Array(floats)
            }
            Self::DictEncoded {
                ids, dictionary, ..
            } => {
                let id = ids[row] as usize;
                if id < dictionary.len() {
                    Value::String(dictionary[id].clone())
                } else {
                    Value::Null
                }
            }
        }
    }
}
