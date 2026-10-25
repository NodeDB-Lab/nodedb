// SPDX-License-Identifier: Apache-2.0

//! Read-only access methods on `ColumnData`: validity checks, value extraction.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;
use nodedb_types::value_from_msgpack;

use super::types::ColumnData;
use crate::error::ColumnarError;

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
    ///
    /// Returns `MemtableCellCorrupt` for a cell whose bytes do not hold a
    /// value of its type: a JSON cell that is not MessagePack, a text cell
    /// that is not UTF-8, or a dictionary ID outside the dictionary.
    /// `column` names the column in that error.
    pub(crate) fn get_value(
        &self,
        row: usize,
        declared: &ColumnType,
        column: &str,
    ) -> Result<Value, ColumnarError> {
        if self.is_null(row) {
            return Ok(Value::Null);
        }
        let corrupt = |reason: String| ColumnarError::MemtableCellCorrupt {
            column: column.to_string(),
            row,
            reason,
        };
        let value = match self {
            Self::Int64 { values, .. } => Value::Integer(values[row]),
            Self::Float64 { values, .. } => Value::Float(values[row]),
            Self::Bool { values, .. } => Value::Bool(values[row]),
            Self::Timestamp { values, .. } => declared.time_cell(values[row]),
            Self::Decimal { values, .. } => {
                Value::Decimal(rust_decimal::Decimal::deserialize(values[row]))
            }
            Self::Uuid { values, .. } => match declared {
                ColumnType::Ulid => Value::Ulid(ulid::Ulid::from_bytes(values[row]).to_string()),
                _ => Value::Uuid(uuid::Uuid::from_bytes(values[row]).to_string()),
            },
            Self::String { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                Value::String(
                    utf8_cell(&data[start..end])
                        .map_err(|e| corrupt(format!("text cell is not UTF-8: {e}")))?,
                )
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
                    value_from_msgpack(slice)
                        .map_err(|e| corrupt(format!("JSON cell is not MessagePack: {e}")))?
                }
            }
            Self::Geometry { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                nodedb_spatial::value_of_ewkb(&data[start..end])
                    .map_err(|e| corrupt(crate::geometry_cell::not_ewkb(e)))?
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
                let id = ids[row];
                let text = usize::try_from(id)
                    .ok()
                    .and_then(|i| dictionary.get(i))
                    .ok_or_else(|| {
                        corrupt(format!(
                            "dictionary ID {id} is outside a dictionary of {} entries",
                            dictionary.len()
                        ))
                    })?;
                Value::String(text.clone())
            }
        };
        Ok(value)
    }
}

/// The text of a string or geometry cell, or the UTF-8 error of its bytes.
fn utf8_cell(bytes: &[u8]) -> Result<String, std::str::Utf8Error> {
    std::str::from_utf8(bytes).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnType;
    use nodedb_types::value::Value;

    use super::super::types::ColumnData;
    use crate::error::ColumnarError;

    fn corrupt_reason(result: Result<Value, ColumnarError>) -> String {
        match result {
            Err(ColumnarError::MemtableCellCorrupt {
                column,
                row,
                reason,
            }) => {
                assert_eq!(column, "c");
                assert_eq!(row, 0);
                reason
            }
            other => panic!("expected MemtableCellCorrupt, got {other:?}"),
        }
    }

    #[test]
    fn a_json_cell_that_is_not_msgpack_is_refused_not_null() {
        // 0xC1 is the one MessagePack marker that is never valid.
        let col = ColumnData::Json {
            data: vec![0xC1],
            offsets: vec![0, 1],
            valid: None,
        };
        let reason = corrupt_reason(col.get_value(0, &ColumnType::Json, "c"));
        assert!(reason.contains("MessagePack"), "{reason}");
    }

    #[test]
    fn a_valid_json_cell_reads_back() {
        let bytes = nodedb_types::value_to_msgpack(&Value::Integer(7)).expect("encode");
        let col = ColumnData::Json {
            offsets: vec![0, bytes.len() as u32],
            data: bytes,
            valid: None,
        };
        assert_eq!(
            col.get_value(0, &ColumnType::Json, "c").expect("read"),
            Value::Integer(7)
        );
    }

    #[test]
    fn a_text_cell_that_is_not_utf8_is_refused() {
        let col = ColumnData::String {
            data: vec![0xFF, 0xFE],
            offsets: vec![0, 2],
            valid: None,
        };
        let reason = corrupt_reason(col.get_value(0, &ColumnType::String, "c"));
        assert!(reason.contains("UTF-8"), "{reason}");
    }

    #[test]
    fn a_dictionary_id_outside_the_dictionary_is_refused() {
        let col = ColumnData::DictEncoded {
            ids: vec![3],
            dictionary: vec!["a".into()],
            reverse: std::collections::HashMap::new(),
            valid: None,
        };
        let reason = corrupt_reason(col.get_value(0, &ColumnType::String, "c"));
        assert!(reason.contains("dictionary ID 3"), "{reason}");
    }
}
