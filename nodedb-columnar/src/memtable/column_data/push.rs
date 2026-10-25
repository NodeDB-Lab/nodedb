// SPDX-License-Identifier: Apache-2.0

//! Append owned values on `ColumnData`.
//!
//! A column accepts every shape the strict document coercion yields for its
//! declared type, so columnar and strict collections hold the same values.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;
use nodedb_types::{value_from_msgpack, value_to_msgpack};

use crate::error::ColumnarError;

use super::types::ColumnData;

/// The 16 stored bytes of an identifier column cell.
///
/// A `Ulid` column parses ULID text. Every other column backed by 16-byte
/// identifier storage parses UUID text. Text that does not parse is refused.
fn parse_id_bytes(
    s: &str,
    col_name: &str,
    col_type: &ColumnType,
) -> Result<[u8; 16], ColumnarError> {
    let parsed = match col_type {
        ColumnType::Ulid => ulid::Ulid::from_string(s).ok().map(|u| u.to_bytes()),
        _ => uuid::Uuid::parse_str(s).ok().map(|u| *u.as_bytes()),
    };
    parsed.ok_or_else(|| ColumnarError::TypeMismatch {
        column: col_name.to_string(),
        expected: col_type.to_string(),
    })
}

/// Encode a `Value` as MessagePack bytes for JSON/Array/Set/Record storage.
///
/// For `Value::String` input, the string is first parsed as JSON so that
/// downstream JSON path operators see a real structure rather than an opaque
/// string literal.
fn encode_as_msgpack(value: &Value, col_name: &str) -> Result<Vec<u8>, ColumnarError> {
    let to_encode: std::borrow::Cow<'_, Value> = match value {
        Value::String(s) => {
            let parsed = sonic_rs::from_str::<serde_json::Value>(s).map_err(|e| {
                ColumnarError::JsonParse {
                    column: col_name.to_string(),
                    source: e,
                }
            })?;
            std::borrow::Cow::Owned(Value::from(parsed))
        }
        other => std::borrow::Cow::Borrowed(other),
    };
    value_to_msgpack(&to_encode).map_err(|e| ColumnarError::MsgpackSerialize {
        column: col_name.to_string(),
        source: e,
    })
}

/// Parse a PostgreSQL range literal into a structured Value.
///
/// Accepts the four standard bound forms: `[lo,hi)`, `(lo,hi]`, `[lo,hi]`,
/// `(lo,hi)`.  The bounds are stored as string tokens so the caller can
/// interpret them as any scalar type.
fn parse_range_literal(s: &str, col_name: &str) -> Result<Vec<u8>, ColumnarError> {
    let s = s.trim();
    let (lower_inclusive, rest) = if let Some(r) = s.strip_prefix('[') {
        (true, r)
    } else if let Some(r) = s.strip_prefix('(') {
        (false, r)
    } else {
        return Err(ColumnarError::RangeParse {
            column: col_name.to_string(),
            literal: s.to_string(),
        });
    };

    let (body, upper_inclusive) = if let Some(b) = rest.strip_suffix(']') {
        (b, true)
    } else if let Some(b) = rest.strip_suffix(')') {
        (b, false)
    } else {
        return Err(ColumnarError::RangeParse {
            column: col_name.to_string(),
            literal: s.to_string(),
        });
    };

    let comma = body.find(',').ok_or_else(|| ColumnarError::RangeParse {
        column: col_name.to_string(),
        literal: s.to_string(),
    })?;
    let lower = body[..comma].trim().to_string();
    let upper = body[comma + 1..].trim().to_string();

    let mut map = std::collections::HashMap::new();
    map.insert("lower".to_string(), Value::String(lower));
    map.insert("upper".to_string(), Value::String(upper));
    map.insert("lower_inclusive".to_string(), Value::Bool(lower_inclusive));
    map.insert("upper_inclusive".to_string(), Value::Bool(upper_inclusive));
    let structured = Value::Object(map);

    value_to_msgpack(&structured).map_err(|e| ColumnarError::MsgpackSerialize {
        column: col_name.to_string(),
        source: e,
    })
}

impl ColumnData {
    /// Push a validity bit (if the column is nullable).
    #[inline(always)]
    pub(crate) fn push_valid(valid: &mut Option<Vec<bool>>, is_valid: bool) {
        if let Some(v) = valid {
            v.push(is_valid);
        }
    }

    /// Append a value. Returns error if type doesn't match.
    pub(crate) fn push(
        &mut self,
        value: &Value,
        col_name: &str,
        col_type: &ColumnType,
    ) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, Value::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, Value::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Decimal { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::Uuid { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Bytes { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Geometry { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Vector { data, dim, valid }, Value::Null) => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize));
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, Value::Integer(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Float(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Integer(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, Value::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::DateTime(dt))
            | (Self::Timestamp { values, valid }, Value::NaiveDateTime(dt)) => {
                values.push(dt.micros);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::Integer(micros)) => {
                values.push(*micros);
                Self::push_valid(valid, true);
            }
            (Self::Decimal { values, valid }, Value::Decimal(d)) => {
                values.push(d.serialize());
                Self::push_valid(valid, true);
            }
            (Self::Uuid { values, valid }, Value::Uuid(s) | Value::Ulid(s) | Value::String(s)) => {
                values.push(parse_id_bytes(s, col_name, col_type)?);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::Duration(d)) => {
                values.push(d.micros);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::Bytes(b),
            ) => {
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            // Bytes columns for Array/Set/Range/Record: accept string literals
            // by parsing them (JSON for Array/Set/Record, range syntax for Range).
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                let encoded = match col_type {
                    ColumnType::Range => parse_range_literal(s, col_name)?,
                    _ => encode_as_msgpack(value, col_name)?,
                };
                data.extend_from_slice(&encoded);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::Object(_) | Value::Array(_),
            ) => {
                let encoded = encode_as_msgpack(value, col_name)?;
                data.extend_from_slice(&encoded);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            // Json column: all value types — serialize as MessagePack.
            (Self::Json { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (
                Self::Json {
                    data,
                    offsets,
                    valid,
                },
                Value::Bytes(b),
            ) => {
                // Bytes are stored as an encoded MessagePack cell. Bytes that
                // do not decode are refused, so every stored JSON cell reads.
                value_from_msgpack(b).map_err(|e| ColumnarError::MsgpackDeserialize {
                    column: col_name.to_string(),
                    source: e,
                })?;
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Json {
                    data,
                    offsets,
                    valid,
                },
                _,
            ) => {
                // String → parse as JSON; Object/Array → encode directly.
                let encoded = encode_as_msgpack(value, col_name)?;
                data.extend_from_slice(&encoded);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            // EWKB, so the cell keeps its SRID, Z and M. A value that does
            // not parse or encode refuses the row.
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                value,
            ) => {
                let ewkb = crate::geometry_cell::encode_geometry_cell(value, col_name)?;
                data.extend_from_slice(&ewkb);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (Self::Vector { data, dim, valid }, Value::Array(arr)) => {
                let d = *dim as usize;
                for (i, v) in arr.iter().take(d).enumerate() {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    if i < d {
                        data.push(f);
                    }
                }
                for _ in arr.len()..d {
                    data.push(0.0);
                }
                Self::push_valid(valid, true);
            }
            // Packed little-endian `f32`s, the form strict coercion yields.
            (Self::Vector { data, dim, valid }, Value::Bytes(b))
                if b.len() == *dim as usize * 4 =>
            {
                data.extend(
                    b.as_chunks::<4>()
                        .0
                        .iter()
                        .map(|chunk| f32::from_le_bytes(*chunk)),
                );
                Self::push_valid(valid, true);
            }
            (Self::DictEncoded { ids, valid, .. }, Value::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(s.as_str()) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push(s.clone());
                    reverse.insert(s.clone(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, _) => {
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: other.type_name().to_string(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::NdbDuration;
    use nodedb_types::columnar::{ColumnDef, ColumnarSchema};

    use super::*;
    use crate::memtable::ColumnarMemtable;

    fn memtable(col_type: ColumnType) -> ColumnarMemtable {
        let schema =
            ColumnarSchema::new(vec![ColumnDef::required("c", col_type)]).expect("valid schema");
        ColumnarMemtable::new(&schema)
    }

    #[test]
    fn uuid_column_stores_uuid_text() {
        let mut mt = memtable(ColumnType::Uuid);
        let text = "67e55044-10b1-426f-9247-bb680e5fe0c8";
        mt.append_row(&[Value::String(text.into())])
            .expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![Value::Uuid(text.into())])
        );
    }

    #[test]
    fn uuid_column_refuses_text_that_is_not_a_uuid() {
        let mut mt = memtable(ColumnType::Uuid);
        let err = mt
            .append_row(&[Value::Uuid("not-a-uuid".into())])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::TypeMismatch { ref column, .. } if column == "c"));
        assert_eq!(mt.row_count(), 0);
    }

    #[test]
    fn ulid_column_round_trips_ulid_text() {
        let mut mt = memtable(ColumnType::Ulid);
        let text = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        mt.append_row(&[Value::String(text.into())])
            .expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![Value::Ulid(text.into())])
        );
    }

    #[test]
    fn string_column_accepts_identifier_text() {
        let mut mt = memtable(ColumnType::String);
        mt.append_row(&[Value::Uuid("u".into())]).expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![Value::String("u".into())])
        );
    }

    #[test]
    fn json_column_refuses_bytes_that_are_not_msgpack() {
        let mut mt = memtable(ColumnType::Json);
        let err = mt.append_row(&[Value::Bytes(vec![0xC1])]).unwrap_err();
        assert!(
            matches!(err, ColumnarError::MsgpackDeserialize { ref column, .. } if column == "c")
        );
        assert_eq!(mt.row_count(), 0);

        let encoded = nodedb_types::value_to_msgpack(&Value::Integer(9)).expect("encode");
        mt.append_row(&[Value::Bytes(encoded)]).expect("append");
        assert_eq!(mt.get_row(0).expect("read"), Some(vec![Value::Integer(9)]));
    }

    #[test]
    fn duration_column_stores_micros() {
        let mut mt = memtable(ColumnType::Duration);
        mt.append_row(&[Value::Duration(NdbDuration::from_micros(1_500))])
            .expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![Value::Integer(1_500)])
        );
    }

    #[test]
    fn vector_column_accepts_packed_floats() {
        let mut mt = memtable(ColumnType::Vector(2));
        let packed: Vec<u8> = [0.5f32, 1.25f32]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        mt.append_row(&[Value::Bytes(packed)]).expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![Value::Array(vec![
                Value::Float(0.5),
                Value::Float(1.25)
            ])])
        );

        let err = mt.append_row(&[Value::Bytes(vec![0; 3])]).unwrap_err();
        assert!(matches!(err, ColumnarError::TypeMismatch { .. }));
        assert_eq!(mt.row_count(), 1);
    }
}
