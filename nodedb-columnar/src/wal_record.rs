// SPDX-License-Identifier: Apache-2.0

//! WAL record types for columnar operations.
//!
//! Each mutation (INSERT, DELETE, memtable flush) produces a WAL record
//! that is written before the mutation is applied. On crash recovery, WAL
//! records are replayed to reconstruct the memtable, delete bitmaps, and
//! segment metadata.
//!
//! Records are serialized as MessagePack for compact wire representation.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;
use serde::{Deserialize, Serialize};
use sonic_rs;
use zerompk::{FromMessagePack, ToMessagePack};

/// A WAL record for a columnar collection operation.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ColumnarWalRecord {
    /// A row was inserted into the memtable.
    ///
    /// Contains the collection name and the row data as packed binary
    /// (the columnar wire format, not MessagePack). On replay, the row
    /// is re-inserted into the memtable.
    #[serde(rename = "insert_row")]
    InsertRow {
        collection: String,
        /// Row data as packed binary values. Each value is encoded per its
        /// column type: i64 as 8 LE bytes, f64 as 8 LE bytes, strings as
        /// length-prefixed UTF-8, etc.
        row_data: Vec<u8>,
        /// The system-time ordinal the row's version starts at.
        sys_from: i64,
    },

    /// Rows were ended: marked in a segment's delete bitmap and stamped
    /// with the ordinal the ending write took.
    ///
    /// On replay, these row indices are re-applied to the segment's
    /// delete bitmap and end at `sys_to`.
    #[serde(rename = "delete_rows")]
    DeleteRows {
        collection: String,
        segment_id: u64,
        row_indices: Vec<u32>,
        /// The system-time ordinal the rows end at.
        sys_to: i64,
    },

    /// The memtable was flushed to a new segment.
    ///
    /// On replay, if the segment file exists, update metadata to include it.
    /// If it doesn't exist, the flush was interrupted; rows are already in
    /// the memtable via InsertRow records.
    #[serde(rename = "memtable_flushed")]
    MemtableFlushed {
        collection: String,
        segment_id: u64,
        row_count: u64,
    },
}

impl ColumnarWalRecord {
    /// Collection name this record belongs to.
    pub fn collection(&self) -> &str {
        match self {
            Self::InsertRow { collection, .. }
            | Self::DeleteRows { collection, .. }
            | Self::MemtableFlushed { collection, .. } => collection,
        }
    }

    /// Serialize the record to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::error::ColumnarError> {
        zerompk::to_msgpack_vec(self)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }

    /// Deserialize a record from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ColumnarError> {
        zerompk::from_msgpack(data)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }
}

/// Encode a row of values into the columnar wire format for WAL records.
///
/// Each value is written as: [type_tag: u8][value_bytes].
/// This is more compact than MessagePack for typed columns and enables
/// direct replay into the memtable without schema interpretation overhead.
pub fn encode_row_for_wal(
    values: &[nodedb_types::value::Value],
) -> Result<Vec<u8>, crate::error::ColumnarError> {
    use nodedb_types::value::Value;

    let mut buf = Vec::with_capacity(values.len() * 10); // Rough estimate.

    for value in values {
        match value {
            Value::Null => buf.push(0),
            Value::Integer(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Bool(v) => {
                buf.push(3);
                buf.push(*v as u8);
            }
            Value::String(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Bytes(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::DateTime(dt) => {
                buf.push(6);
                buf.extend_from_slice(&dt.micros.to_le_bytes());
            }
            Value::Decimal(d) => {
                buf.push(7);
                buf.extend_from_slice(&d.serialize());
            }
            Value::Uuid(s) => {
                buf.push(8);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Array(arr) if arr.iter().all(|v| matches!(v, Value::Float(_))) => {
                // An all-float array: tag(9) + count(u32) + f64 values, so
                // every element decodes to the value it was.
                buf.push(9);
                buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                for v in arr {
                    if let Value::Float(f) = v {
                        buf.extend_from_slice(&f.to_le_bytes());
                    }
                }
            }
            _ => {
                // Any other array, geometry and other complex types: JSON
                // bytes.
                buf.push(10);
                let json = sonic_rs::to_vec(value).map_err(|e| {
                    crate::error::ColumnarError::Serialization(format!(
                        "failed to serialize value as JSON: {e}"
                    ))
                })?;
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(&json);
            }
        }
    }

    Ok(buf)
}

/// Maximum length for a variable-length field in a WAL record (256 MiB).
/// Prevents OOM from crafted/corrupt records with bogus length prefixes.
const MAX_FIELD_LEN: usize = 256 * 1024 * 1024;

/// The error for a WAL row that stops decoding at byte `offset`.
fn corrupt(offset: usize, reason: impl Into<String>) -> crate::error::ColumnarError {
    crate::error::ColumnarError::WalRowCorrupt {
        offset,
        reason: reason.into(),
    }
}

/// Read exactly `N` bytes from `data` at `cursor` as an array, advancing
/// cursor. Returns `Err` if not enough bytes remain.
fn read_array<const N: usize>(
    data: &[u8],
    cursor: &mut usize,
    context: &str,
) -> Result<[u8; N], crate::error::ColumnarError> {
    let at = *cursor;
    let slice = read_slice(data, cursor, N, context)?;
    slice
        .try_into()
        .map_err(|_| corrupt(at, format!("truncated {context}")))
}

/// Read exactly `n` bytes from `data` at `cursor`, advancing cursor.
/// Returns `Err` if not enough bytes remain.
fn read_slice<'a>(
    data: &'a [u8],
    cursor: &mut usize,
    n: usize,
    context: &str,
) -> Result<&'a [u8], crate::error::ColumnarError> {
    let end = cursor
        .checked_add(n)
        .ok_or_else(|| corrupt(*cursor, format!("overflow in {context}")))?;
    if end > data.len() {
        return Err(corrupt(
            *cursor,
            format!(
                "truncated {context}: need {n} bytes, have {}",
                data.len().saturating_sub(*cursor)
            ),
        ));
    }
    let slice = &data[*cursor..end];
    *cursor = end;
    Ok(slice)
}

/// Read a u32 length prefix, validate it against MAX_FIELD_LEN, then read
/// that many bytes. Returns the payload slice.
fn read_length_prefixed<'a>(
    data: &'a [u8],
    cursor: &mut usize,
    context: &str,
) -> Result<&'a [u8], crate::error::ColumnarError> {
    let at = *cursor;
    let len = u32::from_le_bytes(read_array::<4>(data, cursor, context)?) as usize;
    if len > MAX_FIELD_LEN {
        return Err(corrupt(
            at,
            format!("{context} length {len} exceeds maximum {MAX_FIELD_LEN}"),
        ));
    }
    read_slice(data, cursor, len, context)
}

/// Read a length-prefixed UTF-8 string. Bytes that are not UTF-8 are an
/// error, never replacement characters.
fn read_utf8(
    data: &[u8],
    cursor: &mut usize,
    context: &str,
) -> Result<String, crate::error::ColumnarError> {
    let at = *cursor;
    let bytes = read_length_prefixed(data, cursor, context)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| corrupt(at, format!("{context} is not UTF-8: {e}")))
}

/// Decode a row from the columnar wire format back into Values.
///
/// `Err(WalRowCorrupt)` when the bytes do not decode. The corruption is
/// reported here, where it is detected.
pub fn decode_row_from_wal(
    data: &[u8],
) -> Result<Vec<nodedb_types::value::Value>, crate::error::ColumnarError> {
    decode_row(data).inspect_err(crate::diag::wal_row_corrupt)
}

/// The body of [`decode_row_from_wal`].
fn decode_row(data: &[u8]) -> Result<Vec<nodedb_types::value::Value>, crate::error::ColumnarError> {
    use nodedb_types::value::Value;

    let mut values = Vec::new();
    let mut cursor = 0;

    while cursor < data.len() {
        let tag_at = cursor;
        let [tag] = read_array::<1>(data, &mut cursor, "tag")?;

        let value = match tag {
            0 => Value::Null,
            1 => Value::Integer(i64::from_le_bytes(read_array(data, &mut cursor, "i64")?)),
            2 => Value::Float(f64::from_le_bytes(read_array(data, &mut cursor, "f64")?)),
            3 => {
                let [b] = read_array::<1>(data, &mut cursor, "bool")?;
                Value::Bool(b != 0)
            }
            4 => Value::String(read_utf8(data, &mut cursor, "string")?),
            5 => Value::Bytes(read_length_prefixed(data, &mut cursor, "bytes")?.to_vec()),
            8 => Value::Uuid(read_utf8(data, &mut cursor, "uuid")?),
            6 => {
                let micros = i64::from_le_bytes(read_array(data, &mut cursor, "timestamp")?);
                Value::DateTime(nodedb_types::datetime::NdbDateTime::from_micros(micros))
            }
            7 => Value::Decimal(rust_decimal::Decimal::deserialize(read_array(
                data,
                &mut cursor,
                "decimal",
            )?)),
            9 => {
                let count_at = cursor;
                let count =
                    u32::from_le_bytes(read_array(data, &mut cursor, "vector count")?) as usize;
                let remaining_values = data.len().saturating_sub(cursor) / 8;
                let max_count = (MAX_FIELD_LEN / 8).min(remaining_values);
                if count > max_count {
                    return Err(corrupt(
                        count_at,
                        format!("vector count {count} exceeds maximum {max_count}"),
                    ));
                }
                let capacity = checked_decode_capacity(
                    count,
                    size_of::<nodedb_types::value::Value>(),
                    data.len().saturating_sub(cursor),
                    8,
                    max_count,
                    usize::MAX,
                )
                .ok_or_else(|| {
                    corrupt(count_at, "vector count exceeds decode allocation bounds")
                })?;
                let mut arr = Vec::with_capacity(capacity);
                for _ in 0..count {
                    let f = f64::from_le_bytes(read_array(data, &mut cursor, "vector f64")?);
                    arr.push(Value::Float(f));
                }
                Value::Array(arr)
            }
            10 => {
                let json_at = cursor;
                let json_bytes = read_length_prefixed(data, &mut cursor, "json")?;
                sonic_rs::from_slice(json_bytes)
                    .map_err(|e| corrupt(json_at, format!("json value does not decode: {e}")))?
            }
            _ => return Err(corrupt(tag_at, format!("unknown WAL value tag: {tag}"))),
        };

        values.push(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use nodedb_types::datetime::NdbDateTime;
    use nodedb_types::value::Value;

    use super::*;

    #[test]
    fn rejects_huge_vector_count_with_tiny_payload_before_allocation() {
        // tag + one value whose vector count claims nearly u32::MAX values.
        let mut bytes = vec![9];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_row_from_wal(&bytes),
            Err(crate::error::ColumnarError::WalRowCorrupt { .. })
        ));
    }

    #[test]
    fn a_string_that_is_not_utf8_is_refused() {
        let mut bytes = vec![4];
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&[0xC3, 0x28]);
        assert!(matches!(
            decode_row_from_wal(&bytes),
            Err(crate::error::ColumnarError::WalRowCorrupt { offset: 1, .. })
        ));
    }

    #[test]
    fn a_uuid_that_is_not_utf8_is_refused() {
        let mut bytes = vec![8];
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(0xFF);
        assert!(matches!(
            decode_row_from_wal(&bytes),
            Err(crate::error::ColumnarError::WalRowCorrupt { .. })
        ));
    }

    #[test]
    fn a_json_value_that_does_not_decode_is_refused() {
        let json = b"{not json";
        let mut bytes = vec![10];
        bytes.extend_from_slice(&(json.len() as u32).to_le_bytes());
        bytes.extend_from_slice(json);
        assert!(matches!(
            decode_row_from_wal(&bytes),
            Err(crate::error::ColumnarError::WalRowCorrupt { .. })
        ));
    }

    #[test]
    fn arrays_decode_to_the_values_they_were() {
        let values = vec![
            Value::Array(vec![Value::Float(0.1), Value::Float(-2.5)]),
            Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
        ];
        let encoded = encode_row_for_wal(&values).expect("encode");
        assert_eq!(decode_row_from_wal(&encoded).expect("decode"), values);
    }

    #[test]
    fn wal_record_roundtrip() {
        let records = vec![
            ColumnarWalRecord::InsertRow {
                collection: "test".into(),
                row_data: vec![1, 2, 3],
                sys_from: 7,
            },
            ColumnarWalRecord::DeleteRows {
                collection: "test".into(),
                segment_id: 0,
                row_indices: vec![5, 10, 15],
                sys_to: 9,
            },
            ColumnarWalRecord::MemtableFlushed {
                collection: "test".into(),
                segment_id: 3,
                row_count: 1024,
            },
        ];

        for record in &records {
            let bytes = record.to_bytes().expect("serialize");
            let restored = ColumnarWalRecord::from_bytes(&bytes).expect("deserialize");
            assert_eq!(restored.collection(), record.collection());
        }
    }

    #[test]
    fn row_wire_format_roundtrip() {
        let values = vec![
            Value::Integer(42),
            Value::Float(0.75),
            Value::Bool(true),
            Value::String("hello".into()),
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000)),
            Value::Decimal(rust_decimal::Decimal::new(314, 2)),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::Null,
            Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]),
        ];

        let encoded = encode_row_for_wal(&values).expect("encode");
        let decoded = decode_row_from_wal(&encoded).expect("decode");

        assert_eq!(decoded.len(), values.len());
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Float(0.75));
        assert_eq!(decoded[2], Value::Bool(true));
        assert_eq!(decoded[3], Value::String("hello".into()));
        assert_eq!(decoded[4], Value::Bytes(vec![0xDE, 0xAD]));
        assert_eq!(
            decoded[5],
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000))
        );
        assert_eq!(
            decoded[7],
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(decoded[8], Value::Null);
    }

    #[test]
    fn decode_truncated_i64_returns_error() {
        // Tag 1 (i64) requires 8 payload bytes; supply none.
        // A slice index `data[cursor..cursor+8]` here would panic with an
        // index out-of-bounds. `try_into()` must return the
        // Serialization error instead.
        let result = decode_row_from_wal(&[1]);
        assert!(
            result.is_err(),
            "truncated i64 payload must return Err, not panic"
        );
    }

    #[test]
    fn decode_truncated_string_returns_error() {
        // Tag 4 (string): length prefix says 255 bytes but the slice ends
        // immediately after the 4-byte length field. The read of
        // `data[cursor..cursor+255]` must error, not panic.
        let input = {
            let mut v = vec![4u8]; // tag = string
            v.extend_from_slice(&255u32.to_le_bytes()); // len = 255
            // no payload bytes follow
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "truncated string payload must return Err, not panic"
        );
    }

    #[test]
    fn decode_huge_vector_count_returns_error() {
        // Tag 9 (vector array): count = 0x7FFFFFFF. After reading the count,
        // the very first iteration tries to read 4 bytes of f32 from an empty
        // slice. The loop must error out cleanly there, before any allocation
        // proportional to count is attempted, rather than panic.
        let input = {
            let mut v = vec![9u8]; // tag = vector array
            v.extend_from_slice(&0x7FFF_FFFFu32.to_le_bytes()); // count
            // no f32 bytes follow
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "huge vector count with no payload must return Err, not panic or OOM"
        );
    }

    #[test]
    fn decode_truncated_decimal_returns_error() {
        // Tag 7 (Decimal) requires 16 bytes; supply only 4.
        // `data[cursor..cursor+16]` must error, not panic.
        let input = {
            let mut v = vec![7u8]; // tag = decimal
            v.extend_from_slice(&[0u8; 4]); // only 4 bytes, need 16
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "truncated decimal payload must return Err, not panic"
        );
    }
}
