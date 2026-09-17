// SPDX-License-Identifier: Apache-2.0

//! Msgpack → `nodedb_types::Value` reader.
//!
//! [`read_native_value`] is the one decoder of a plain (untagged) msgpack
//! value. It is generic over `zerompk::Read`, so `value_from_msgpack` (slice
//! entry point) and `NativeCell::read` (inside a zerompk derive) share it.
//!
//! Instant ext values (`fixext8` type 1 / 2) become `Value::DateTime` /
//! `Value::NaiveDateTime`. Every other ext marker, and a `fixext8` of any
//! other type, becomes `Value::Null`. Map keys that are not strings are
//! rendered with `Debug`.

use zerompk::{Read, SliceReader};

use super::super::error::{MsgpackError, MsgpackResult};
use super::super::instant_ext::instant_from_ext;
use crate::Value;

const FIXEXT8: u8 = 0xD7;

/// Deserialize a `nodedb_types::Value` from standard MessagePack bytes.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected.
pub fn value_from_msgpack(bytes: &[u8]) -> MsgpackResult<Value> {
    let mut reader = SliceReader::new(bytes);
    let value = read_native_value(&mut reader)?;
    if reader.peek_marker().is_ok() {
        return Err(MsgpackError::TrailingBytes {
            consumed: consumed_len(bytes),
            total: bytes.len(),
        });
    }
    Ok(value)
}

/// Length of the top-level value at the start of `bytes`.
///
/// `SliceReader` exposes no position, so the length is found by binary
/// search over prefix lengths: decoding a prefix shorter than the value fails
/// on the missing bytes, and decoding any prefix at least as long as the value
/// succeeds. Called only on the trailing-bytes error path, after a full decode
/// of `bytes` succeeded.
fn consumed_len(bytes: &[u8]) -> usize {
    let (mut lo, mut hi) = (0usize, bytes.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match read_native_value(&mut SliceReader::new(&bytes[..mid])) {
            Ok(_) => hi = mid,
            Err(_) => lo = mid + 1,
        }
    }
    lo
}

/// Read one plain msgpack value, dispatching on the peeked marker.
pub(crate) fn read_native_value<'de, R: Read<'de>>(reader: &mut R) -> zerompk::Result<Value> {
    let marker = reader.peek_marker()?;
    match marker {
        0xC0 => {
            reader.read_nil()?;
            Ok(Value::Null)
        }
        0xC2 | 0xC3 => Ok(Value::Bool(reader.read_boolean()?)),
        0x00..=0x7F | 0xE0..=0xFF | 0xD0..=0xD3 => Ok(Value::Integer(reader.read_i64()?)),
        0xCC..=0xCF => Ok(Value::Integer(reader.read_u64()? as i64)),
        0xCA => Ok(Value::Float(f64::from(reader.read_f32()?))),
        0xCB => Ok(Value::Float(reader.read_f64()?)),
        0xA0..=0xBF | 0xD9..=0xDB => Ok(Value::String(reader.read_string()?.into_owned())),
        0xC4..=0xC6 => Ok(Value::Bytes(reader.read_binary()?.into_owned())),
        0x90..=0x9F | 0xDC | 0xDD => {
            let len = reader.read_array_len()?;
            reader.increment_depth()?;
            let mut arr = Vec::with_capacity(len.min(4096));
            for _ in 0..len {
                arr.push(read_native_value(reader)?);
            }
            reader.decrement_depth();
            Ok(Value::Array(arr))
        }
        0x80..=0x8F | 0xDE | 0xDF => {
            let len = reader.read_map_len()?;
            reader.increment_depth()?;
            let mut map = std::collections::HashMap::with_capacity(len.min(4096));
            for _ in 0..len {
                let key = read_native_key(reader)?;
                map.insert(key, read_native_value(reader)?);
            }
            reader.decrement_depth();
            Ok(Value::Object(map))
        }
        0xC7..=0xC9 | 0xD4..=0xD8 => {
            let (ext_type, payload) = reader.read_ext()?;
            let instant = if marker == FIXEXT8 {
                instant_from_ext(ext_type, &payload)
            } else {
                None
            };
            Ok(match instant {
                Some((kind, micros)) => kind.from_micros(micros),
                None => Value::Null,
            })
        }
        other => Err(zerompk::Error::InvalidMarker(other)),
    }
}

/// Read a map key. A string key is taken as-is; any other value is rendered
/// with `Debug`.
fn read_native_key<'de, R: Read<'de>>(reader: &mut R) -> zerompk::Result<String> {
    match reader.peek_marker()? {
        0xA0..=0xBF | 0xD9..=0xDB => Ok(reader.read_string()?.into_owned()),
        _ => {
            let v = read_native_value(reader)?;
            Ok(format!("{v:?}"))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Roundtrip tests for the native reader against the native writer, and
    //! agreement between the `value_from_msgpack` and `NativeCell` entry points.

    use super::*;
    use crate::NdbDateTime;
    use crate::json_msgpack::NativeCell;
    use crate::json_msgpack::transcoder::msgpack_to_json_string;
    use crate::json_msgpack::writer::value_to_msgpack;

    fn through_cell(bytes: &[u8]) -> zerompk::Result<Value> {
        zerompk::from_msgpack::<NativeCell>(bytes).map(Value::from)
    }

    #[test]
    fn native_value_roundtrip() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String("host1".into()));
        map.insert("cpu".to_string(), Value::Float(0.75));
        map.insert("mem".to_string(), Value::Float(0.5));

        let row = Value::Object(map);
        let arr = Value::Array(vec![row]);

        let bytes = value_to_msgpack(&arr).unwrap();
        let decoded = value_from_msgpack(&bytes).unwrap();

        match &decoded {
            Value::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    Value::Object(m) => {
                        assert_eq!(m.len(), 3);
                        assert_eq!(m.get("id"), Some(&Value::String("host1".into())));
                        assert_eq!(m.get("cpu"), Some(&Value::Float(0.75)));
                        assert_eq!(m.get("mem"), Some(&Value::Float(0.5)));
                    }
                    other => panic!("expected Object, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn native_value_scalars() {
        let cases: Vec<Value> = vec![
            Value::Null,
            Value::Bool(true),
            Value::Integer(42),
            Value::Float(2.72),
            Value::String("hello".into()),
        ];
        for val in cases {
            let bytes = value_to_msgpack(&val).unwrap();
            let decoded = value_from_msgpack(&bytes).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn instant_roundtrip_both_kinds() {
        let dt = NdbDateTime::from_micros(1_710_498_600_000_000);
        for val in [
            Value::DateTime(dt),
            Value::NaiveDateTime(dt),
            Value::DateTime(NdbDateTime::from_micros(-1)),
        ] {
            let bytes = value_to_msgpack(&val).unwrap();
            assert_eq!(bytes.len(), 10);
            assert_eq!(bytes[0], 0xD7);
            assert_eq!(value_from_msgpack(&bytes).unwrap(), val);
        }

        let bytes = value_to_msgpack(&Value::NaiveDateTime(dt)).unwrap();
        assert_eq!(
            msgpack_to_json_string(&bytes).unwrap(),
            "\"2024-03-15T10:30:00.000000Z\""
        );
    }

    #[test]
    fn unknown_ext_is_null() {
        let bytes = [0xD7, 0x09, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(value_from_msgpack(&bytes).unwrap(), Value::Null);
    }

    #[test]
    fn unsigned_64_reads_as_integer() {
        let bytes = [0xCF, 0, 0, 0, 0, 0, 0, 0x01, 0x00];
        assert_eq!(through_cell(&bytes).unwrap(), Value::Integer(256));
        assert_eq!(value_from_msgpack(&bytes).unwrap(), Value::Integer(256));
    }

    #[test]
    fn unknown_ext_reads_as_null() {
        let bytes = [0xD4, 0x07, 0x00];
        assert_eq!(through_cell(&bytes).unwrap(), Value::Null);
        assert_eq!(value_from_msgpack(&bytes).unwrap(), Value::Null);
    }

    #[test]
    fn unused_marker_is_an_error() {
        assert!(through_cell(&[0xC1]).is_err());
        assert!(value_from_msgpack(&[0xC1]).is_err());
    }

    #[test]
    fn non_string_map_key_is_debug_rendered() {
        // {1: "a"}
        let bytes = [0x81, 0x01, 0xA1, b'a'];
        let expected = Value::Object(std::collections::HashMap::from([(
            "Integer(1)".to_string(),
            Value::String("a".into()),
        )]));
        assert_eq!(value_from_msgpack(&bytes).unwrap(), expected);
        assert_eq!(through_cell(&bytes).unwrap(), expected);
    }

    #[test]
    fn trailing_bytes_report_consumed_length() {
        let mut bytes = value_to_msgpack(&Value::Array(vec![
            Value::Integer(1),
            Value::String("ab".into()),
        ]))
        .unwrap();
        let len = bytes.len();
        bytes.extend_from_slice(&[0xC0, 0xC0]);
        match value_from_msgpack(&bytes) {
            Err(MsgpackError::TrailingBytes { consumed, total }) => {
                assert_eq!(consumed, len);
                assert_eq!(total, len + 2);
            }
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
    }

    #[test]
    fn both_entry_points_agree() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("k".to_string(), Value::Array(vec![Value::Integer(1)]));
        let mut outer = std::collections::HashMap::new();
        outer.insert("nested".to_string(), Value::Object(inner));
        outer.insert("list".to_string(), Value::Array(vec![Value::Null]));

        let mut cases: Vec<Vec<u8>> = [
            Value::Object(outer),
            Value::DateTime(NdbDateTime::from_micros(1_583_402_400_000_000)),
            Value::NaiveDateTime(NdbDateTime::from_micros(-86_400_000_000)),
            Value::Integer(-70_000),
            Value::Bytes(vec![0, 255, 7]),
        ]
        .iter()
        .map(|v| value_to_msgpack(v).unwrap())
        .collect();
        // u64 above i64::MAX
        cases.push(vec![0xCF, 0x80, 0, 0, 0, 0, 0, 0, 0x01]);
        // unknown ext (ext8, type 9, one payload byte)
        cases.push(vec![0xC7, 0x01, 0x09, 0x2A]);

        for bytes in cases {
            let a = value_from_msgpack(&bytes).unwrap();
            let b = through_cell(&bytes).unwrap();
            assert_eq!(a, b, "{bytes:02X?}");
        }
    }
}
