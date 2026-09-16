// SPDX-License-Identifier: Apache-2.0

//! Msgpack → `nodedb_types::Value` reader.
//!
//! Instant ext values (fixext8 type 1 / 2) become `Value::DateTime` /
//! `Value::NaiveDateTime`. Every other ext type becomes `Value::Null`.

use super::super::error::MsgpackResult;
use super::super::instant_ext::instant_from_ext;
use super::cursor::Cursor;

/// Deserialize a `nodedb_types::Value` from standard MessagePack bytes.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected.
pub fn value_from_msgpack(bytes: &[u8]) -> MsgpackResult<crate::Value> {
    let mut cursor = Cursor::new(bytes);
    let value = read_native_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

fn read_native_value(c: &mut Cursor<'_>) -> zerompk::Result<crate::Value> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        0xC0 => Ok(crate::Value::Null),
        0xC2 => Ok(crate::Value::Bool(false)),
        0xC3 => Ok(crate::Value::Bool(true)),

        0x00..=0x7F => Ok(crate::Value::Integer(marker as i64)),
        0xE0..=0xFF => Ok(crate::Value::Integer(marker as i8 as i64)),

        0xCC => Ok(crate::Value::Integer(c.take()? as i64)),
        0xCD => Ok(crate::Value::Integer(c.read_u16_be()? as i64)),
        0xCE => Ok(crate::Value::Integer(c.read_u32_be()? as i64)),
        0xCF => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Integer(u64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ]) as i64))
        }

        0xD0 => Ok(crate::Value::Integer(c.take()? as i8 as i64)),
        0xD1 => {
            let b = c.take_n(2)?;
            Ok(crate::Value::Integer(
                i16::from_be_bytes([b[0], b[1]]) as i64
            ))
        }
        0xD2 => {
            let b = c.take_n(4)?;
            Ok(crate::Value::Integer(
                i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64,
            ))
        }
        0xD3 => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Integer(i64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }

        0xCA => {
            let b = c.take_n(4)?;
            Ok(crate::Value::Float(
                f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64,
            ))
        }
        0xCB => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Float(f64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }

        m @ 0xA0..=0xBF => read_native_str(c, (m & 0x1F) as usize),
        0xD9 => {
            let l = c.take()? as usize;
            read_native_str(c, l)
        }
        0xDA => {
            let l = c.read_u16_be()? as usize;
            read_native_str(c, l)
        }
        0xDB => {
            let l = c.read_u32_be()? as usize;
            read_native_str(c, l)
        }

        0xC4 => {
            let l = c.take()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }
        0xC5 => {
            let l = c.read_u16_be()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }
        0xC6 => {
            let l = c.read_u32_be()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }

        m @ 0x90..=0x9F => read_native_array(c, (m & 0x0F) as usize),
        0xDC => {
            let l = c.read_u16_be()? as usize;
            read_native_array(c, l)
        }
        0xDD => {
            let l = c.read_u32_be()? as usize;
            read_native_array(c, l)
        }

        m @ 0x80..=0x8F => read_native_map(c, (m & 0x0F) as usize),
        0xDE => {
            let l = c.read_u16_be()? as usize;
            read_native_map(c, l)
        }
        0xDF => {
            let l = c.read_u32_be()? as usize;
            read_native_map(c, l)
        }

        // fixext8: instants decode to their Value variant, other types to Null
        0xD7 => {
            let (ext_type, payload) = c.take_fixext8()?;
            Ok(match instant_from_ext(ext_type, payload) {
                Some((kind, micros)) => kind.from_micros(micros),
                None => crate::Value::Null,
            })
        }

        // other ext types — skip
        0xD4 => {
            c.take_n(2)?;
            Ok(crate::Value::Null)
        }
        0xD5 => {
            c.take_n(3)?;
            Ok(crate::Value::Null)
        }
        0xD6 => {
            c.take_n(5)?;
            Ok(crate::Value::Null)
        }
        0xD8 => {
            c.take_n(17)?;
            Ok(crate::Value::Null)
        }
        0xC7 => {
            let l = c.take()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }
        0xC8 => {
            let l = c.read_u16_be()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }
        0xC9 => {
            let l = c.read_u32_be()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }

        _ => Err(zerompk::Error::InvalidMarker(marker)),
    }
}

fn read_native_str(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    let bytes = c.take_n(len)?;
    let s = String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
    Ok(crate::Value::String(s))
}

fn read_native_array(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    c.depth += 1;
    let mut arr = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        arr.push(read_native_value(c)?);
    }
    c.depth -= 1;
    Ok(crate::Value::Array(arr))
}

fn read_native_map(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    c.depth += 1;
    let mut map = std::collections::HashMap::with_capacity(len.min(4096));
    for _ in 0..len {
        let key_marker = c.peek()?;
        let key = if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            match read_native_value(c)? {
                crate::Value::String(s) => s,
                other => format!("{other:?}"),
            }
        } else {
            let v = read_native_value(c)?;
            format!("{v:?}")
        };
        let val = read_native_value(c)?;
        map.insert(key, val);
    }
    c.depth -= 1;
    Ok(crate::Value::Object(map))
}

#[cfg(test)]
mod tests {
    //! Roundtrip tests for the native reader against the native writer.

    use super::*;
    use crate::NdbDateTime;
    use crate::json_msgpack::transcoder::msgpack_to_json_string;
    use crate::json_msgpack::writer::value_to_msgpack;

    #[test]
    fn native_value_roundtrip() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), crate::Value::String("host1".into()));
        map.insert("cpu".to_string(), crate::Value::Float(0.75));
        map.insert("mem".to_string(), crate::Value::Float(0.5));

        let row = crate::Value::Object(map);
        let arr = crate::Value::Array(vec![row]);

        let bytes = value_to_msgpack(&arr).unwrap();
        let decoded = value_from_msgpack(&bytes).unwrap();

        match &decoded {
            crate::Value::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    crate::Value::Object(m) => {
                        assert_eq!(m.len(), 3);
                        assert_eq!(m.get("id"), Some(&crate::Value::String("host1".into())));
                        assert_eq!(m.get("cpu"), Some(&crate::Value::Float(0.75)));
                        assert_eq!(m.get("mem"), Some(&crate::Value::Float(0.5)));
                    }
                    other => panic!("expected Object, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn native_value_scalars() {
        let cases: Vec<crate::Value> = vec![
            crate::Value::Null,
            crate::Value::Bool(true),
            crate::Value::Integer(42),
            crate::Value::Float(2.72),
            crate::Value::String("hello".into()),
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
            crate::Value::DateTime(dt),
            crate::Value::NaiveDateTime(dt),
            crate::Value::DateTime(NdbDateTime::from_micros(-1)),
        ] {
            let bytes = value_to_msgpack(&val).unwrap();
            assert_eq!(bytes.len(), 10);
            assert_eq!(bytes[0], 0xD7);
            assert_eq!(value_from_msgpack(&bytes).unwrap(), val);
        }

        let bytes = value_to_msgpack(&crate::Value::NaiveDateTime(dt)).unwrap();
        assert_eq!(
            msgpack_to_json_string(&bytes).unwrap(),
            "\"2024-03-15T10:30:00.000000Z\""
        );
    }

    #[test]
    fn unknown_ext_is_null() {
        let bytes = [0xD7, 0x09, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(value_from_msgpack(&bytes).unwrap(), crate::Value::Null);
    }
}
