// SPDX-License-Identifier: Apache-2.0

//! Msgpack → `serde_json::Value` reader.
//!
//! Instant ext values (fixext8 type 1 / 2) become ISO 8601 strings. Every
//! other ext type becomes `null`.

use super::super::error::MsgpackResult;
use super::super::instant_ext::instant_from_ext;
use super::cursor::Cursor;
use crate::datetime::NdbDateTime;

/// Deserialize a `serde_json::Value` from MessagePack bytes.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected.
pub fn json_from_msgpack(bytes: &[u8]) -> MsgpackResult<serde_json::Value> {
    let mut cursor = Cursor::new(bytes);
    let value = read_json_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

fn read_json_value(c: &mut Cursor<'_>) -> zerompk::Result<serde_json::Value> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        0xC0 => Ok(serde_json::Value::Null),
        0xC2 => Ok(serde_json::Value::Bool(false)),
        0xC3 => Ok(serde_json::Value::Bool(true)),

        0x00..=0x7F => Ok(serde_json::Value::Number((marker as i64).into())),
        0xE0..=0xFF => Ok(serde_json::Value::Number((marker as i8 as i64).into())),

        0xCC => Ok(serde_json::Value::Number(c.take()?.into())),
        0xCD => Ok(serde_json::Value::Number(c.read_u16_be()?.into())),
        0xCE => Ok(serde_json::Value::Number(c.read_u32_be()?.into())),
        0xCF => {
            let b = c.take_n(8)?;
            let v = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(serde_json::Value::Number(v.into()))
        }

        0xD0 => Ok(serde_json::Value::Number((c.take()? as i8 as i64).into())),
        0xD1 => {
            let b = c.take_n(2)?;
            Ok(serde_json::Value::Number(
                (i16::from_be_bytes([b[0], b[1]]) as i64).into(),
            ))
        }
        0xD2 => {
            let b = c.take_n(4)?;
            Ok(serde_json::Value::Number(
                (i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64).into(),
            ))
        }
        0xD3 => {
            let b = c.take_n(8)?;
            Ok(serde_json::Value::Number(
                i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]).into(),
            ))
        }

        0xCA => {
            let b = c.take_n(4)?;
            Ok(serde_json::json!(
                f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64
            ))
        }
        0xCB => {
            let b = c.take_n(8)?;
            Ok(serde_json::json!(f64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]
            ])))
        }

        m @ 0xA0..=0xBF => read_json_str(c, (m & 0x1F) as usize),
        0xD9 => {
            let l = c.take()? as usize;
            read_json_str(c, l)
        }
        0xDA => {
            let l = c.read_u16_be()? as usize;
            read_json_str(c, l)
        }
        0xDB => {
            let l = c.read_u32_be()? as usize;
            read_json_str(c, l)
        }

        0xC4 => {
            let l = c.take()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }
        0xC5 => {
            let l = c.read_u16_be()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }
        0xC6 => {
            let l = c.read_u32_be()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }

        m @ 0x90..=0x9F => read_json_array(c, (m & 0x0F) as usize),
        0xDC => {
            let l = c.read_u16_be()? as usize;
            read_json_array(c, l)
        }
        0xDD => {
            let l = c.read_u32_be()? as usize;
            read_json_array(c, l)
        }

        m @ 0x80..=0x8F => read_json_map(c, (m & 0x0F) as usize),
        0xDE => {
            let l = c.read_u16_be()? as usize;
            read_json_map(c, l)
        }
        0xDF => {
            let l = c.read_u32_be()? as usize;
            read_json_map(c, l)
        }

        // fixext8: instants render as ISO 8601, other types as null
        0xD7 => {
            let (ext_type, payload) = c.take_fixext8()?;
            Ok(match instant_from_ext(ext_type, payload) {
                Some((_, micros)) => {
                    serde_json::Value::String(NdbDateTime::from_micros(micros).to_iso8601())
                }
                None => serde_json::Value::Null,
            })
        }

        // other ext types — skip
        0xD4 => {
            c.take_n(2)?;
            Ok(serde_json::Value::Null)
        }
        0xD5 => {
            c.take_n(3)?;
            Ok(serde_json::Value::Null)
        }
        0xD6 => {
            c.take_n(5)?;
            Ok(serde_json::Value::Null)
        }
        0xD8 => {
            c.take_n(17)?;
            Ok(serde_json::Value::Null)
        }
        0xC7 => {
            let l = c.take()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }
        0xC8 => {
            let l = c.read_u16_be()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }
        0xC9 => {
            let l = c.read_u32_be()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }

        _ => Err(zerompk::Error::InvalidMarker(marker)),
    }
}

fn read_json_str(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    let bytes = c.take_n(len)?;
    let s = String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
    Ok(serde_json::Value::String(s))
}

fn read_json_array(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut arr = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        arr.push(read_json_value(c)?);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Array(arr))
}

fn read_json_map(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut map = serde_json::Map::with_capacity(len.min(4096));
    for _ in 0..len {
        let key_marker = c.peek()?;
        let key = if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            match read_json_value(c)? {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            }
        } else {
            read_json_value(c)?.to_string()
        };
        let val = read_json_value(c)?;
        map.insert(key, val);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Object(map))
}

pub(crate) fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(out, "{}", CHARS[((triple >> 18) & 0x3F) as usize] as char);
        let _ = write!(out, "{}", CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = write!(out, "{}", CHARS[((triple >> 6) & 0x3F) as usize] as char);
        }
        if chunk.len() > 2 {
            let _ = write!(out, "{}", CHARS[(triple & 0x3F) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    //! Roundtrip tests for the JSON reader against the JSON writer.

    use super::*;
    use crate::json_msgpack::instant_ext::{InstantKind, write_instant};
    use crate::json_msgpack::writer::json_to_msgpack;
    use serde_json::json;

    #[test]
    fn roundtrip_null() {
        let val = json!(null);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_bool() {
        for val in [json!(true), json!(false)] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_integers() {
        for val in [
            json!(0),
            json!(42),
            json!(-1),
            json!(i64::MAX),
            json!(i64::MIN),
        ] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_float() {
        let val = json!(9.81);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_string() {
        let val = json!("hello world");
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_array() {
        let val = json!([1, "two", true, null, 2.72]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_nested_object() {
        let val = json!({"a": 1, "b": {"c": [2, 3]}, "d": null});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_map() {
        let val = json!({});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_array() {
        let val = json!([]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_large_string() {
        let s = "x".repeat(300);
        let val = json!(s);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn instant_ext_renders_iso8601() {
        let mut bytes = vec![0x91];
        write_instant(&mut bytes, InstantKind::Utc, 1_710_498_600_000_000);
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(restored, json!(["2024-03-15T10:30:00.000000Z"]));
    }

    #[test]
    fn unknown_ext_is_null() {
        let bytes = [0xD7, 0x09, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(json_from_msgpack(&bytes).unwrap(), json!(null));
    }

    #[test]
    fn truncated_instant_ext_is_error() {
        let mut bytes = Vec::new();
        write_instant(&mut bytes, InstantKind::Naive, 7);
        bytes.truncate(6);
        assert!(json_from_msgpack(&bytes).is_err());
    }
}
