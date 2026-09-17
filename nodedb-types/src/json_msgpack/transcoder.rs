// SPDX-License-Identifier: Apache-2.0

//! Streaming msgpack → JSON string transcoder.
//!
//! Walks msgpack bytes and writes JSON text directly into a String.
//! No intermediate `serde_json::Value` or `nodedb_types::Value`.
//! Used ONLY at the outermost pgwire/HTTP layer for client compatibility.
//!
//! Instant ext values (fixext8 type 1 / 2) render as ISO 8601 strings.
//! Every other ext type renders as `null`.

use std::fmt::Write as _;

use super::error::MsgpackResult;
use super::instant_ext::instant_from_ext;
use super::reader::{Cursor, base64_encode};
use crate::datetime::NdbDateTime;

/// Transcode raw msgpack bytes to a JSON string without intermediate types.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected rather than silently dropped.
pub fn msgpack_to_json_string(bytes: &[u8]) -> MsgpackResult<String> {
    if bytes.is_empty() {
        return Ok(String::new());
    }
    let mut c = Cursor::new(bytes);
    let mut out = String::with_capacity(bytes.len() * 2);
    transcode_value(&mut c, &mut out)?;
    c.finish()?;
    Ok(out)
}

fn transcode_value(c: &mut Cursor<'_>, out: &mut String) -> zerompk::Result<()> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        0xC0 => out.push_str("null"),
        0xC2 => out.push_str("false"),
        0xC3 => out.push_str("true"),

        0x00..=0x7F => {
            write_int(out, marker as i64);
        }
        0xE0..=0xFF => {
            write_int(out, marker as i8 as i64);
        }

        0xCC => {
            write_uint(out, c.take()? as u64);
        }
        0xCD => {
            write_uint(out, c.read_u16_be()? as u64);
        }
        0xCE => {
            write_uint(out, c.read_u32_be()? as u64);
        }
        0xCF => {
            let b = c.take_n(8)?;
            write_uint(
                out,
                u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
            );
        }

        0xD0 => {
            write_int(out, c.take()? as i8 as i64);
        }
        0xD1 => {
            let b = c.take_n(2)?;
            write_int(out, i16::from_be_bytes([b[0], b[1]]) as i64);
        }
        0xD2 => {
            let b = c.take_n(4)?;
            write_int(out, i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64);
        }
        0xD3 => {
            let b = c.take_n(8)?;
            write_int(
                out,
                i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
            );
        }

        0xCA => {
            let b = c.take_n(4)?;
            write_float(out, f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64);
        }
        0xCB => {
            let b = c.take_n(8)?;
            write_float(
                out,
                f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
            );
        }

        m @ 0xA0..=0xBF => transcode_str(c, out, (m & 0x1F) as usize)?,
        0xD9 => {
            let l = c.take()? as usize;
            transcode_str(c, out, l)?;
        }
        0xDA => {
            let l = c.read_u16_be()? as usize;
            transcode_str(c, out, l)?;
        }
        0xDB => {
            let l = c.read_u32_be()? as usize;
            transcode_str(c, out, l)?;
        }

        0xC4 => {
            let l = c.take()? as usize;
            transcode_bin(c, out, l)?;
        }
        0xC5 => {
            let l = c.read_u16_be()? as usize;
            transcode_bin(c, out, l)?;
        }
        0xC6 => {
            let l = c.read_u32_be()? as usize;
            transcode_bin(c, out, l)?;
        }

        m @ 0x90..=0x9F => transcode_array(c, out, (m & 0x0F) as usize)?,
        0xDC => {
            let l = c.read_u16_be()? as usize;
            transcode_array(c, out, l)?;
        }
        0xDD => {
            let l = c.read_u32_be()? as usize;
            transcode_array(c, out, l)?;
        }

        m @ 0x80..=0x8F => transcode_map(c, out, (m & 0x0F) as usize)?,
        0xDE => {
            let l = c.read_u16_be()? as usize;
            transcode_map(c, out, l)?;
        }
        0xDF => {
            let l = c.read_u32_be()? as usize;
            transcode_map(c, out, l)?;
        }

        // fixext8: instants render as ISO 8601, other types as null
        0xD7 => {
            let (ext_type, payload) = c.take_fixext8()?;
            match instant_from_ext(ext_type, payload) {
                Some((_, micros)) => {
                    out.push('"');
                    out.push_str(&NdbDateTime::from_micros(micros).to_iso8601());
                    out.push('"');
                }
                None => out.push_str("null"),
            }
        }

        // other ext types — render as null
        0xD4 => {
            c.take_n(2)?;
            out.push_str("null");
        }
        0xD5 => {
            c.take_n(3)?;
            out.push_str("null");
        }
        0xD6 => {
            c.take_n(5)?;
            out.push_str("null");
        }
        0xD8 => {
            c.take_n(17)?;
            out.push_str("null");
        }
        0xC7 => {
            let l = c.take()? as usize;
            c.take_n(1 + l)?;
            out.push_str("null");
        }
        0xC8 => {
            let l = c.read_u16_be()? as usize;
            c.take_n(1 + l)?;
            out.push_str("null");
        }
        0xC9 => {
            let l = c.read_u32_be()? as usize;
            c.take_n(1 + l)?;
            out.push_str("null");
        }

        _ => return Err(zerompk::Error::InvalidMarker(marker)),
    }
    Ok(())
}

fn write_int(out: &mut String, v: i64) {
    let _ = write!(out, "{v}");
}

fn write_uint(out: &mut String, v: u64) {
    let _ = write!(out, "{v}");
}

fn write_float(out: &mut String, v: f64) {
    if v.is_nan() || v.is_infinite() {
        out.push_str("null");
    } else if v.fract() == 0.0 && v.abs() < (1i64 << 53) as f64 {
        let _ = write!(out, "{v:.1}");
    } else {
        let _ = write!(out, "{v}");
    }
}

fn transcode_str(c: &mut Cursor<'_>, out: &mut String, len: usize) -> zerompk::Result<()> {
    let bytes = c.take_n(len)?;
    let s = std::str::from_utf8(bytes).map_err(|_| zerompk::Error::InvalidMarker(0))?;
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    Ok(())
}

fn transcode_bin(c: &mut Cursor<'_>, out: &mut String, len: usize) -> zerompk::Result<()> {
    let bytes = c.take_n(len)?;
    out.push('"');
    out.push_str(&base64_encode(bytes));
    out.push('"');
    Ok(())
}

fn transcode_array(c: &mut Cursor<'_>, out: &mut String, len: usize) -> zerompk::Result<()> {
    c.depth += 1;
    out.push('[');
    for i in 0..len {
        if i > 0 {
            out.push(',');
        }
        transcode_value(c, out)?;
    }
    out.push(']');
    c.depth -= 1;
    Ok(())
}

fn transcode_map(c: &mut Cursor<'_>, out: &mut String, len: usize) -> zerompk::Result<()> {
    c.depth += 1;
    out.push('{');
    for i in 0..len {
        if i > 0 {
            out.push(',');
        }
        let key_marker = c.peek()?;
        if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            transcode_value(c, out)?;
        } else {
            let mut tmp = String::new();
            transcode_value(c, &mut tmp)?;
            out.push('"');
            out.push_str(&tmp);
            out.push('"');
        }
        out.push(':');
        transcode_value(c, out)?;
    }
    out.push('}');
    c.depth -= 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_msgpack::writer::json_to_msgpack;

    #[test]
    fn basic_map() {
        let val = serde_json::json!({"name": "alice", "age": 30, "active": true});
        let mp = json_to_msgpack(&val).unwrap();
        let json_str = msgpack_to_json_string(&mp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["name"], "alice");
        assert_eq!(parsed["age"], 30);
        assert_eq!(parsed["active"], true);
    }

    #[test]
    fn array() {
        let val = serde_json::json!([1, "two", null, false]);
        let mp = json_to_msgpack(&val).unwrap();
        let json_str = msgpack_to_json_string(&mp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn escaping() {
        let val = serde_json::json!({"msg": "hello \"world\"\nnewline"});
        let mp = json_to_msgpack(&val).unwrap();
        let json_str = msgpack_to_json_string(&mp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["msg"], "hello \"world\"\nnewline");
    }

    #[test]
    fn empty() {
        assert_eq!(msgpack_to_json_string(&[]).unwrap(), "");
    }

    #[test]
    fn instant_ext_renders_iso8601() {
        use crate::json_msgpack::instant_ext::{InstantKind, write_instant};
        let mut mp = vec![0x81, 0xA2, b't', b's'];
        write_instant(&mut mp, InstantKind::Naive, 1_710_498_600_000_000);
        let json_str = msgpack_to_json_string(&mp).unwrap();
        assert_eq!(json_str, "{\"ts\":\"2024-03-15T10:30:00.000000Z\"}");

        let unknown = [0xD7, 0x09, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(msgpack_to_json_string(&unknown).unwrap(), "null");
    }

    #[test]
    fn nested() {
        let val = serde_json::json!({"a": {"b": [1, 2, {"c": 3}]}});
        let mp = json_to_msgpack(&val).unwrap();
        let json_str = msgpack_to_json_string(&mp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed, val);
    }
}
