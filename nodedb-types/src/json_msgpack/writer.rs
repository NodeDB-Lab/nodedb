// SPDX-License-Identifier: Apache-2.0

//! Msgpack serialization for `serde_json::Value` and `nodedb_types::Value`.
//!
//! `Value::DateTime` and `Value::NaiveDateTime` are written as the instant
//! ext (`fixext8`, see `instant_ext`). `Duration`, `Decimal`, and `Geometry`
//! are written as strings. `Vector` is a float64 array, `ArrayCell` a map,
//! and `Range` / `Record` are `nil`.

use super::instant_ext::{InstantKind, write_instant};
use super::json_value::JsonValue;

/// Serialize a `serde_json::Value` to MessagePack bytes.
#[inline]
pub fn json_to_msgpack(value: &serde_json::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&JsonValue(value.clone()))
}

/// Serialize a `serde_json::Value` to MessagePack bytes, returning an empty
/// msgpack map (`0x80`) on failure.
///
/// Suitable for filter evaluation where an empty map causes all field
/// predicates to pass vacuously. Callers that need error propagation
/// should use [`json_to_msgpack`] instead.
#[inline]
pub fn json_to_msgpack_or_empty(value: &serde_json::Value) -> Vec<u8> {
    json_to_msgpack(value).unwrap_or_else(|_| vec![0x80])
}

/// Serialize a `nodedb_types::Value` to standard MessagePack bytes.
///
/// Writes standard msgpack format (fixmap 0x80-0x8F, fixstr 0xA0-0xBF, etc.)
/// directly from `Value` — no zerompk tagged encoding.
pub fn value_to_msgpack(value: &crate::Value) -> zerompk::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(128);
    write_native_value(&mut buf, value);
    Ok(buf)
}

/// Write a `nodedb_types::Value` as standard msgpack bytes.
fn write_native_value(buf: &mut Vec<u8>, value: &crate::Value) {
    match value {
        crate::Value::Null => buf.push(0xC0),
        crate::Value::Bool(false) => buf.push(0xC2),
        crate::Value::Bool(true) => buf.push(0xC3),
        crate::Value::Integer(i) => write_native_int(buf, *i),
        crate::Value::Float(f) => {
            buf.push(0xCB);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        crate::Value::String(s)
        | crate::Value::Uuid(s)
        | crate::Value::Ulid(s)
        | crate::Value::Regex(s) => write_native_str(buf, s),
        crate::Value::Bytes(b) => write_native_bin(buf, b),
        crate::Value::Array(arr) | crate::Value::Set(arr) => {
            write_native_array_header(buf, arr.len());
            for v in arr {
                write_native_value(buf, v);
            }
        }
        crate::Value::Object(map) => {
            write_native_map_header(buf, map.len());
            for (k, v) in map {
                write_native_str(buf, k);
                write_native_value(buf, v);
            }
        }
        crate::Value::DateTime(dt) => write_instant(buf, InstantKind::Utc, dt.micros),
        crate::Value::NaiveDateTime(dt) => write_instant(buf, InstantKind::Naive, dt.micros),
        crate::Value::Duration(d) => write_native_str(buf, &d.to_string()),
        crate::Value::Decimal(d) => write_native_str(buf, &d.to_string()),
        crate::Value::Geometry(g) => {
            if let Ok(s) = sonic_rs::to_string(g) {
                write_native_str(buf, &s);
            } else {
                buf.push(0xC0);
            }
        }
        crate::Value::Range { .. } | crate::Value::Record { .. } => buf.push(0xC0),
        crate::Value::Vector(v) => {
            // Encode as a standard msgpack array of float64 values so that
            // pgwire clients receive a plain JSON number array.
            write_native_array_header(buf, v.len());
            for f in v.iter() {
                buf.push(0xCB);
                buf.extend_from_slice(&(*f as f64).to_be_bytes());
            }
        }
        // ArrayCell is encoded as a `{coords:[...], attrs:[...]}` map so the
        // pgwire `msgpack_to_json_string` transcoder produces clean JSON for
        // clients reading slice/project rows. On audit-log (AS OF SYSTEM TIME
        // NULL) reads each version carries its system-time, surfaced as the
        // `_ts_system` column (mirrors the document-engine audit-log shape).
        crate::Value::ArrayCell(cell) => {
            let map_len = if cell.system_time.is_some() { 3 } else { 2 };
            write_native_map_header(buf, map_len);
            write_native_str(buf, "coords");
            write_native_array_header(buf, cell.coords.len());
            for v in &cell.coords {
                write_native_value(buf, v);
            }
            write_native_str(buf, "attrs");
            write_native_array_header(buf, cell.attrs.len());
            for v in &cell.attrs {
                write_native_value(buf, v);
            }
            if let Some(ts) = cell.system_time {
                write_native_str(buf, "_ts_system");
                write_native_int(buf, ts);
            }
        }
    }
}

fn write_native_int(buf: &mut Vec<u8>, i: i64) {
    if (0..=0x7F).contains(&i) {
        buf.push(i as u8);
    } else if (-32..0).contains(&i) {
        buf.push(i as u8); // negative fixint
    } else if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
        buf.push(0xD0);
        buf.push(i as i8 as u8);
    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
        buf.push(0xD1);
        buf.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
        buf.push(0xD2);
        buf.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        buf.push(0xD3);
        buf.extend_from_slice(&i.to_be_bytes());
    }
}

fn write_native_str(buf: &mut Vec<u8>, s: &str) {
    let len = s.len();
    if len < 32 {
        buf.push(0xA0 | len as u8);
    } else if len <= u8::MAX as usize {
        buf.push(0xD9);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDA);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDB);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(s.as_bytes());
}

fn write_native_bin(buf: &mut Vec<u8>, b: &[u8]) {
    let len = b.len();
    if len <= u8::MAX as usize {
        buf.push(0xC4);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xC5);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xC6);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(b);
}

fn write_native_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn write_native_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}
