//! Zero-copy MessagePack serialization for `serde_json::Value`.
//!
//! Provides standalone functions and a newtype wrapper `JsonValue`
//! with `ToMessagePack`/`FromMessagePack` impls, enabling zerompk
//! serialization of JSON values via zerompk.

use zerompk::{ToMessagePack, Write};

/// Newtype wrapper around `serde_json::Value` implementing zerompk traits.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonValue(pub serde_json::Value);

impl From<serde_json::Value> for JsonValue {
    #[inline]
    fn from(v: serde_json::Value) -> Self {
        Self(v)
    }
}

impl From<JsonValue> for serde_json::Value {
    #[inline]
    fn from(v: JsonValue) -> Self {
        v.0
    }
}

impl std::ops::Deref for JsonValue {
    type Target = serde_json::Value;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for JsonValue {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ─── Serialization (via zerompk Write trait) ───────────────────────────────

impl ToMessagePack for JsonValue {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        write_json_value(&self.0, writer)
    }
}

fn write_json_value<W: Write>(val: &serde_json::Value, writer: &mut W) -> zerompk::Result<()> {
    match val {
        serde_json::Value::Null => writer.write_nil(),
        serde_json::Value::Bool(b) => writer.write_boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                writer.write_i64(i)
            } else if let Some(u) = n.as_u64() {
                writer.write_u64(u)
            } else if let Some(f) = n.as_f64() {
                writer.write_f64(f)
            } else {
                writer.write_f64(0.0)
            }
        }
        serde_json::Value::String(s) => writer.write_string(s),
        serde_json::Value::Array(arr) => {
            writer.write_array_len(arr.len())?;
            for item in arr {
                write_json_value(item, writer)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            writer.write_map_len(map.len())?;
            for (key, val) in map {
                writer.write_string(key)?;
                write_json_value(val, writer)?;
            }
            Ok(())
        }
    }
}

// ─── Deserialization (deterministic raw byte parser) ───────────────────────
//
// MessagePack's first byte deterministically identifies the type. We parse
// raw bytes directly instead of guessing through the Read trait, making this
// work reliably with any reader implementation.

impl<'a> zerompk::FromMessagePack<'a> for JsonValue {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        // We delegate to the standalone raw parser via a roundtrip through bytes.
        // This is only used when JsonValue appears as a field in a zerompk-derived
        // struct. For top-level deserialization, use json_from_msgpack() directly.
        //
        // The overhead is one allocation for the intermediate bytes, but this
        // ensures correctness with any Read implementation.

        // Unfortunately we can't extract raw bytes from an arbitrary Read<'a>.
        // We must use the Read trait methods. The zerompk SliceReader implementation
        // DOES peek without advancing on marker mismatch (verified in source),
        // so the try-each approach is safe for SliceReader. For IOReader it would
        // not be safe, but NodeDB never uses IOReader for deserialization.
        //
        // To be robust: we use SliceReader's behavior but add a clear contract.
        read_json_from_reader(reader)
    }
}

/// Read a JSON value from a zerompk reader.
///
/// SAFETY CONTRACT: This function relies on the reader returning Err WITHOUT
/// advancing the cursor when a marker byte doesn't match. This is guaranteed
/// by zerompk::SliceReader (used by from_msgpack) but NOT by IOReader.
fn read_json_from_reader<'a, R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<JsonValue> {
    // Try nil (0xC0)
    if reader.read_nil().is_ok() {
        return Ok(JsonValue(serde_json::Value::Null));
    }
    // Try bool (0xC2, 0xC3)
    if let Ok(b) = reader.read_boolean() {
        return Ok(JsonValue(serde_json::Value::Bool(b)));
    }
    // Try i64 (covers fixint 0x00-0x7F, neg fixint 0xE0-0xFF, int8-int64)
    if let Ok(i) = reader.read_i64() {
        return Ok(JsonValue(serde_json::Value::Number(i.into())));
    }
    // Try u64 (covers uint64 values > i64::MAX)
    if let Ok(u) = reader.read_u64() {
        return Ok(JsonValue(serde_json::Value::Number(u.into())));
    }
    // Try f64 (0xCB)
    if let Ok(f) = reader.read_f64() {
        return Ok(JsonValue(serde_json::json!(f)));
    }
    // Try f32 (0xCA)
    if let Ok(f) = reader.read_f32() {
        return Ok(JsonValue(serde_json::json!(f as f64)));
    }
    // Try string (fixstr 0xA0-0xBF, str8 0xD9, str16 0xDA, str32 0xDB)
    if let Ok(s) = reader.read_string() {
        return Ok(JsonValue(serde_json::Value::String(s.into_owned())));
    }
    // Try array (fixarray 0x90-0x9F, array16 0xDC, array32 0xDD)
    if let Ok(len) = reader.read_array_len() {
        reader.increment_depth()?;
        let mut arr = Vec::with_capacity(len.min(4096));
        for _ in 0..len {
            let JsonValue(v) = read_json_from_reader(reader)?;
            arr.push(v);
        }
        reader.decrement_depth();
        return Ok(JsonValue(serde_json::Value::Array(arr)));
    }
    // Try map (fixmap 0x80-0x8F, map16 0xDE, map32 0xDF)
    if let Ok(len) = reader.read_map_len() {
        reader.increment_depth()?;
        let mut map = serde_json::Map::with_capacity(len.min(4096));
        for _ in 0..len {
            let key = reader.read_string()?;
            let JsonValue(val) = read_json_from_reader(reader)?;
            map.insert(key.into_owned(), val);
        }
        reader.decrement_depth();
        return Ok(JsonValue(serde_json::Value::Object(map)));
    }

    Err(zerompk::Error::InvalidMarker(0))
}

// ─── Standalone raw byte parser (no Read trait dependency) ─────────────────
//
// This parser reads msgpack bytes directly using a cursor, determining the
// type from the first byte per the msgpack spec. Works with any byte source.

/// Serialize a `serde_json::Value` to MessagePack bytes.
#[inline]
pub fn json_to_msgpack(value: &serde_json::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&JsonValue(value.clone()))
}

/// Deserialize a `serde_json::Value` from MessagePack bytes.
///
/// Uses a deterministic raw byte parser — the first byte of each msgpack
/// value unambiguously identifies its type per the msgpack specification.
/// No guessing or backtracking needed.
pub fn json_from_msgpack(bytes: &[u8]) -> zerompk::Result<serde_json::Value> {
    let mut cursor = Cursor::new(bytes);
    read_value(&mut cursor)
}

/// Serialize a `nodedb_types::Value` to MessagePack bytes using zerompk.
#[inline]
pub fn value_to_msgpack(value: &crate::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value)
}

/// Deserialize a `nodedb_types::Value` from MessagePack bytes using zerompk.
#[inline]
pub fn value_from_msgpack(bytes: &[u8]) -> zerompk::Result<crate::Value> {
    zerompk::from_msgpack(bytes)
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            depth: 0,
        }
    }

    #[inline]
    fn peek(&self) -> zerompk::Result<u8> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or(zerompk::Error::BufferTooSmall)
    }

    #[inline]
    fn take(&mut self) -> zerompk::Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    fn take_n(&mut self, n: usize) -> zerompk::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return Err(zerompk::Error::BufferTooSmall);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u16_be(&mut self) -> zerompk::Result<u16> {
        let b = self.take_n(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_u32_be(&mut self) -> zerompk::Result<u32> {
        let b = self.take_n(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
}

/// Read a msgpack-encoded JSON value from raw bytes. First byte determines type.
fn read_value(c: &mut Cursor<'_>) -> zerompk::Result<serde_json::Value> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        // nil
        0xC0 => Ok(serde_json::Value::Null),

        // bool
        0xC2 => Ok(serde_json::Value::Bool(false)),
        0xC3 => Ok(serde_json::Value::Bool(true)),

        // positive fixint (0x00 - 0x7F)
        0x00..=0x7F => Ok(serde_json::Value::Number(serde_json::Number::from(
            marker as i64,
        ))),

        // negative fixint (0xE0 - 0xFF)
        0xE0..=0xFF => Ok(serde_json::Value::Number(serde_json::Number::from(
            marker as i8 as i64,
        ))),

        // uint 8
        0xCC => {
            let v = c.take()?;
            Ok(serde_json::Value::Number(v.into()))
        }
        // uint 16
        0xCD => {
            let v = c.read_u16_be()?;
            Ok(serde_json::Value::Number(v.into()))
        }
        // uint 32
        0xCE => {
            let v = c.read_u32_be()?;
            Ok(serde_json::Value::Number(v.into()))
        }
        // uint 64
        0xCF => {
            let b = c.take_n(8)?;
            let v = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(serde_json::Value::Number(v.into()))
        }

        // int 8
        0xD0 => {
            let v = c.take()? as i8;
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        // int 16
        0xD1 => {
            let b = c.take_n(2)?;
            let v = i16::from_be_bytes([b[0], b[1]]);
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        // int 32
        0xD2 => {
            let b = c.take_n(4)?;
            let v = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        // int 64
        0xD3 => {
            let b = c.take_n(8)?;
            let v = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(serde_json::Value::Number(v.into()))
        }

        // float 32
        0xCA => {
            let b = c.take_n(4)?;
            let v = f32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            Ok(serde_json::json!(v as f64))
        }
        // float 64
        0xCB => {
            let b = c.take_n(8)?;
            let v = f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(serde_json::json!(v))
        }

        // fixstr (0xA0 - 0xBF)
        m @ 0xA0..=0xBF => {
            let len = (m & 0x1F) as usize;
            let bytes = c.take_n(len)?;
            let s =
                String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
            Ok(serde_json::Value::String(s))
        }
        // str 8
        0xD9 => {
            let len = c.take()? as usize;
            let bytes = c.take_n(len)?;
            let s =
                String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
            Ok(serde_json::Value::String(s))
        }
        // str 16
        0xDA => {
            let len = c.read_u16_be()? as usize;
            let bytes = c.take_n(len)?;
            let s =
                String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
            Ok(serde_json::Value::String(s))
        }
        // str 32
        0xDB => {
            let len = c.read_u32_be()? as usize;
            let bytes = c.take_n(len)?;
            let s =
                String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
            Ok(serde_json::Value::String(s))
        }

        // bin 8/16/32 → encode as base64 string (JSON has no binary type)
        0xC4 => {
            let len = c.take()? as usize;
            let bytes = c.take_n(len)?;
            Ok(serde_json::Value::String(base64_encode(bytes)))
        }
        0xC5 => {
            let len = c.read_u16_be()? as usize;
            let bytes = c.take_n(len)?;
            Ok(serde_json::Value::String(base64_encode(bytes)))
        }
        0xC6 => {
            let len = c.read_u32_be()? as usize;
            let bytes = c.take_n(len)?;
            Ok(serde_json::Value::String(base64_encode(bytes)))
        }

        // fixarray (0x90 - 0x9F)
        m @ 0x90..=0x9F => {
            let len = (m & 0x0F) as usize;
            read_array(c, len)
        }
        // array 16
        0xDC => {
            let len = c.read_u16_be()? as usize;
            read_array(c, len)
        }
        // array 32
        0xDD => {
            let len = c.read_u32_be()? as usize;
            read_array(c, len)
        }

        // fixmap (0x80 - 0x8F)
        m @ 0x80..=0x8F => {
            let len = (m & 0x0F) as usize;
            read_map(c, len)
        }
        // map 16
        0xDE => {
            let len = c.read_u16_be()? as usize;
            read_map(c, len)
        }
        // map 32
        0xDF => {
            let len = c.read_u32_be()? as usize;
            read_map(c, len)
        }

        // ext types, timestamps — skip by reading and discarding
        0xD4 => {
            c.take_n(2)?; // fixext 1: type + 1 byte
            Ok(serde_json::Value::Null)
        }
        0xD5 => {
            c.take_n(3)?; // fixext 2: type + 2 bytes
            Ok(serde_json::Value::Null)
        }
        0xD6 => {
            c.take_n(5)?; // fixext 4: type + 4 bytes
            Ok(serde_json::Value::Null)
        }
        0xD7 => {
            c.take_n(9)?; // fixext 8: type + 8 bytes
            Ok(serde_json::Value::Null)
        }
        0xD8 => {
            c.take_n(17)?; // fixext 16: type + 16 bytes
            Ok(serde_json::Value::Null)
        }
        0xC7 => {
            let len = c.take()? as usize;
            c.take_n(1 + len)?; // ext 8: type + N bytes
            Ok(serde_json::Value::Null)
        }
        0xC8 => {
            let len = c.read_u16_be()? as usize;
            c.take_n(1 + len)?; // ext 16
            Ok(serde_json::Value::Null)
        }
        0xC9 => {
            let len = c.read_u32_be()? as usize;
            c.take_n(1 + len)?; // ext 32
            Ok(serde_json::Value::Null)
        }

        _ => Err(zerompk::Error::InvalidMarker(marker)),
    }
}

fn read_array(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut arr = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        arr.push(read_value(c)?);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Array(arr))
}

fn read_map(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut map = serde_json::Map::with_capacity(len.min(4096));
    for _ in 0..len {
        // Map keys: read the next value as a string key.
        // If non-string key, convert to string representation.
        let key_marker = c.peek()?;
        let key = if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            match read_value(c)? {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            }
        } else {
            // Non-string key — read as value and stringify
            read_value(c)?.to_string()
        };
        let val = read_value(c)?;
        map.insert(key, val);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Object(map))
}

/// Simple base64 encoding for binary data (no padding).
fn base64_encode(data: &[u8]) -> String {
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
    use super::*;
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
        let val = json!([1, "two", null, true, 9.81]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_object() {
        let val = json!({"name": "Alice", "age": 30, "active": true, "scores": [95, 87]});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_nested() {
        let val = json!({
            "users": [
                {"id": 1, "name": "Alice", "meta": null},
                {"id": 2, "name": "Bob", "meta": {"role": "admin"}}
            ],
            "count": 2
        });
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_object() {
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
    fn roundtrip_u64_large() {
        let val = json!(u64::MAX);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}
