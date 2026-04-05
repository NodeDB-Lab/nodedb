//! Low-level MessagePack binary reader: tag parsing, value skipping, and typed reads.
//!
//! All functions operate on `&[u8]` with explicit offsets. Zero allocation,
//! zero copy. Returns `None` on truncated/invalid data — never panics.

use std::str;

// ── Tag constants ──────────────────────────────────────────────────────

const NIL: u8 = 0xc0;
const FALSE: u8 = 0xc2;
const TRUE: u8 = 0xc3;
const BIN8: u8 = 0xc4;
const BIN16: u8 = 0xc5;
const BIN32: u8 = 0xc6;
const EXT8: u8 = 0xc7;
const EXT16: u8 = 0xc8;
const EXT32: u8 = 0xc9;
const FLOAT32: u8 = 0xca;
const FLOAT64: u8 = 0xcb;
const UINT8: u8 = 0xcc;
const UINT16: u8 = 0xcd;
const UINT32: u8 = 0xce;
const UINT64: u8 = 0xcf;
const INT8: u8 = 0xd0;
const INT16: u8 = 0xd1;
const INT32: u8 = 0xd2;
const INT64: u8 = 0xd3;
const FIXEXT1: u8 = 0xd4;
const FIXEXT2: u8 = 0xd5;
const FIXEXT4: u8 = 0xd6;
const FIXEXT8: u8 = 0xd7;
const FIXEXT16: u8 = 0xd8;
const STR8: u8 = 0xd9;
const STR16: u8 = 0xda;
const STR32: u8 = 0xdb;
const ARRAY16: u8 = 0xdc;
const ARRAY32: u8 = 0xdd;
const MAP16: u8 = 0xde;
const MAP32: u8 = 0xdf;

/// Maximum nesting depth to prevent stack overflow on malicious payloads.
const MAX_DEPTH: u16 = 128;

// ── Inline helpers ─────────────────────────────────────────────────────

#[inline(always)]
fn get(buf: &[u8], pos: usize) -> Option<u8> {
    buf.get(pos).copied()
}

#[inline(always)]
fn read_u16_be(buf: &[u8], pos: usize) -> Option<u16> {
    let bytes = buf.get(pos..pos + 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

#[inline(always)]
fn read_u32_be(buf: &[u8], pos: usize) -> Option<u32> {
    let bytes = buf.get(pos..pos + 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[inline(always)]
fn read_u64_be(buf: &[u8], pos: usize) -> Option<u64> {
    let bytes = buf.get(pos..pos + 8)?;
    Some(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Return `Some(offset + size)` only if the buffer has enough bytes.
#[inline(always)]
fn checked_advance(buf: &[u8], offset: usize, size: usize) -> Option<usize> {
    let end = offset + size;
    if end <= buf.len() { Some(end) } else { None }
}

// ── skip_value ─────────────────────────────────────────────────────────

/// Advance past the MessagePack value starting at `offset`, returning the
/// offset of the next value. Returns `None` if the buffer is truncated or
/// nesting exceeds `MAX_DEPTH`.
///
/// This is the performance-critical primitive. It never allocates.
pub fn skip_value(buf: &[u8], offset: usize) -> Option<usize> {
    skip_value_depth(buf, offset, 0)
}

fn skip_value_depth(buf: &[u8], offset: usize, depth: u16) -> Option<usize> {
    if depth > MAX_DEPTH {
        return None;
    }
    let tag = get(buf, offset)?;
    match tag {
        // positive fixint (0x00..=0x7f)
        0x00..=0x7f => Some(offset + 1),
        // negative fixint (0xe0..=0xff)
        0xe0..=0xff => Some(offset + 1),
        // nil, false, true
        NIL | FALSE | TRUE => Some(offset + 1),

        // fixmap (0x80..=0x8f)
        0x80..=0x8f => {
            let count = (tag & 0x0f) as usize;
            skip_n_pairs(buf, offset + 1, count, depth)
        }
        MAP16 => {
            let count = read_u16_be(buf, offset + 1)? as usize;
            skip_n_pairs(buf, offset + 3, count, depth)
        }
        MAP32 => {
            let count = read_u32_be(buf, offset + 1)? as usize;
            skip_n_pairs(buf, offset + 5, count, depth)
        }

        // fixarray (0x90..=0x9f)
        0x90..=0x9f => {
            let count = (tag & 0x0f) as usize;
            skip_n_values(buf, offset + 1, count, depth)
        }
        ARRAY16 => {
            let count = read_u16_be(buf, offset + 1)? as usize;
            skip_n_values(buf, offset + 3, count, depth)
        }
        ARRAY32 => {
            let count = read_u32_be(buf, offset + 1)? as usize;
            skip_n_values(buf, offset + 5, count, depth)
        }

        // fixstr (0xa0..=0xbf)
        0xa0..=0xbf => {
            let len = (tag & 0x1f) as usize;
            checked_advance(buf, offset, 1 + len)
        }
        STR8 => {
            let len = get(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 2 + len)
        }
        STR16 => {
            let len = read_u16_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 3 + len)
        }
        STR32 => {
            let len = read_u32_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 5 + len)
        }

        // bin
        BIN8 => {
            let len = get(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 2 + len)
        }
        BIN16 => {
            let len = read_u16_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 3 + len)
        }
        BIN32 => {
            let len = read_u32_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 5 + len)
        }

        // fixed-width numerics (bounds-check against buffer length)
        FLOAT32 => checked_advance(buf, offset, 5),
        FLOAT64 => checked_advance(buf, offset, 9),
        UINT8 | INT8 => checked_advance(buf, offset, 2),
        UINT16 | INT16 => checked_advance(buf, offset, 3),
        UINT32 | INT32 => checked_advance(buf, offset, 5),
        UINT64 | INT64 => checked_advance(buf, offset, 9),

        // ext
        FIXEXT1 => checked_advance(buf, offset, 3),
        FIXEXT2 => checked_advance(buf, offset, 4),
        FIXEXT4 => checked_advance(buf, offset, 6),
        FIXEXT8 => checked_advance(buf, offset, 10),
        FIXEXT16 => checked_advance(buf, offset, 18),
        EXT8 => {
            let len = get(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 3 + len)
        }
        EXT16 => {
            let len = read_u16_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 4 + len)
        }
        EXT32 => {
            let len = read_u32_be(buf, offset + 1)? as usize;
            checked_advance(buf, offset, 6 + len)
        }

        // 0xc1 is never used in the spec
        _ => None,
    }
}

fn skip_n_values(buf: &[u8], mut pos: usize, count: usize, depth: u16) -> Option<usize> {
    for _ in 0..count {
        pos = skip_value_depth(buf, pos, depth + 1)?;
    }
    Some(pos)
}

fn skip_n_pairs(buf: &[u8], mut pos: usize, count: usize, depth: u16) -> Option<usize> {
    for _ in 0..count {
        pos = skip_value_depth(buf, pos, depth + 1)?; // key
        pos = skip_value_depth(buf, pos, depth + 1)?; // value
    }
    Some(pos)
}

// ── Typed reads ────────────────────────────────────────────────────────

/// Read an f64 from the value at `offset`. Handles float32, float64,
/// and all integer types (coerced to f64).
pub fn read_f64(buf: &[u8], offset: usize) -> Option<f64> {
    let tag = get(buf, offset)?;
    match tag {
        // positive fixint
        0x00..=0x7f => Some(tag as f64),
        // negative fixint
        0xe0..=0xff => Some((tag as i8) as f64),
        FLOAT64 => {
            let bits = read_u64_be(buf, offset + 1)?;
            Some(f64::from_bits(bits))
        }
        FLOAT32 => {
            let bits = read_u32_be(buf, offset + 1)?;
            Some(f32::from_bits(bits) as f64)
        }
        UINT8 => Some(get(buf, offset + 1)? as f64),
        UINT16 => Some(read_u16_be(buf, offset + 1)? as f64),
        UINT32 => Some(read_u32_be(buf, offset + 1)? as f64),
        UINT64 => Some(read_u64_be(buf, offset + 1)? as f64),
        INT8 => Some(get(buf, offset + 1)? as i8 as f64),
        INT16 => Some(read_u16_be(buf, offset + 1)? as i16 as f64),
        INT32 => Some(read_u32_be(buf, offset + 1)? as i32 as f64),
        INT64 => Some(read_u64_be(buf, offset + 1)? as i64 as f64),
        _ => None,
    }
}

/// Read an i64 from the value at `offset`. Handles all integer types.
/// Floats return `None` — use `read_f64` for those.
pub fn read_i64(buf: &[u8], offset: usize) -> Option<i64> {
    let tag = get(buf, offset)?;
    match tag {
        0x00..=0x7f => Some(tag as i64),
        0xe0..=0xff => Some((tag as i8) as i64),
        UINT8 => Some(get(buf, offset + 1)? as i64),
        UINT16 => Some(read_u16_be(buf, offset + 1)? as i64),
        UINT32 => Some(read_u32_be(buf, offset + 1)? as i64),
        UINT64 => {
            let v = read_u64_be(buf, offset + 1)?;
            Some(v as i64)
        }
        INT8 => Some(get(buf, offset + 1)? as i8 as i64),
        INT16 => Some(read_u16_be(buf, offset + 1)? as i16 as i64),
        INT32 => Some(read_u32_be(buf, offset + 1)? as i32 as i64),
        INT64 => {
            let v = read_u64_be(buf, offset + 1)?;
            Some(v as i64)
        }
        _ => None,
    }
}

/// Read a string slice from the value at `offset`. Zero-copy — borrows
/// directly from the input buffer. Returns `None` for non-string types
/// or invalid UTF-8.
pub fn read_str(buf: &[u8], offset: usize) -> Option<&str> {
    let (start, len) = str_bounds(buf, offset)?;
    let bytes = buf.get(start..start + len)?;
    str::from_utf8(bytes).ok()
}

/// Return `(data_start, byte_len)` for the string at `offset` without
/// validating UTF-8. Used internally for key comparison.
pub(crate) fn str_bounds(buf: &[u8], offset: usize) -> Option<(usize, usize)> {
    let tag = get(buf, offset)?;
    match tag {
        0xa0..=0xbf => {
            let len = (tag & 0x1f) as usize;
            Some((offset + 1, len))
        }
        STR8 => {
            let len = get(buf, offset + 1)? as usize;
            Some((offset + 2, len))
        }
        STR16 => {
            let len = read_u16_be(buf, offset + 1)? as usize;
            Some((offset + 3, len))
        }
        STR32 => {
            let len = read_u32_be(buf, offset + 1)? as usize;
            Some((offset + 5, len))
        }
        _ => None,
    }
}

/// Read a boolean from the value at `offset`.
pub fn read_bool(buf: &[u8], offset: usize) -> Option<bool> {
    match get(buf, offset)? {
        TRUE => Some(true),
        FALSE => Some(false),
        _ => None,
    }
}

/// Check if the value at `offset` is nil.
pub fn read_null(buf: &[u8], offset: usize) -> bool {
    get(buf, offset) == Some(NIL)
}

/// Return the number of key-value pairs and the offset of the first pair,
/// for the map starting at `offset`. Returns `None` if not a map.
pub(crate) fn map_header(buf: &[u8], offset: usize) -> Option<(usize, usize)> {
    let tag = get(buf, offset)?;
    match tag {
        0x80..=0x8f => Some(((tag & 0x0f) as usize, offset + 1)),
        MAP16 => Some((read_u16_be(buf, offset + 1)? as usize, offset + 3)),
        MAP32 => Some((read_u32_be(buf, offset + 1)? as usize, offset + 5)),
        _ => None,
    }
}

/// Return the number of elements and the offset of the first element,
/// for the array starting at `offset`. Returns `None` if not an array.
pub fn array_header(buf: &[u8], offset: usize) -> Option<(usize, usize)> {
    let tag = get(buf, offset)?;
    match tag {
        0x90..=0x9f => Some(((tag & 0x0f) as usize, offset + 1)),
        ARRAY16 => Some((read_u16_be(buf, offset + 1)? as usize, offset + 3)),
        ARRAY32 => Some((read_u32_be(buf, offset + 1)? as usize, offset + 5)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    /// Helper: encode a serde_json::Value to MessagePack bytes.
    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn skip_positive_fixint() {
        let buf = [0x05, 0xff];
        assert_eq!(skip_value(&buf, 0), Some(1));
    }

    #[test]
    fn skip_negative_fixint() {
        let buf = [0xe0, 0x00];
        assert_eq!(skip_value(&buf, 0), Some(1));
    }

    #[test]
    fn skip_nil_bool() {
        assert_eq!(skip_value(&[NIL], 0), Some(1));
        assert_eq!(skip_value(&[TRUE], 0), Some(1));
        assert_eq!(skip_value(&[FALSE], 0), Some(1));
    }

    #[test]
    fn skip_float64() {
        let buf = encode(&json!(9.81));
        assert_eq!(skip_value(&buf, 0), Some(buf.len()));
    }

    #[test]
    fn skip_string() {
        let buf = encode(&json!("hello"));
        assert_eq!(skip_value(&buf, 0), Some(buf.len()));
    }

    #[test]
    fn skip_map() {
        let buf = encode(&json!({"a": 1, "b": 2}));
        assert_eq!(skip_value(&buf, 0), Some(buf.len()));
    }

    #[test]
    fn skip_nested_array() {
        let buf = encode(&json!([[1, 2], [3, 4, 5]]));
        assert_eq!(skip_value(&buf, 0), Some(buf.len()));
    }

    #[test]
    fn skip_truncated_returns_none() {
        let buf = [FLOAT64, 0x40]; // truncated float64
        assert_eq!(skip_value(&buf, 0), None);
    }

    #[test]
    fn read_f64_fixint() {
        assert_eq!(read_f64(&[42u8], 0), Some(42.0));
    }

    #[test]
    fn read_f64_negative_fixint() {
        assert_eq!(read_f64(&[0xffu8], 0), Some(-1.0));
    }

    #[test]
    fn read_f64_float64() {
        let buf = encode(&json!(std::f64::consts::PI));
        assert_eq!(read_f64(&buf, 0), Some(std::f64::consts::PI));
    }

    #[test]
    fn read_f64_uint16() {
        let buf = encode(&json!(1000));
        assert_eq!(read_f64(&buf, 0), Some(1000.0));
    }

    #[test]
    fn read_i64_values() {
        assert_eq!(read_i64(&[42u8], 0), Some(42));
        assert_eq!(read_i64(&[0xffu8], 0), Some(-1));

        let buf = encode(&json!(300));
        assert_eq!(read_i64(&buf, 0), Some(300));

        let buf = encode(&json!(-500));
        assert_eq!(read_i64(&buf, 0), Some(-500));
    }

    #[test]
    fn read_str_fixstr() {
        let buf = encode(&json!("hi"));
        assert_eq!(read_str(&buf, 0), Some("hi"));
    }

    #[test]
    fn read_str_str8() {
        let long = "a".repeat(40);
        let buf = encode(&json!(long));
        assert_eq!(read_str(&buf, 0), Some(long.as_str()));
    }

    #[test]
    fn read_bool_values() {
        assert_eq!(read_bool(&[TRUE], 0), Some(true));
        assert_eq!(read_bool(&[FALSE], 0), Some(false));
        assert_eq!(read_bool(&[NIL], 0), None);
    }

    #[test]
    fn read_null_check() {
        assert!(read_null(&[NIL], 0));
        assert!(!read_null(&[TRUE], 0));
    }

    #[test]
    fn map_header_fixmap() {
        let buf = encode(&json!({"x": 1}));
        let (count, _data_offset) = map_header(&buf, 0).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn skip_bin() {
        // bin8: 0xc4, len=3, 3 bytes of data
        let buf = [BIN8, 3, 0xde, 0xad, 0xbe, 0xff];
        assert_eq!(skip_value(&buf, 0), Some(5));
    }

    #[test]
    fn skip_ext() {
        // fixext1: 0xd4, type byte, 1 data byte
        let buf = [FIXEXT1, 0x01, 0xab, 0xff];
        assert_eq!(skip_value(&buf, 0), Some(3));
    }

    #[test]
    fn read_f64_float32() {
        // json! always produces f64, so test float32 with raw bytes
        // float32 tag (0xca) + 1.5 in IEEE 754 big-endian
        let buf = [0xca, 0x3f, 0xc0, 0x00, 0x00];
        let val = read_f64(&buf, 0).unwrap();
        assert!((val - 1.5).abs() < 1e-6);
    }

    #[test]
    fn skip_empty_containers() {
        // empty fixmap
        assert_eq!(skip_value(&[0x80], 0), Some(1));
        // empty fixarray
        assert_eq!(skip_value(&[0x90], 0), Some(1));
    }

    #[test]
    fn array_header_fixarray() {
        let buf = encode(&json!([10, 20, 30]));
        let (count, data_offset) = array_header(&buf, 0).unwrap();
        assert_eq!(count, 3);
        assert_eq!(read_i64(&buf, data_offset), Some(10));
    }
}
