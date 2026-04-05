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

/// Read a scalar msgpack value at `offset` into `nodedb_types::Value`.
///
/// Handles null, bool, integers, floats, and strings. For complex types
/// (array, map, bin, ext), returns `None` — caller should use
/// `json_from_msgpack` for those.
pub fn read_value(buf: &[u8], offset: usize) -> Option<nodedb_types::Value> {
    let tag = get(buf, offset)?;
    match tag {
        NIL => Some(nodedb_types::Value::Null),
        TRUE => Some(nodedb_types::Value::Bool(true)),
        FALSE => Some(nodedb_types::Value::Bool(false)),
        // Integers
        0x00..=0x7f => Some(nodedb_types::Value::Integer(tag as i64)),
        0xe0..=0xff => Some(nodedb_types::Value::Integer((tag as i8) as i64)),
        UINT8 => Some(nodedb_types::Value::Integer(get(buf, offset + 1)? as i64)),
        UINT16 => Some(nodedb_types::Value::Integer(
            read_u16_be(buf, offset + 1)? as i64
        )),
        UINT32 => Some(nodedb_types::Value::Integer(
            read_u32_be(buf, offset + 1)? as i64
        )),
        UINT64 => Some(nodedb_types::Value::Integer(
            read_u64_be(buf, offset + 1)? as i64
        )),
        INT8 => Some(nodedb_types::Value::Integer(
            get(buf, offset + 1)? as i8 as i64
        )),
        INT16 => Some(nodedb_types::Value::Integer(
            read_u16_be(buf, offset + 1)? as i16 as i64,
        )),
        INT32 => Some(nodedb_types::Value::Integer(
            read_u32_be(buf, offset + 1)? as i32 as i64,
        )),
        INT64 => Some(nodedb_types::Value::Integer(
            read_u64_be(buf, offset + 1)? as i64
        )),
        // Floats
        FLOAT32 => {
            let bits = read_u32_be(buf, offset + 1)?;
            Some(nodedb_types::Value::Float(f32::from_bits(bits) as f64))
        }
        FLOAT64 => {
            let bits = read_u64_be(buf, offset + 1)?;
            Some(nodedb_types::Value::Float(f64::from_bits(bits)))
        }
        // Strings
        0xa0..=0xbf | STR8 | STR16 | STR32 => {
            read_str(buf, offset).map(|s| nodedb_types::Value::String(s.to_string()))
        }
        _ => None,
    }
}

/// Return the number of key-value pairs and the offset of the first pair,
/// for the map starting at `offset`. Returns `None` if not a map.
pub fn map_header(buf: &[u8], offset: usize) -> Option<(usize, usize)> {
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

    // ── Canonical encoding guarantee tests ─────────────────────────────

    #[test]
    fn canonical_integer_smallest_representation() {
        // fixint (0-127): single byte
        let buf = encode(&json!(42));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 42);

        // 0 as fixint
        let buf = encode(&json!(0));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);

        // 127 as fixint
        let buf = encode(&json!(127));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 127);

        // 128 should NOT be fixint. JSON parses as i64, so zerompk uses
        // int16 (0xd1) since 128 > i8::MAX. This is canonical for signed path.
        let buf = encode(&json!(128));
        assert_eq!(buf[0], 0xd1); // int16 tag
        assert_eq!(buf.len(), 3); // tag + 2 bytes

        // negative fixint (-32 to -1)
        let buf = encode(&json!(-1));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0xff); // -1 as negative fixint

        let buf = encode(&json!(-32));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0xe0); // -32 as negative fixint
    }

    #[test]
    fn canonical_map_keys_sorted() {
        // Keys should be lexicographically sorted in msgpack output.
        // Encode with keys in non-sorted order in JSON source.
        let buf = encode(&json!({"z": 1, "a": 2, "m": 3}));

        // Parse map and verify keys come out sorted
        let (count, mut pos) = map_header(&buf, 0).unwrap();
        assert_eq!(count, 3);

        let mut keys = Vec::new();
        for _ in 0..count {
            let key = read_str(&buf, pos).unwrap();
            keys.push(key.to_string());
            pos = skip_value(&buf, pos).unwrap(); // skip key
            pos = skip_value(&buf, pos).unwrap(); // skip value
        }
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn canonical_deterministic_bytes() {
        // Same logical document encoded twice must produce identical bytes.
        let doc1 = encode(&json!({"name": "alice", "age": 30, "active": true}));
        let doc2 = encode(&json!({"age": 30, "active": true, "name": "alice"}));
        assert_eq!(
            doc1, doc2,
            "same logical doc must produce identical msgpack bytes"
        );
    }

    #[test]
    fn canonical_nested_map_keys_sorted() {
        let buf = encode(&json!({"outer": {"z": 1, "a": 2}}));
        // Extract the inner map
        let (start, _end) = crate::msgpack_scan::field::extract_field(&buf, 0, "outer").unwrap();

        let (count, mut pos) = map_header(&buf, start).unwrap();
        assert_eq!(count, 2);

        let key1 = read_str(&buf, pos).unwrap();
        pos = skip_value(&buf, pos).unwrap();
        pos = skip_value(&buf, pos).unwrap();
        let key2 = read_str(&buf, pos).unwrap();

        assert_eq!(key1, "a");
        assert_eq!(key2, "z");
    }

    // ── Fuzz-style tests ───────────────────────────────────────────────────

    /// Feed every single-byte sequence through all reader functions. None may
    /// panic — they must return `None` or a valid result.
    #[test]
    fn fuzz_all_single_byte_sequences() {
        for byte in 0u8..=255 {
            let buf = [byte];
            // None of these must panic
            let _ = skip_value(&buf, 0);
            let _ = read_f64(&buf, 0);
            let _ = read_i64(&buf, 0);
            let _ = read_str(&buf, 0);
            let _ = read_bool(&buf, 0);
            let _ = read_null(&buf, 0);
            let _ = map_header(&buf, 0);
            let _ = array_header(&buf, 0);
            let _ = read_value(&buf, 0);
        }
    }

    /// Feed two-byte patterns to cover tag + partial payload (truncated).
    #[test]
    fn fuzz_two_byte_patterns() {
        // Tags that expect more bytes than we provide
        let tags_need_extra: &[u8] = &[
            0xca, // FLOAT32 needs 4 more
            0xcb, // FLOAT64 needs 8 more
            0xcc, // UINT8 needs 1 more
            0xcd, // UINT16 needs 2 more
            0xce, // UINT32 needs 4 more
            0xcf, // UINT64 needs 8 more
            0xd0, // INT8 needs 1 more
            0xd1, // INT16 needs 2 more
            0xd2, // INT32 needs 4 more
            0xd3, // INT64 needs 8 more
            0xd9, // STR8 length byte then data
            0xda, // STR16 2-byte length then data
            0xdb, // STR32 4-byte length then data
            0xdc, // ARRAY16 2-byte count then elements
            0xdd, // ARRAY32 4-byte count then elements
            0xde, // MAP16 2-byte count then pairs
            0xdf, // MAP32 4-byte count then pairs
            0xc4, // BIN8
            0xc5, // BIN16
            0xc6, // BIN32
            0xd4, // FIXEXT1
            0xd5, // FIXEXT2
            0xd6, // FIXEXT4
            0xd7, // FIXEXT8
            0xd8, // FIXEXT16
        ];
        for &tag in tags_need_extra {
            // Single byte (completely truncated payload)
            let buf = [tag];
            let _ = skip_value(&buf, 0);
            let _ = read_f64(&buf, 0);
            let _ = read_i64(&buf, 0);
            let _ = read_value(&buf, 0);

            // Tag + one garbage byte
            for second in [0x00u8, 0x01, 0x7f, 0x80, 0xff] {
                let buf = [tag, second];
                let _ = skip_value(&buf, 0);
                let _ = read_f64(&buf, 0);
                let _ = read_i64(&buf, 0);
                let _ = read_value(&buf, 0);
            }
        }
    }

    /// Deterministic pseudo-random byte sequences must not cause panics.
    #[test]
    fn fuzz_deterministic_random_payloads() {
        // Generate deterministic sequences without external crates using a
        // simple LCG (Knuth multiplicative hash).
        let mut state: u64 = 0xdeadbeef_cafebabe;
        let next = |s: &mut u64| -> u8 {
            *s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (*s >> 33) as u8
        };

        let mut buf = vec![0u8; 256];
        for _ in 0..2000 {
            // Randomize buffer length (1..=256) and contents
            let len = (next(&mut state) as usize % 256) + 1;
            for b in buf[..len].iter_mut() {
                *b = next(&mut state);
            }
            let slice = &buf[..len];

            // Try reading from multiple offsets
            for offset in [0, 1, len / 2, len.saturating_sub(1)] {
                let _ = skip_value(slice, offset);
                let _ = read_f64(slice, offset);
                let _ = read_i64(slice, offset);
                let _ = read_str(slice, offset);
                let _ = read_bool(slice, offset);
                let _ = read_null(slice, offset);
                let _ = map_header(slice, offset);
                let _ = array_header(slice, offset);
                let _ = read_value(slice, offset);
            }
        }
    }

    /// Truncate a valid msgpack buffer at every byte position.
    /// All reader functions must return `None` — never panic.
    #[test]
    fn fuzz_truncated_valid_payloads() {
        let docs = [
            json!({"key": "value", "num": 42, "flag": true}),
            json!({"nested": {"a": 1, "b": [1, 2, 3]}}),
            json!([1, "two", 3.0, null, false]),
            json!({"large": 9999999999_i64}),
            json!({"float": 1.23456789}),
        ];

        for doc in &docs {
            let full = encode(doc);
            // Truncate at every position from 0 to full.len()-1
            for truncate_at in 0..full.len() {
                let slice = &full[..truncate_at];
                // None of these may panic; result doesn't matter
                let _ = skip_value(slice, 0);
                let _ = read_f64(slice, 0);
                let _ = read_i64(slice, 0);
                let _ = read_str(slice, 0);
                let _ = read_bool(slice, 0);
                let _ = map_header(slice, 0);
                let _ = array_header(slice, 0);
                let _ = read_value(slice, 0);
            }
        }
    }

    /// The never-used 0xc1 tag must return `None` for all functions.
    #[test]
    fn fuzz_never_used_tag_c1() {
        // 0xc1 is explicitly "never used" in the msgpack spec
        let buf = [0xc1u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            skip_value(&buf, 0),
            None,
            "0xc1 must return None from skip_value"
        );
        assert_eq!(read_f64(&buf, 0), None);
        assert_eq!(read_i64(&buf, 0), None);
        assert_eq!(read_str(&buf, 0), None);
        assert_eq!(read_bool(&buf, 0), None);
        assert_eq!(map_header(&buf, 0), None);
        assert_eq!(array_header(&buf, 0), None);
        assert_eq!(read_value(&buf, 0), None);
    }

    /// All tag boundary bytes — test transitions at fixint/fixmap/fixarray/fixstr edges.
    #[test]
    fn fuzz_tag_boundaries() {
        // Each entry: (tag, expected_skip_result)
        // For tags that are self-contained single bytes, skip returns Some(1).
        // For tags requiring more data we just verify no panic with empty tail.
        let boundary_tags: &[(u8, bool)] = &[
            (0x00, true),  // positive fixint 0
            (0x7f, true),  // positive fixint 127
            (0x80, true),  // fixmap length 0 (empty map)
            (0x8f, false), // fixmap length 15 — needs 15 pairs
            (0x90, true),  // fixarray length 0 (empty array)
            (0x9f, false), // fixarray length 15 — needs 15 elements
            (0xa0, true),  // fixstr length 0 (empty string)
            (0xbf, false), // fixstr length 31 — needs 31 bytes after
            (0xc0, true),  // nil
            (0xc1, false), // never used — must return None
            (0xc2, true),  // false
            (0xc3, true),  // true
            (0xe0, true),  // negative fixint -32
            (0xff, true),  // negative fixint -1
        ];
        for &(tag, self_contained) in boundary_tags {
            let buf = [tag; 64]; // fill with the same tag as padding
            let result = skip_value(&buf, 0);
            if self_contained {
                assert!(result.is_some(), "tag 0x{tag:02x} should skip OK");
            } else if tag == 0xc1 {
                assert_eq!(result, None, "0xc1 must always return None");
            }
            // For non-self-contained tags with valid padding we just verify no panic.
        }
    }

    /// Buffers where length fields claim enormous sizes but the buffer is tiny.
    #[test]
    fn fuzz_adversarial_length_fields() {
        // STR32: tag 0xdb + 4-byte big-endian length claiming 0xffffffff bytes
        let buf = [0xdbu8, 0xff, 0xff, 0xff, 0xff, b'x', b'y'];
        assert_eq!(skip_value(&buf, 0), None);
        assert_eq!(read_str(&buf, 0), None);

        // STR16: tag 0xda + 2-byte length claiming 0xffff bytes
        let buf = [0xdau8, 0xff, 0xff, b'x'];
        assert_eq!(skip_value(&buf, 0), None);

        // ARRAY32: claims 0xffffffff elements but buffer is empty after header
        let buf = [0xddu8, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(skip_value(&buf, 0), None);

        // MAP32: claims 0xffffffff pairs but buffer is empty after header
        let buf = [0xdfu8, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(skip_value(&buf, 0), None);

        // ARRAY16: claims 0xffff elements
        let buf = [0xdcu8, 0xff, 0xff];
        assert_eq!(skip_value(&buf, 0), None);

        // MAP16: claims 0xffff pairs
        let buf = [0xdeu8, 0xff, 0xff];
        assert_eq!(skip_value(&buf, 0), None);

        // BIN32: claims max length
        let buf = [0xc6u8, 0xff, 0xff, 0xff, 0xff, 0x00];
        assert_eq!(skip_value(&buf, 0), None);

        // EXT32: claims max length
        let buf = [0xc9u8, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00];
        assert_eq!(skip_value(&buf, 0), None);
    }

    /// Deeply nested maps/arrays must cause `skip_value` to return `None`
    /// once nesting exceeds MAX_DEPTH (128).
    #[test]
    fn fuzz_malicious_nesting_depth() {
        // Build a buffer with 200 levels of fixarray (each containing 1 element)
        // fixarray tag for 1 element = 0x91
        let depth = 200usize;
        let mut buf = Vec::with_capacity(depth + 1);
        for _ in 0..depth {
            buf.push(0x91u8); // fixarray(1) — opens 1-element array
        }
        buf.push(0xc0u8); // nil at the innermost leaf

        // skip_value must return None because nesting > MAX_DEPTH
        assert_eq!(
            skip_value(&buf, 0),
            None,
            "deeply nested arrays must return None to guard against stack overflow"
        );

        // Same with maps: fixmap(1) = 0x81, then a fixstr(1) key + value
        // Build 200 levels of fixmap(1) — each pair is (fixstr key, next map)
        let mut map_buf: Vec<u8> = Vec::new();
        for i in 0..(depth as u8) {
            map_buf.push(0x81); // fixmap(1)
            map_buf.push(0xa1); // fixstr(1) key
            map_buf.push(b'a'.wrapping_add(i % 26));
            // value = next map (already pushed in next iteration), or nil at end
        }
        map_buf.push(0xc0); // nil leaf

        assert_eq!(
            skip_value(&map_buf, 0),
            None,
            "deeply nested maps must return None"
        );
    }

    /// Verify skip_value correctly consumes exactly the right number of bytes
    /// for all fixed-width numeric types and returns the correct next offset.
    #[test]
    fn fuzz_fixed_width_numeric_skip_offsets() {
        // (tag, expected_total_bytes_consumed)
        let cases: &[(u8, usize)] = &[
            (0xca, 5), // FLOAT32: 1 tag + 4 data
            (0xcb, 9), // FLOAT64: 1 tag + 8 data
            (0xcc, 2), // UINT8
            (0xcd, 3), // UINT16
            (0xce, 5), // UINT32
            (0xcf, 9), // UINT64
            (0xd0, 2), // INT8
            (0xd1, 3), // INT16
            (0xd2, 5), // INT32
            (0xd3, 9), // INT64
        ];
        for &(tag, size) in cases {
            let mut buf = vec![0u8; size + 4]; // extra padding
            buf[0] = tag;
            let result = skip_value(&buf, 0);
            assert_eq!(
                result,
                Some(size),
                "tag 0x{tag:02x} should advance by {size} bytes"
            );
        }
    }

    /// Verify all fixext types consume the correct byte count.
    #[test]
    fn fuzz_fixext_skip_offsets() {
        // (tag, expected_bytes_consumed)
        let cases: &[(u8, usize)] = &[
            (0xd4, 3),  // FIXEXT1: 1+1+1
            (0xd5, 4),  // FIXEXT2: 1+1+2
            (0xd6, 6),  // FIXEXT4: 1+1+4
            (0xd7, 10), // FIXEXT8: 1+1+8
            (0xd8, 18), // FIXEXT16: 1+1+16
        ];
        for &(tag, size) in cases {
            let mut buf = vec![0u8; size + 4];
            buf[0] = tag;
            let result = skip_value(&buf, 0);
            assert_eq!(
                result,
                Some(size),
                "fixext tag 0x{tag:02x} should advance by {size} bytes"
            );
        }
    }

    /// Out-of-bounds offset must return `None` — not panic.
    #[test]
    fn fuzz_out_of_bounds_offset() {
        let buf = encode(&json!({"x": 1}));
        let way_out = buf.len() + 1000;
        assert_eq!(skip_value(&buf, way_out), None);
        assert_eq!(read_f64(&buf, way_out), None);
        assert_eq!(read_i64(&buf, way_out), None);
        assert_eq!(read_str(&buf, way_out), None);
        assert_eq!(read_bool(&buf, way_out), None);
        assert_eq!(map_header(&buf, way_out), None);
        assert_eq!(array_header(&buf, way_out), None);
        assert_eq!(read_value(&buf, way_out), None);
    }

    /// Empty buffer must return `None` for all functions that can.
    #[test]
    fn fuzz_empty_buffer() {
        let buf: &[u8] = &[];
        assert_eq!(skip_value(buf, 0), None);
        assert_eq!(read_f64(buf, 0), None);
        assert_eq!(read_i64(buf, 0), None);
        assert_eq!(read_str(buf, 0), None);
        assert_eq!(read_bool(buf, 0), None);
        assert!(!read_null(buf, 0)); // returns bool, not Option
        assert_eq!(map_header(buf, 0), None);
        assert_eq!(array_header(buf, 0), None);
        assert_eq!(read_value(buf, 0), None);
    }
}
