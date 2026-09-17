// SPDX-License-Identifier: Apache-2.0

//! Typed scalar reads and container headers.

use std::str;

use super::tags::*;

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

/// Read a string slice at `*off`, advancing `*off` past it. Zero-copy.
/// Returns `None` for non-string types, invalid UTF-8, or truncated input.
pub fn read_str_advance<'a>(buf: &'a [u8], off: &mut usize) -> Option<&'a str> {
    let (start, len) = str_bounds(buf, *off)?;
    let bytes = buf.get(start..start + len)?;
    let s = str::from_utf8(bytes).ok()?;
    *off = start + len;
    Some(s)
}

/// Read a `bin` value at `*off`, advancing `*off` past it. Zero-copy —
/// the returned slice borrows from `buf`. Returns `None` for non-bin tags
/// or truncated input.
pub fn read_bin_advance<'a>(buf: &'a [u8], off: &mut usize) -> Option<&'a [u8]> {
    let tag = get(buf, *off)?;
    let (len, header) = match tag {
        BIN8 => (get(buf, *off + 1)? as usize, 2),
        BIN16 => (read_u16_be(buf, *off + 1)? as usize, 3),
        BIN32 => (read_u32_be(buf, *off + 1)? as usize, 5),
        _ => return None,
    };
    let start = *off + header;
    let end = start + len;
    let data = buf.get(start..end)?;
    *off = end;
    Some(data)
}

/// Read an unsigned integer that fits in a `u32` at `*off`, advancing `*off`
/// past it. Accepts positive fixint, uint8, uint16, uint32. Returns `None`
/// for negative, signed-typed, oversized (uint64), or non-integer values.
pub fn read_u32_advance(buf: &[u8], off: &mut usize) -> Option<u32> {
    let tag = get(buf, *off)?;
    match tag {
        0x00..=0x7f => {
            *off += 1;
            Some(tag as u32)
        }
        UINT8 => {
            let v = get(buf, *off + 1)? as u32;
            *off += 2;
            Some(v)
        }
        UINT16 => {
            let v = read_u16_be(buf, *off + 1)? as u32;
            *off += 3;
            Some(v)
        }
        UINT32 => {
            let v = read_u32_be(buf, *off + 1)?;
            *off += 5;
            Some(v)
        }
        _ => None,
    }
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
    use crate::msgpack_scan::reader::skip_value;

    use serde_json::json;

    /// Helper: encode a serde_json::Value to MessagePack bytes.
    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
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
    fn read_f64_float32() {
        // json! always produces f64, so test float32 with raw bytes
        // float32 tag (0xca) + 1.5 in IEEE 754 big-endian
        let buf = [0xca, 0x3f, 0xc0, 0x00, 0x00];
        let val = read_f64(&buf, 0).unwrap();
        assert!((val - 1.5).abs() < 1e-6);
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
    fn array_header_fixarray() {
        let buf = encode(&json!([10, 20, 30]));
        let (count, data_offset) = array_header(&buf, 0).unwrap();
        assert_eq!(count, 3);
        assert_eq!(read_i64(&buf, data_offset), Some(10));
    }

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

    #[test]
    fn read_bin_advance_all_widths() {
        // bin8: 0xc4, len=3
        let mut off = 0;
        let buf = [BIN8, 3, 0xde, 0xad, 0xbe, 0xff];
        assert_eq!(
            read_bin_advance(&buf, &mut off),
            Some(&[0xde, 0xad, 0xbe][..])
        );
        assert_eq!(off, 5);

        // bin16: 0xc5, big-endian len=4
        let mut off = 0;
        let buf = [BIN16, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(
            read_bin_advance(&buf, &mut off),
            Some(&[0x01, 0x02, 0x03, 0x04][..])
        );
        assert_eq!(off, 7);

        // bin32: 0xc6, big-endian len=2
        let mut off = 0;
        let buf = [BIN32, 0x00, 0x00, 0x00, 0x02, 0xaa, 0xbb];
        assert_eq!(read_bin_advance(&buf, &mut off), Some(&[0xaa, 0xbb][..]));
        assert_eq!(off, 7);

        // Non-bin tag returns None and does not advance.
        let mut off = 0;
        let buf = [0xc0u8]; // nil
        assert_eq!(read_bin_advance(&buf, &mut off), None);
        assert_eq!(off, 0);

        // Truncated returns None.
        let mut off = 0;
        let buf = [BIN8, 5, 0x01]; // claims 5 bytes, only 1 present
        assert_eq!(read_bin_advance(&buf, &mut off), None);
    }

    #[test]
    fn read_u32_advance_all_widths() {
        // positive fixint
        let mut off = 0;
        assert_eq!(read_u32_advance(&[42u8], &mut off), Some(42));
        assert_eq!(off, 1);

        // uint8
        let mut off = 0;
        assert_eq!(read_u32_advance(&[UINT8, 200], &mut off), Some(200));
        assert_eq!(off, 2);

        // uint16
        let mut off = 0;
        let buf = [UINT16, 0x12, 0x34];
        assert_eq!(read_u32_advance(&buf, &mut off), Some(0x1234));
        assert_eq!(off, 3);

        // uint32
        let mut off = 0;
        let buf = [UINT32, 0xde, 0xad, 0xbe, 0xef];
        assert_eq!(read_u32_advance(&buf, &mut off), Some(0xdeadbeef));
        assert_eq!(off, 5);

        // negative fixint, int*, uint64, float, etc. all rejected
        let mut off = 0;
        assert_eq!(read_u32_advance(&[0xffu8], &mut off), None); // negative fixint
        assert_eq!(off, 0);
        let mut off = 0;
        assert_eq!(read_u32_advance(&[INT8, 5], &mut off), None);
        let mut off = 0;
        assert_eq!(
            read_u32_advance(&[UINT64, 0, 0, 0, 0, 0, 0, 0, 1], &mut off),
            None
        );

        // Truncated returns None.
        let mut off = 0;
        assert_eq!(read_u32_advance(&[UINT16, 0x12], &mut off), None);
    }

    #[test]
    fn read_str_advance_basic() {
        // fixstr "hi"
        let mut off = 0;
        let buf = encode(&json!("hi"));
        assert_eq!(read_str_advance(&buf, &mut off), Some("hi"));
        assert_eq!(off, buf.len());

        // Sequential reads
        let buf = encode(&json!(["one", "two"]));
        let (count, mut off) = array_header(&buf, 0).unwrap();
        assert_eq!(count, 2);
        assert_eq!(read_str_advance(&buf, &mut off), Some("one"));
        assert_eq!(read_str_advance(&buf, &mut off), Some("two"));
        assert_eq!(off, buf.len());

        // Non-string returns None.
        let mut off = 0;
        assert_eq!(read_str_advance(&[NIL], &mut off), None);
        assert_eq!(off, 0);
    }
}
