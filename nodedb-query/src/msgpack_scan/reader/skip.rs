// SPDX-License-Identifier: Apache-2.0

//! `skip_value`: advance past one MessagePack value without decoding it.

use super::tags::*;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::read_str;

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
    fn skip_empty_containers() {
        // empty fixmap
        assert_eq!(skip_value(&[0x80], 0), Some(1));
        // empty fixarray
        assert_eq!(skip_value(&[0x90], 0), Some(1));
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
        let mut buf = vec![0x91u8; depth]; // fixarray(1) — opens 1-element array
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
}
