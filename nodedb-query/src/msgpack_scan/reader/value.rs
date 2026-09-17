// SPDX-License-Identifier: Apache-2.0

//! `read_value`: decode one scalar into `nodedb_types::Value`.

use nodedb_types::read_instant;

use super::scalar::read_str;
use super::tags::*;

/// Read a scalar msgpack value at `offset` into `nodedb_types::Value`.
///
/// Handles null, bool, integers, floats, strings, and instant ext
/// (`fixext8` type 1 / 2 → `Value::DateTime` / `Value::NaiveDateTime`).
/// For complex types (array, map, bin, other ext), returns `None` — caller
/// should use `json_from_msgpack` for those.
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
        // Instants
        FIXEXT8 => read_instant(buf, offset).map(|(kind, micros)| kind.from_micros(micros)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::{
        array_header, map_header, read_bool, read_f64, read_i64, read_null, skip_value,
    };
    use nodedb_types::{InstantKind, NdbDateTime, Value, write_instant};

    use serde_json::json;

    /// Helper: encode a serde_json::Value to MessagePack bytes.
    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn read_value_instants() {
        let mut buf = vec![0xc0];
        write_instant(&mut buf, InstantKind::Utc, 1_710_498_600_000_000);
        write_instant(&mut buf, InstantKind::Naive, -5);
        assert_eq!(
            read_value(&buf, 1),
            Some(Value::DateTime(NdbDateTime::from_micros(
                1_710_498_600_000_000
            )))
        );
        assert_eq!(
            read_value(&buf, 11),
            Some(Value::NaiveDateTime(NdbDateTime::from_micros(-5)))
        );
        assert_eq!(skip_value(&buf, 1), Some(11));
    }

    #[test]
    fn read_value_unknown_ext_is_none() {
        let buf = [FIXEXT8, 0x09, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(read_value(&buf, 0), None);
        let buf = [FIXEXT1, 0x01, 0xab];
        assert_eq!(read_value(&buf, 0), None);
    }

    #[test]
    fn read_value_truncated_instant_is_none() {
        let mut buf = Vec::new();
        write_instant(&mut buf, InstantKind::Utc, 42);
        buf.truncate(9);
        assert_eq!(read_value(&buf, 0), None);
    }

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
