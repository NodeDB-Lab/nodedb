//! Byte-level comparison and hashing for MessagePack field values.
//!
//! Operates on raw byte ranges returned by `extract_field`. Used for
//! join key matching, GROUP BY key deduplication, ORDER BY, and DISTINCT.

use std::cmp::Ordering;
use std::hash::{BuildHasher, Hasher};

use crate::msgpack_scan::reader::{read_f64, read_i64, read_null, str_bounds};

/// Hash the raw bytes of a MessagePack value at `range` within `buf`.
/// Uses a fast non-cryptographic hash suitable for hash joins and GROUP BY.
///
/// For canonical-encoded documents (integers in smallest form, sorted keys),
/// semantically equal values produce identical byte sequences and thus
/// identical hashes.
pub fn hash_field_bytes(buf: &[u8], range: (usize, usize)) -> u64 {
    let slice = match buf.get(range.0..range.1) {
        Some(s) => s,
        None => return 0,
    };
    let hasher_builder = std::collections::hash_map::RandomState::new();
    let mut hasher = hasher_builder.build_hasher();
    hasher.write(slice);
    hasher.finish()
}

/// Hash the raw bytes using a provided `RandomState` for consistent hashing
/// within a single query (all docs hashed with the same seed).
pub fn hash_field_bytes_with(
    buf: &[u8],
    range: (usize, usize),
    state: &std::collections::hash_map::RandomState,
) -> u64 {
    let slice = match buf.get(range.0..range.1) {
        Some(s) => s,
        None => return 0,
    };
    let mut hasher = state.build_hasher();
    hasher.write(slice);
    hasher.finish()
}

/// Compare two MessagePack values by their decoded content.
///
/// Comparison order:
/// 1. Null < Bool < Number < String < Binary < Array < Map
/// 2. Within numbers: compare as f64
/// 3. Within strings: lexicographic on raw bytes (valid UTF-8 guarantees
///    byte order = Unicode code-point order for ASCII/Latin-1)
/// 4. Fallback: raw byte comparison
pub fn compare_field_bytes(
    a_buf: &[u8],
    a_range: (usize, usize),
    b_buf: &[u8],
    b_range: (usize, usize),
) -> Ordering {
    let a_off = a_range.0;
    let b_off = b_range.0;

    let a_tag = match a_buf.get(a_off) {
        Some(&t) => t,
        None => return Ordering::Less,
    };
    let b_tag = match b_buf.get(b_off) {
        Some(&t) => t,
        None => return Ordering::Greater,
    };

    let a_type = type_rank(a_tag);
    let b_type = type_rank(b_tag);

    if a_type != b_type {
        return a_type.cmp(&b_type);
    }

    match a_type {
        0 => Ordering::Equal, // both null
        1 => {
            // bool
            let a_val = a_tag == 0xc3; // true
            let b_val = b_tag == 0xc3;
            a_val.cmp(&b_val)
        }
        2 => {
            // number — compare as f64
            match (read_f64(a_buf, a_off), read_f64(b_buf, b_off)) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        }
        3 => {
            // string — compare raw bytes
            match (str_bounds(a_buf, a_off), str_bounds(b_buf, b_off)) {
                (Some((a_s, a_l)), Some((b_s, b_l))) => {
                    let a_bytes = &a_buf[a_s..a_s + a_l];
                    let b_bytes = &b_buf[b_s..b_s + b_l];
                    a_bytes.cmp(b_bytes)
                }
                _ => Ordering::Equal,
            }
        }
        _ => {
            // binary, array, map, ext — fallback to raw byte comparison
            let a_slice = &a_buf[a_range.0..a_range.1];
            let b_slice = &b_buf[b_range.0..b_range.1];
            a_slice.cmp(b_slice)
        }
    }
}

/// Compare two numeric MessagePack values as i64.
/// Useful when the caller knows both values are integers.
pub fn compare_field_i64(a_buf: &[u8], a_off: usize, b_buf: &[u8], b_off: usize) -> Ordering {
    match (read_i64(a_buf, a_off), read_i64(b_buf, b_off)) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

/// Check if two MessagePack values are byte-identical.
/// For canonical-encoded documents, byte equality implies semantic equality.
pub fn field_bytes_eq(
    a_buf: &[u8],
    a_range: (usize, usize),
    b_buf: &[u8],
    b_range: (usize, usize),
) -> bool {
    let a_slice = match a_buf.get(a_range.0..a_range.1) {
        Some(s) => s,
        None => return false,
    };
    let b_slice = match b_buf.get(b_range.0..b_range.1) {
        Some(s) => s,
        None => return false,
    };
    a_slice == b_slice
}

/// Check if a field value is null without extracting it.
pub fn is_field_null(buf: &[u8], range: (usize, usize)) -> bool {
    read_null(buf, range.0)
}

/// Type rank for cross-type ordering. Lower rank = sorts first.
/// Null(0) < Bool(1) < Number(2) < String(3) < Binary(4) < Array(5) < Map(6) < Ext(7)
fn type_rank(tag: u8) -> u8 {
    match tag {
        0xc0 => 0,                      // nil
        0xc2 | 0xc3 => 1,               // bool
        0x00..=0x7f | 0xe0..=0xff => 2, // fixint
        0xca..=0xd3 => 2,               // float/uint/int
        0xa0..=0xbf | 0xd9..=0xdb => 3, // string
        0xc4..=0xc6 => 4,               // binary
        0x90..=0x9f | 0xdc | 0xdd => 5, // array
        0x80..=0x8f | 0xde | 0xdf => 6, // map
        0xc7..=0xc9 | 0xd4..=0xd8 => 7, // ext
        _ => 8,                         // unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn val_range(buf: &[u8]) -> (usize, usize) {
        (0, buf.len())
    }

    #[test]
    fn hash_same_bytes_same_hash() {
        let buf = encode(&json!(42));
        let state = std::collections::hash_map::RandomState::new();
        let h1 = hash_field_bytes_with(&buf, val_range(&buf), &state);
        let h2 = hash_field_bytes_with(&buf, val_range(&buf), &state);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_different_values_likely_differ() {
        let buf1 = encode(&json!(42));
        let buf2 = encode(&json!(43));
        let state = std::collections::hash_map::RandomState::new();
        let h1 = hash_field_bytes_with(&buf1, val_range(&buf1), &state);
        let h2 = hash_field_bytes_with(&buf2, val_range(&buf2), &state);
        assert_ne!(h1, h2);
    }

    #[test]
    fn compare_integers() {
        let a = encode(&json!(10));
        let b = encode(&json!(20));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
        assert_eq!(
            compare_field_bytes(&b, val_range(&b), &a, val_range(&a)),
            Ordering::Greater
        );
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &a, val_range(&a)),
            Ordering::Equal
        );
    }

    #[test]
    fn compare_strings() {
        let a = encode(&json!("apple"));
        let b = encode(&json!("banana"));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_cross_type_null_vs_number() {
        let a = encode(&json!(null));
        let b = encode(&json!(42));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_cross_type_string_vs_number() {
        let a = encode(&json!(42));
        let b = encode(&json!("hello"));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_booleans() {
        let a = encode(&json!(false));
        let b = encode(&json!(true));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_negative_integers() {
        let a = encode(&json!(-10));
        let b = encode(&json!(-5));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn field_bytes_eq_works() {
        let a = encode(&json!("test"));
        let b = encode(&json!("test"));
        let c = encode(&json!("other"));
        assert!(field_bytes_eq(&a, val_range(&a), &b, val_range(&b)));
        assert!(!field_bytes_eq(&a, val_range(&a), &c, val_range(&c)));
    }

    #[test]
    fn is_field_null_works() {
        let null_buf = encode(&json!(null));
        let int_buf = encode(&json!(42));
        assert!(is_field_null(&null_buf, val_range(&null_buf)));
        assert!(!is_field_null(&int_buf, val_range(&int_buf)));
    }

    #[test]
    fn compare_floats() {
        let a = encode(&json!(1.5));
        let b = encode(&json!(2.5));
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
    }

    #[test]
    fn hash_from_extracted_field() {
        let buf = encode(&json!({"id": 42}));
        let range = crate::msgpack_scan::field::extract_field(&buf, 0, "id").unwrap();
        let h = hash_field_bytes(&buf, range);
        assert_ne!(h, 0);
    }
}
