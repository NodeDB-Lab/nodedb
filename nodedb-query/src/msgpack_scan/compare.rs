// SPDX-License-Identifier: Apache-2.0

//! Byte-level comparison and hashing for MessagePack field values.
//!
//! Operates on raw byte ranges returned by `extract_field`. Used for
//! join key matching, GROUP BY key deduplication, ORDER BY, and DISTINCT.

use std::cmp::Ordering;
use std::hash::{BuildHasher, Hasher};

use nodedb_types::read_instant;

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
/// 1. Null < Bool < Number < Instant < String < Binary < Array < Map < Ext
/// 2. Within numbers: compare as f64
/// 3. Within instants: by kind (UTC before naive), then signed epoch micros
/// 4. Within strings: lexicographic on raw bytes (valid UTF-8 guarantees
///    byte order = Unicode code-point order for ASCII/Latin-1)
/// 5. Fallback: raw byte comparison
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

    let a_type = type_rank(a_buf, a_off, a_tag);
    let b_type = type_rank(b_buf, b_off, b_tag);

    if a_type != b_type {
        return a_type.cmp(&b_type);
    }

    match a_type {
        RANK_NULL => Ordering::Equal,
        RANK_BOOL => {
            let a_val = a_tag == 0xc3; // true
            let b_val = b_tag == 0xc3;
            a_val.cmp(&b_val)
        }
        RANK_NUMBER => {
            // compare as f64
            match (read_f64(a_buf, a_off), read_f64(b_buf, b_off)) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        }
        RANK_INSTANT => {
            // Both sides decoded as instants by `type_rank`. Kind first, then
            // signed micros: the raw payload is not byte-comparable below 0.
            match (read_instant(a_buf, a_off), read_instant(b_buf, b_off)) {
                (Some((a_kind, a_us)), Some((b_kind, b_us))) => a_kind
                    .ext_type()
                    .cmp(&b_kind.ext_type())
                    .then(a_us.cmp(&b_us)),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        }
        RANK_STRING => {
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

const RANK_NULL: u8 = 0;
const RANK_BOOL: u8 = 1;
const RANK_NUMBER: u8 = 2;
const RANK_INSTANT: u8 = 3;
const RANK_STRING: u8 = 4;
const RANK_BINARY: u8 = 5;
const RANK_ARRAY: u8 = 6;
const RANK_MAP: u8 = 7;
const RANK_EXT: u8 = 8;
const RANK_UNKNOWN: u8 = 9;

/// Type rank for cross-type ordering. Lower rank = sorts first.
/// Null < Bool < Number < Instant < String < Binary < Array < Map < Ext
///
/// An instant is a complete `fixext8` of type 1 / 2 at `off`. A `fixext8`
/// of any other type, or a truncated one, ranks as ext.
fn type_rank(buf: &[u8], off: usize, tag: u8) -> u8 {
    match tag {
        0xc0 => RANK_NULL,
        0xc2 | 0xc3 => RANK_BOOL,
        0x00..=0x7f | 0xe0..=0xff => RANK_NUMBER, // fixint
        0xca..=0xd3 => RANK_NUMBER,               // float/uint/int
        0xa0..=0xbf | 0xd9..=0xdb => RANK_STRING,
        0xc4..=0xc6 => RANK_BINARY,
        0x90..=0x9f | 0xdc | 0xdd => RANK_ARRAY,
        0x80..=0x8f | 0xde | 0xdf => RANK_MAP,
        0xd7 => {
            if read_instant(buf, off).is_some() {
                RANK_INSTANT
            } else {
                RANK_EXT
            }
        }
        0xc7..=0xc9 | 0xd4..=0xd6 | 0xd8 => RANK_EXT,
        _ => RANK_UNKNOWN,
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
    fn compare_instants_negative_micros() {
        use nodedb_types::{InstantKind, write_instant};
        let mut a = Vec::new();
        write_instant(&mut a, InstantKind::Utc, -10);
        let mut b = Vec::new();
        write_instant(&mut b, InstantKind::Utc, -5);
        let mut c = Vec::new();
        write_instant(&mut c, InstantKind::Utc, 1);
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &b, val_range(&b)),
            Ordering::Less
        );
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &c, val_range(&c)),
            Ordering::Less
        );
        assert_eq!(
            compare_field_bytes(&c, val_range(&c), &b, val_range(&b)),
            Ordering::Greater
        );
        assert_eq!(
            compare_field_bytes(&a, val_range(&a), &a, val_range(&a)),
            Ordering::Equal
        );
    }

    #[test]
    fn compare_instant_kinds_order_utc_first() {
        use nodedb_types::{InstantKind, write_instant};
        let mut utc = Vec::new();
        write_instant(&mut utc, InstantKind::Utc, 100);
        let mut naive = Vec::new();
        write_instant(&mut naive, InstantKind::Naive, 1);
        assert_eq!(
            compare_field_bytes(&utc, val_range(&utc), &naive, val_range(&naive)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_instant_vs_integer_uses_rank() {
        use nodedb_types::{InstantKind, write_instant};
        // A negative instant has a payload starting 0xff, above any fixint
        // byte. Rank places it after every number and before every string.
        let mut inst = Vec::new();
        write_instant(&mut inst, InstantKind::Naive, -1);
        let int = encode(&json!(i64::MAX));
        let s = encode(&json!(""));
        assert_eq!(
            compare_field_bytes(&int, val_range(&int), &inst, val_range(&inst)),
            Ordering::Less
        );
        assert_eq!(
            compare_field_bytes(&inst, val_range(&inst), &s, val_range(&s)),
            Ordering::Less
        );
    }

    #[test]
    fn unknown_fixext8_ranks_as_ext() {
        use nodedb_types::{InstantKind, write_instant};
        let mut inst = Vec::new();
        write_instant(&mut inst, InstantKind::Utc, 0);
        let other = [0xd7, 0x09, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            compare_field_bytes(&inst, val_range(&inst), &other, val_range(&other)),
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
