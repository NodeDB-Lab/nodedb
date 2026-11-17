// SPDX-License-Identifier: BUSL-1.1

//! The key a KV secondary index files a field value under.
//!
//! A value is filed under its [`Value::eq_coerced_bucket`]. Two values a
//! WHERE `=` reports equal share a key, so an equality probe on the
//! literal's key finds every row the coerced equality accepts:
//!
//! - number, or a string that reads as one: the number's bucket `f64`;
//! - datetime, or a string that reads as an instant: its epoch microseconds;
//! - any other string: its text;
//! - bool: its value;
//! - bytes: the bytes;
//! - ND cell: one shared key.
//!
//! One key can hold values that are not equal, such as the strings `'1'`
//! and `'1.0'`, so a probe's candidates are confirmed by the full predicate.
//! NULL, and a value that equals nothing (array, object), get keys
//! no probe asks for: a composite index still files the row under them.
//!
//! Every key starts with a type tag. Within the number and instant tags,
//! byte order is value order.

use nodedb_types::Value;
use nodedb_types::value::coerce::EqCoercedBucket;

use crate::engine::kv::sorted_index::number;

const TAG_NULL: u8 = 0x01;
const TAG_BOOL: u8 = 0x02;
const TAG_NUMBER: u8 = 0x03;
const TAG_INSTANT: u8 = 0x04;
const TAG_TEXT: u8 = 0x05;
const TAG_CELL: u8 = 0x06;
const TAG_UNCOMPARABLE: u8 = 0x07;
const TAG_BYTES: u8 = 0x08;

/// The key of a NULL or absent field.
pub const NULL_KEY: &[u8] = &[TAG_NULL];

/// The key `value` is filed under.
pub fn kv_index_key(value: &Value) -> Vec<u8> {
    match value.eq_coerced_bucket() {
        Some(bucket) => bucket_key(&bucket),
        None if matches!(value, Value::Null) => NULL_KEY.to_vec(),
        None => vec![TAG_UNCOMPARABLE],
    }
}

/// The key an equality probe on `literal` reads: `None` when the literal
/// equals no value, NULL included.
pub fn kv_probe_key(literal: &Value) -> Option<Vec<u8>> {
    literal
        .eq_coerced_bucket()
        .map(|bucket| bucket_key(&bucket))
}

fn bucket_key(bucket: &EqCoercedBucket) -> Vec<u8> {
    match bucket {
        EqCoercedBucket::Bool(b) => vec![TAG_BOOL, u8::from(*b)],
        EqCoercedBucket::Number(bits) => {
            let mut key = Vec::with_capacity(1 + number::NUMBER_LEN);
            key.push(TAG_NUMBER);
            key.extend_from_slice(&number::encode_f64(f64::from_bits(*bits)));
            key
        }
        EqCoercedBucket::Instant(micros) => {
            // Flipping the sign bit orders the two's-complement value as
            // unsigned bytes.
            let ordered = (*micros as u64) ^ (1u64 << 63);
            let mut key = Vec::with_capacity(9);
            key.push(TAG_INSTANT);
            key.extend_from_slice(&ordered.to_be_bytes());
            key
        }
        EqCoercedBucket::Text(text) => {
            let mut key = Vec::with_capacity(1 + text.len());
            key.push(TAG_TEXT);
            key.extend_from_slice(text.as_bytes());
            key
        }
        EqCoercedBucket::Cell => vec![TAG_CELL],
        EqCoercedBucket::Bytes(bytes) => {
            let mut key = Vec::with_capacity(1 + bytes.len());
            key.push(TAG_BYTES);
            key.extend_from_slice(bytes);
            key
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Value {
        Value::String(s.into())
    }

    /// Equal bytes share a key, distinct from text and from other bytes.
    #[test]
    fn equal_bytes_share_a_key() {
        let key = kv_index_key(&Value::Bytes(vec![1, 2]));
        assert_eq!(kv_probe_key(&Value::Bytes(vec![1, 2])), Some(key.clone()));
        assert_ne!(kv_index_key(&Value::Bytes(vec![1, 3])), key);
        assert_ne!(kv_index_key(&text("\u{1}\u{2}")), key);
    }

    /// An integer, its numeric string, and the equal float share a key.
    #[test]
    fn equal_numbers_share_a_key() {
        let key = kv_index_key(&Value::Integer(5));
        assert_eq!(kv_index_key(&text("5")), key);
        assert_eq!(kv_index_key(&Value::Float(5.0)), key);
        assert_eq!(kv_probe_key(&Value::Integer(5)), Some(key));
        assert_ne!(
            kv_index_key(&Value::Integer(6)),
            kv_index_key(&Value::Integer(5))
        );
    }

    /// A datetime and the string naming the same instant share a key.
    #[test]
    fn a_datetime_and_its_text_share_a_key() {
        let dt = nodedb_types::NdbDateTime::parse("2024-01-02T03:04:05").expect("instant");
        assert_eq!(
            kv_index_key(&Value::DateTime(dt)),
            kv_index_key(&text("2024-01-02T03:04:05"))
        );
        assert_eq!(
            kv_index_key(&Value::NaiveDateTime(dt)),
            kv_index_key(&text("2024-01-02 03:04:05"))
        );
    }

    /// Plain text keys by its bytes, apart from every other type.
    #[test]
    fn text_keys_apart_from_numbers_and_bools() {
        assert_ne!(kv_index_key(&text("abc")), kv_index_key(&text("abd")));
        assert_ne!(
            kv_index_key(&text("true")),
            kv_index_key(&Value::Bool(true))
        );
        assert_ne!(
            kv_index_key(&Value::Bool(false)),
            kv_index_key(&Value::Integer(0))
        );
    }

    /// NULL is filed but never probed.
    #[test]
    fn null_is_filed_but_never_probed() {
        assert_eq!(kv_index_key(&Value::Null), NULL_KEY.to_vec());
        assert_eq!(kv_probe_key(&Value::Null), None);
    }

    /// Number and instant keys order by value.
    #[test]
    fn number_and_instant_keys_order_by_value() {
        assert!(kv_index_key(&Value::Integer(-3)) < kv_index_key(&Value::Float(-2.5)));
        assert!(kv_index_key(&Value::Float(-2.5)) < kv_index_key(&Value::Integer(10)));
        assert!(kv_index_key(&text("1999-12-31")) < kv_index_key(&text("2000-01-01")));
    }
}
