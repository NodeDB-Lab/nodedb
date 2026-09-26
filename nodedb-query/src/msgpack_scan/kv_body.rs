// SPDX-License-Identifier: Apache-2.0

//! Shape-preserving decode / encode of a KV row body for read-modify-write.
//!
//! A KV body is stored in one of two shapes (see [`super::kv_row_msgpack`]):
//! a msgpack map for typed columns, or raw scalar bytes for the single-`value`
//! SQL form and RESP `SET`. A merge decodes the body into the same row object
//! reads present, mutates it, and re-encodes into the shape it came from.
//! Decoding raw bytes as msgpack instead either fails (multi-byte value) or
//! reads the first byte as a fixint and discards the rest.

use std::collections::HashMap;

use nodedb_types::{MsgpackError, NotScalar, Value, scalar_to_raw_bytes};

use crate::msgpack_scan::{map_header, write_map_header, write_str};

/// The on-disk shape of a KV row body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvBodyShape {
    /// Scalar bytes, not msgpack: the single-`value` SQL form and RESP `SET`.
    Raw,
    /// A msgpack map of typed columns.
    Map,
}

/// A merged row cannot be encoded back into its body shape.
#[derive(Debug, thiserror::Error)]
pub enum KvBodyError {
    /// The stored map body is not well-formed msgpack.
    #[error("KV map body: {0}")]
    Decode(#[from] MsgpackError),
    /// The merged row is not an object.
    #[error("KV row must be an object, got {kind}")]
    RowNotObject {
        /// `Value::type_name()` of the merged row.
        kind: &'static str,
    },
    /// A raw body holds only `value`; the merged row lost it.
    #[error("KV raw body has no `value` field after merge")]
    RawMissingValue,
    /// A raw body holds only `value`; the merged row carries other keys.
    #[error("KV row holds a bare `value`, not typed columns; cannot set {keys}")]
    RawExtraKeys {
        /// The offending keys, sorted, comma-separated.
        keys: String,
    },
    /// A raw body holds a scalar; the merged `value` is not one.
    #[error("KV raw body `value`: {0}")]
    RawNotScalar(#[from] NotScalar),
    /// The merged map row does not encode.
    #[error("KV map body encode: {0}")]
    Encode(zerompk::Error),
}

/// The shape of a stored body: a msgpack map header marks a map, anything
/// else (an empty body included) is raw scalar bytes.
pub fn kv_body_shape(body: &[u8]) -> KvBodyShape {
    if map_header(body, 0).is_some() {
        KvBodyShape::Map
    } else {
        KvBodyShape::Raw
    }
}

/// Decode a stored body into the row object a merge operates on.
///
/// A map body decodes as is. A raw body becomes `{"value": <string>}`, the
/// exact row `kv_row_msgpack` presents to reads (non-UTF-8 bytes take the
/// lossy view there too). An empty body is a raw empty string. Returns the
/// shape so the writer re-encodes in the same one.
pub fn kv_body_to_row(body: &[u8]) -> Result<(Value, KvBodyShape), MsgpackError> {
    if kv_body_shape(body) == KvBodyShape::Map {
        return Ok((nodedb_types::value_from_msgpack(body)?, KvBodyShape::Map));
    }
    let mut row = std::collections::HashMap::with_capacity(1);
    row.insert(
        "value".to_string(),
        Value::String(String::from_utf8_lossy(body).into_owned()),
    );
    Ok((Value::Object(row), KvBodyShape::Raw))
}

/// Encode a merged row back into `shape`.
///
/// `Map` writes the fields in key order, so the same logical row encodes to
/// the same bytes on every node and on WAL replay. `Raw` accepts only an
/// object whose single key is `value` holding a scalar; the scalar encodes
/// through [`scalar_to_raw_bytes`], the same rule the SQL lowering uses for
/// a fresh insert. Any other object is an error naming the extra keys: a raw
/// row never silently turns into a map.
pub fn row_to_kv_body(row: &Value, shape: KvBodyShape) -> Result<Vec<u8>, KvBodyError> {
    let Value::Object(map) = row else {
        return Err(KvBodyError::RowNotObject {
            kind: row.type_name(),
        });
    };
    match shape {
        KvBodyShape::Map => {
            let mut fields: Vec<(&String, &Value)> = map.iter().collect();
            fields.sort_unstable_by(|a, b| a.0.cmp(b.0));
            let mut buf = Vec::with_capacity(map.len() * 16);
            write_map_header(&mut buf, fields.len());
            for (key, value) in fields {
                write_str(&mut buf, key);
                let encoded = nodedb_types::value_to_msgpack(value).map_err(KvBodyError::Encode)?;
                buf.extend_from_slice(&encoded);
            }
            Ok(buf)
        }
        KvBodyShape::Raw => {
            let mut extra: Vec<&str> = map
                .keys()
                .map(String::as_str)
                .filter(|k| *k != "value")
                .collect();
            if !extra.is_empty() {
                extra.sort_unstable();
                return Err(KvBodyError::RawExtraKeys {
                    keys: extra.join(", "),
                });
            }
            let value = map.get("value").ok_or(KvBodyError::RawMissingValue)?;
            Ok(scalar_to_raw_bytes(value)?)
        }
    }
}

/// The fields a KV body stores for a msgpack-encoded row, and the body shape.
///
/// This is the inverse of [`super::kv_row_msgpack`]. `key` is the entry's key.
/// A `key` field equal to it is the injected primary key and is dropped. A
/// `key` field that differs is a stored column and is kept. The remaining
/// fields pick the shape the SQL lowering picks for a fresh insert:
/// - `value` alone: [`KvBodyShape::Raw`];
/// - any other field set: [`KvBodyShape::Map`].
///
/// The caller encodes the fields in its own body format.
/// [`row_to_kv_body`] is the Origin format.
pub fn kv_row_to_body_fields(
    key: &str,
    row: &[u8],
) -> Result<(HashMap<String, Value>, KvBodyShape), KvBodyError> {
    let row = nodedb_types::value_from_msgpack(row)?;
    let Value::Object(mut fields) = row else {
        return Err(KvBodyError::RowNotObject {
            kind: row.type_name(),
        });
    };
    if matches!(fields.get("key"), Some(Value::String(stored)) if stored == key) {
        fields.remove("key");
    }
    let shape = if fields.len() == 1 && fields.contains_key("value") {
        KvBodyShape::Raw
    } else {
        KvBodyShape::Map
    };
    Ok((fields, shape))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body_from_row(key: &str, row: &[u8]) -> Vec<u8> {
        let (fields, shape) = kv_row_to_body_fields(key, row).unwrap();
        row_to_kv_body(&Value::Object(fields), shape).unwrap()
    }

    #[test]
    fn a_raw_body_round_trips_through_its_row() {
        for body in [b"v1".to_vec(), b"1".to_vec(), Vec::new()] {
            let row = crate::msgpack_scan::kv_row_msgpack("k1", &body);
            assert_eq!(body_from_row("k1", &row), body);
        }
    }

    #[test]
    fn a_map_body_round_trips_through_its_row() {
        let mut fields = HashMap::new();
        fields.insert("n".to_string(), Value::Integer(7));
        fields.insert("s".to_string(), Value::String("x".into()));
        let body = row_to_kv_body(&Value::Object(fields), KvBodyShape::Map).unwrap();
        let row = crate::msgpack_scan::kv_row_msgpack("k2", &body);
        assert_eq!(body_from_row("k2", &row), body);
    }

    #[test]
    fn a_key_column_that_differs_from_the_entry_key_is_kept() {
        let mut fields = HashMap::new();
        fields.insert("key".to_string(), Value::String("stored".into()));
        fields.insert("n".to_string(), Value::Integer(1));
        let body = row_to_kv_body(&Value::Object(fields), KvBodyShape::Map).unwrap();
        let row = crate::msgpack_scan::kv_row_msgpack("slot", &body);
        assert_eq!(body_from_row("slot", &row), body);
    }

    #[test]
    fn a_row_that_is_not_a_map_is_an_error() {
        let not_a_map = nodedb_types::value_to_msgpack(&Value::Integer(118)).unwrap();
        assert!(matches!(
            kv_row_to_body_fields("k", &not_a_map),
            Err(KvBodyError::RowNotObject { kind: "int" })
        ));
    }

    #[test]
    fn bytes_that_are_not_msgpack_are_a_decode_error() {
        assert!(matches!(
            kv_row_to_body_fields("k", &[0x81]),
            Err(KvBodyError::Decode(_))
        ));
    }

    fn value_of(row: &Value) -> &Value {
        row.get("value").expect("row carries `value`")
    }

    #[test]
    fn raw_string_round_trips_in_raw_shape() {
        let (row, shape) = kv_body_to_row(b"second-longer-value").unwrap();
        assert_eq!(shape, KvBodyShape::Raw);
        assert_eq!(value_of(&row), &Value::String("second-longer-value".into()));
        assert_eq!(
            row_to_kv_body(&row, shape).unwrap(),
            b"second-longer-value".to_vec()
        );
    }

    #[test]
    fn raw_single_byte_value_is_a_string_not_a_fixint() {
        // `b"1"` is 0x31, a valid msgpack fixint. It must still read as the
        // string "1" and write back as the byte 0x31.
        let (row, shape) = kv_body_to_row(b"1").unwrap();
        assert_eq!(shape, KvBodyShape::Raw);
        assert_eq!(value_of(&row), &Value::String("1".into()));
        assert_eq!(row_to_kv_body(&row, shape).unwrap(), b"1".to_vec());
    }

    #[test]
    fn raw_integer_value_encodes_as_decimal_text() {
        let mut row = HashMap::new();
        row.insert("value".to_string(), Value::Integer(42));
        assert_eq!(
            row_to_kv_body(&Value::Object(row), KvBodyShape::Raw).unwrap(),
            b"42".to_vec()
        );
    }

    #[test]
    fn map_body_round_trips_in_map_shape() {
        let mut fields = HashMap::new();
        fields.insert("n".to_string(), Value::Integer(7));
        let body = nodedb_types::value_to_msgpack(&Value::Object(fields.clone())).unwrap();

        let (row, shape) = kv_body_to_row(&body).unwrap();
        assert_eq!(shape, KvBodyShape::Map);
        assert_eq!(row, Value::Object(fields.clone()));

        let out = row_to_kv_body(&row, shape).unwrap();
        assert_eq!(
            nodedb_types::value_from_msgpack(&out).unwrap(),
            Value::Object(fields)
        );
    }

    #[test]
    fn map_body_encodes_fields_in_key_order() {
        let mut fields = HashMap::new();
        fields.insert("b".to_string(), Value::Integer(2));
        fields.insert("a".to_string(), Value::Integer(1));
        let out = row_to_kv_body(&Value::Object(fields), KvBodyShape::Map).unwrap();
        assert_eq!(out[0], 0x82, "fixmap of two entries");
        let key_at = |name: u8| out.windows(2).position(|w| w == [0xa1, name]).unwrap();
        assert!(
            key_at(b'a') < key_at(b'b'),
            "keys must be written in sorted order"
        );
    }

    #[test]
    fn raw_shape_with_extra_keys_is_an_error_naming_them() {
        let mut row = HashMap::new();
        row.insert("value".to_string(), Value::String("x".into()));
        row.insert("n".to_string(), Value::Integer(1));
        row.insert("a".to_string(), Value::Integer(2));
        let err = row_to_kv_body(&Value::Object(row), KvBodyShape::Raw).unwrap_err();
        match err {
            KvBodyError::RawExtraKeys { keys } => assert_eq!(keys, "a, n"),
            other => panic!("expected RawExtraKeys, got {other:?}"),
        }
    }

    #[test]
    fn raw_shape_with_non_scalar_value_is_an_error() {
        let mut row = HashMap::new();
        row.insert("value".to_string(), Value::Array(vec![Value::Integer(1)]));
        let err = row_to_kv_body(&Value::Object(row), KvBodyShape::Raw).unwrap_err();
        assert!(matches!(err, KvBodyError::RawNotScalar(_)), "{err:?}");
    }

    #[test]
    fn non_object_row_is_an_error_in_either_shape() {
        for shape in [KvBodyShape::Raw, KvBodyShape::Map] {
            let err = row_to_kv_body(&Value::Integer(1), shape).unwrap_err();
            assert!(matches!(err, KvBodyError::RowNotObject { kind: "int" }));
        }
    }

    #[test]
    fn empty_body_is_a_raw_empty_string() {
        let (row, shape) = kv_body_to_row(b"").unwrap();
        assert_eq!(shape, KvBodyShape::Raw);
        assert_eq!(value_of(&row), &Value::String(String::new()));
        assert!(row_to_kv_body(&row, shape).unwrap().is_empty());
    }

    #[test]
    fn corrupt_map_body_is_a_decode_error() {
        // fixmap header claiming one entry, then nothing.
        assert!(kv_body_to_row(&[0x81]).is_err());
    }
}
