// SPDX-License-Identifier: Apache-2.0

//! The one rule for turning a scalar into a raw KV body.
//!
//! A KV row written through the single-`value` SQL column, or through RESP
//! `SET`, stores its scalar as raw bytes rather than a msgpack map. The SQL
//! lowering and every KV read-modify-write encode through this function, so
//! a merged row re-encodes byte-for-byte as a fresh insert of the same
//! scalar.

use crate::value::core::Value;

/// The value is not a scalar, so it has no raw KV body form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{kind} has no raw KV body form; only a scalar does")]
pub struct NotScalar {
    /// `Value::type_name()` of the offending value.
    pub kind: &'static str,
}

/// Raw KV body bytes for a scalar.
///
/// - string, UUID, ULID: the UTF-8 bytes
/// - bytes: verbatim
/// - integer, float, decimal, bool: decimal / `true` / `false` text
/// - timestamps: ISO-8601 text
/// - null: empty
pub fn scalar_to_raw_bytes(value: &Value) -> Result<Vec<u8>, NotScalar> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Bool(b) => Ok(b.to_string().into_bytes()),
        Value::Integer(i) => Ok(i.to_string().into_bytes()),
        Value::Float(f) => Ok(f.to_string().into_bytes()),
        Value::Decimal(d) => Ok(d.to_string().into_bytes()),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => Ok(s.as_bytes().to_vec()),
        Value::Bytes(b) => Ok(b.clone()),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => Ok(dt.to_iso8601().into_bytes()),
        Value::Duration(_)
        | Value::Array(_)
        | Value::Object(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Geometry(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        | Value::Vector(_) => Err(NotScalar {
            kind: value.type_name(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn string_is_its_utf8_bytes() {
        assert_eq!(
            scalar_to_raw_bytes(&Value::String("v1".into())).unwrap(),
            b"v1".to_vec()
        );
    }

    #[test]
    fn integer_is_decimal_text() {
        assert_eq!(
            scalar_to_raw_bytes(&Value::Integer(-42)).unwrap(),
            b"-42".to_vec()
        );
    }

    #[test]
    fn bool_and_null() {
        assert_eq!(
            scalar_to_raw_bytes(&Value::Bool(true)).unwrap(),
            b"true".to_vec()
        );
        assert!(scalar_to_raw_bytes(&Value::Null).unwrap().is_empty());
    }

    #[test]
    fn object_is_not_scalar() {
        let err = scalar_to_raw_bytes(&Value::Object(HashMap::new())).unwrap_err();
        assert_eq!(err.kind, Value::Object(HashMap::new()).type_name());
    }
}
