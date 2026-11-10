// SPDX-License-Identifier: BUSL-1.1

//! Canonical value identity for row deduplication.
//!
//! Set operations and `DISTINCT` decide whether two rows are one row. A value
//! reads typed, from a plain map or a native object, and its identity is the
//! shared canonical group identity
//! (`nodedb_query::msgpack_scan::group_key::encode_group_values`): `5`, `5.0`
//! and `5.00` are one value of any numeric type, and a string stays a string.
//! A nested object compares independent of its map order.

use nodedb_query::msgpack_scan::group_key::encode_group_values;
use nodedb_types::{ObjectView, Value};

/// The marker that opens an object's ordered form, so an object never
/// matches an array that happens to hold the same pairs.
const OBJECT_MARKER: &str = "\u{0}object";

/// `value` with every object rewritten as its key-ordered pairs, so its
/// identity does not depend on map order.
pub fn ordered(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut pairs: Vec<(String, Value)> = map.into_iter().collect();
            pairs.sort_by(|left, right| left.0.cmp(&right.0));
            let mut items = Vec::with_capacity(pairs.len() + 1);
            items.push(Value::String(OBJECT_MARKER.to_string()));
            for (key, value) in pairs {
                items.push(Value::Array(vec![Value::String(key), ordered(value)]));
            }
            Value::Array(items)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(ordered).collect()),
        Value::Set(items) => Value::Set(items.into_iter().map(ordered).collect()),
        other => other,
    }
}

/// The canonical identity text of `values`, in order.
pub fn canonical_text(values: &[Value]) -> crate::Result<String> {
    encode_group_values(values).map_err(|e| crate::Error::Codec {
        detail: format!("a row value has no canonical identity: {e}"),
    })
}

/// The identity of one row object, plain or native, compared by column
/// name: a sorted set of name/value pairs. Column order and encoding do not
/// change it.
pub fn named_row_identity(row: &[u8]) -> crate::Result<Vec<String>> {
    let view = ObjectView::new(row).map_err(|e| row_error(row, e))?;
    let mut parts = Vec::new();
    for entry in view.fields() {
        let (key, bytes) = entry.map_err(|e| row_error(row, e))?;
        let value = view.decode_value(bytes).map_err(|e| row_error(row, e))?;
        parts.push(canonical_text(&[
            Value::String(key.to_owned()),
            ordered(value),
        ])?);
    }
    parts.sort_unstable();
    Ok(parts)
}

/// The error for a row that does not read as a row object.
pub fn row_error(row: &[u8], detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Codec {
        detail: format!("a {}-byte row is not a row object: {detail}", row.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native(pairs: Vec<(&str, Value)>) -> Vec<u8> {
        let row = Value::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect());
        zerompk::to_msgpack_vec(&row).unwrap()
    }

    /// Numbers compare by value (a native Decimal 5.0 and a plain integer 5),
    /// a string stays a string, and map order and encoding do not matter.
    #[test]
    fn rows_of_one_value_share_an_identity() {
        let five = named_row_identity(&native(vec![
            ("v", Value::Decimal("5.0".parse().unwrap())),
            ("w", Value::Integer(1)),
        ]))
        .unwrap();
        let plain = nodedb_types::ordered_map_to_msgpack(&[
            ("w", &Value::Integer(1)),
            ("v", &Value::Integer(5)),
        ])
        .unwrap();
        assert_eq!(named_row_identity(&plain).unwrap(), five);
        let text = named_row_identity(&native(vec![
            ("v", Value::String("5.0".into())),
            ("w", Value::Integer(1)),
        ]))
        .unwrap();
        assert_ne!(text, five);
    }

    #[test]
    fn a_non_object_row_is_an_error() {
        assert!(matches!(
            named_row_identity(&[0x2a]),
            Err(crate::Error::Codec { .. })
        ));
    }
}
