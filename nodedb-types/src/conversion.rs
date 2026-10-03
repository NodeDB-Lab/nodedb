// SPDX-License-Identifier: Apache-2.0

//! Conversions between `serde_json::Value` and `nodedb_types::Value`.
//!
//! Shared across crates to avoid duplicating JSON-to-Value logic.

use crate::Value;

/// A JSON number as the `Value` that keeps its exact number: an `i64` as an
/// `Integer`, a larger `u64` through [`Value::from_u64`], anything else as a
/// `Float`.
fn json_number(n: &serde_json::Number) -> Value {
    if let Some(i) = n.as_i64() {
        Value::Integer(i)
    } else if let Some(u) = n.as_u64() {
        Value::from_u64(u)
    } else {
        n.as_f64().map_or(Value::Null, Value::Float)
    }
}

/// Convert a `serde_json::Value` to a `Value` by consuming ownership.
///
/// Nested objects are preserved as `Value::Object`.
pub fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => json_number(&n),
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => Value::Array(arr.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
        ),
    }
}

/// Convert a `&serde_json::Value` to a `Value`, borrowing the input and
/// cloning only the leaves it must own.
///
/// Structure-preserving all the way down, exactly like [`json_to_value`];
/// this variant exists only so a caller holding a reference does not have to
/// clone a whole subtree just to convert it.
///
/// Nested objects and arrays MUST stay nested. An earlier version of this
/// function rendered objects as their JSON text for "tabular display", which
/// silently made every nested field of a row unreadable: a client
/// deserializing the row back into the struct it was written from got a
/// string where an object belonged, and a field that genuinely held a JSON
/// string was indistinguishable from one that had been flattened. Rendering
/// a value as text is a property of a textual wire protocol, so it belongs to
/// that protocol's encoder — never to a shared conversion every caller reaches
/// for.
pub fn json_to_value_ref(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => json_number(n),
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value_ref).collect()),
        serde_json::Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value_ref(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_preserves_nested_objects() {
        let v = serde_json::json!({"a": 1, "b": {"nested": true}});
        let val = json_to_value(v);
        match val {
            Value::Object(map) => {
                assert_eq!(map.get("a"), Some(&Value::Integer(1)));
                assert!(matches!(map.get("b"), Some(Value::Object(_))));
            }
            _ => panic!("expected Object"),
        }
    }

    /// The by-reference conversion must agree with the owned one all the way
    /// down. It once flattened objects into JSON text, which made every
    /// nested field of a row unreadable to a client deserializing it back
    /// into the struct it was written from.
    #[test]
    fn by_reference_preserves_nested_objects_and_arrays() {
        let v = serde_json::json!({"a": 1, "b": {"nested": true}, "c": [{"deep": "x"}]});
        assert_eq!(json_to_value_ref(&v), json_to_value(v.clone()));

        let Value::Object(map) = json_to_value_ref(&v) else {
            panic!("expected Object");
        };
        assert!(matches!(map.get("b"), Some(Value::Object(_))));
        let Some(Value::Array(items)) = map.get("c") else {
            panic!("expected Array");
        };
        assert!(matches!(items.first(), Some(Value::Object(_))));
    }

    #[test]
    fn primitives_roundtrip() {
        assert_eq!(json_to_value(serde_json::Value::Null), Value::Null);
        assert_eq!(
            json_to_value(serde_json::Value::Bool(true)),
            Value::Bool(true)
        );
        assert_eq!(json_to_value(serde_json::json!(42)), Value::Integer(42));
        let max = serde_json::json!(u64::MAX);
        let exact = Value::Decimal(rust_decimal::Decimal::from(u64::MAX));
        assert_eq!(json_to_value_ref(&max), exact);
        assert_eq!(json_to_value(max), exact);
        assert_eq!(
            json_to_value(serde_json::json!("hello")),
            Value::String("hello".into())
        );
    }
}
