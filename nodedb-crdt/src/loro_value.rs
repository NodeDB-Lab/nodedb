// SPDX-License-Identifier: Apache-2.0

//! The one encoding of a [`nodedb_types::Value`] inside a Loro document.
//!
//! Loro holds null, booleans, `i64`, `f64`, strings, lists, maps and binary.
//! The kinds Loro holds natively map to their Loro form:
//!
//! | `Value`   | `LoroValue` |
//! |-----------|-------------|
//! | `Null`    | `Null`      |
//! | `Bool`    | `Bool`      |
//! | `Integer` | `I64`       |
//! | `Float`   | `Double`    |
//! | `String`  | `String`    |
//! | `Array`   | `List`      |
//! | `Object`  | `Map`       |
//!
//! Every other kind is a `LoroValue::Binary` holding the value's tagged
//! MessagePack (`zerompk::to_msgpack_vec(&value)`): `Bytes`, `Decimal`,
//! `Uuid`, `Ulid`, `DateTime`, `NaiveDateTime`, `Duration`, `Geometry`,
//! `Set`, `Regex`, `Range`, `Record`, `ArrayCell` and `Vector`. Raw bytes
//! are a tagged `Bytes` value too, so every Loro binary in a NodeDB document
//! decodes one way. A Decimal keeps its scale, a UUID stays a UUID, an
//! instant keeps its kind and microseconds.
//!
//! Every writer of a NodeDB Loro document (Origin, Lite, a sync peer) encodes
//! through [`value_to_loro`] and every reader decodes through
//! [`loro_to_value`]. A binary that is not a tagged value is an error, never
//! read as bytes.

use std::collections::HashMap;

use loro::LoroValue;
use nodedb_types::Value;

use crate::error::{CrdtError, Result};

/// The Loro form of `value`.
pub fn value_to_loro(value: &Value) -> Result<LoroValue> {
    Ok(match value {
        Value::Null => LoroValue::Null,
        Value::Bool(b) => LoroValue::Bool(*b),
        Value::Integer(i) => LoroValue::I64(*i),
        Value::Float(f) => LoroValue::Double(*f),
        Value::String(s) => LoroValue::String(s.clone().into()),
        Value::Array(items) => LoroValue::List(
            items
                .iter()
                .map(value_to_loro)
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        Value::Object(map) => LoroValue::Map(fields_to_loro_map(map)?.into()),
        Value::Bytes(_)
        | Value::Uuid(_)
        | Value::Ulid(_)
        | Value::DateTime(_)
        | Value::NaiveDateTime(_)
        | Value::Duration(_)
        | Value::Decimal(_)
        | Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        | Value::Vector(_) => {
            let bytes =
                zerompk::to_msgpack_vec(value).map_err(|e| CrdtError::TypedValueMalformed {
                    detail: format!("{} value does not encode: {e}", value.type_name()),
                })?;
            LoroValue::Binary(bytes.into())
        }
        // `Value` is non-exhaustive: a kind added later is refused by name
        // until it is given a Loro form, never stored silently.
        other => {
            return Err(CrdtError::TypedValueMalformed {
                detail: format!(
                    "a {} value has no Loro encoding in this version",
                    other.type_name()
                ),
            });
        }
    })
}

/// The Loro form of each field of `fields`, keyed by field name.
pub fn fields_to_loro_map(fields: &HashMap<String, Value>) -> Result<HashMap<String, LoroValue>> {
    fields
        .iter()
        .map(|(name, value)| Ok((name.clone(), value_to_loro(value)?)))
        .collect()
}

/// The value `loro` encodes.
///
/// A `Container` is a reference to a nested CRDT container, not row data. It
/// reads as `Value::Null`. A deep value resolves containers before it gets
/// here.
pub fn loro_to_value(loro: &LoroValue) -> Result<Value> {
    Ok(match loro {
        LoroValue::Null | LoroValue::Container(_) => Value::Null,
        LoroValue::Bool(b) => Value::Bool(*b),
        LoroValue::I64(n) => Value::Integer(*n),
        LoroValue::Double(f) => Value::Float(*f),
        LoroValue::String(s) => Value::String(s.to_string()),
        LoroValue::Binary(bytes) => {
            let bytes = bytes.as_slice();
            zerompk::from_msgpack::<Value>(bytes).map_err(|e| CrdtError::TypedValueMalformed {
                detail: format!(
                    "a {}-byte Loro binary is not a tagged value: {e}",
                    bytes.len()
                ),
            })?
        }
        LoroValue::List(items) => Value::Array(
            items
                .iter()
                .map(loro_to_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        LoroValue::Map(map) => {
            let mut out = HashMap::with_capacity(map.len());
            for (name, item) in map.iter() {
                out.insert(name.clone(), loro_to_value(item)?);
            }
            Value::Object(out)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::NdbDateTime;

    fn round_trip(value: &Value) -> Value {
        let loro = value_to_loro(value).expect("encode");
        loro_to_value(&loro).expect("decode")
    }

    #[test]
    fn native_kinds_keep_their_loro_form() {
        assert_eq!(
            value_to_loro(&Value::Integer(7)).ok(),
            Some(LoroValue::I64(7))
        );
        assert_eq!(
            value_to_loro(&Value::String("hi".into())).ok(),
            Some(LoroValue::String("hi".into()))
        );
        assert_eq!(value_to_loro(&Value::Null).ok(), Some(LoroValue::Null));
        assert_eq!(round_trip(&Value::Float(1.5)), Value::Float(1.5));
        assert_eq!(round_trip(&Value::Bool(true)), Value::Bool(true));
    }

    #[test]
    fn typed_kinds_round_trip_exactly() {
        let at = NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant");
        for value in [
            Value::Decimal("12.50".parse().expect("decimal literal")),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::DateTime(at),
            Value::NaiveDateTime(at),
            Value::Bytes(vec![0x00, 0x01, 0xff]),
        ] {
            assert!(matches!(
                value_to_loro(&value).expect("encode"),
                LoroValue::Binary(_)
            ));
            assert_eq!(round_trip(&value), value, "{value:?}");
        }
        let Value::Decimal(price) =
            round_trip(&Value::Decimal("12.50".parse().expect("decimal literal")))
        else {
            panic!("a Decimal decodes as a Decimal");
        };
        assert_eq!(price.to_string(), "12.50", "the scale survives");
    }

    #[test]
    fn nested_containers_keep_typed_leaves() {
        let value = Value::Object(HashMap::from([(
            "items".to_string(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ]),
        )]));
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn a_binary_that_is_not_a_tagged_value_is_refused() {
        let raw = LoroValue::Binary(vec![0xc1].into());
        assert!(matches!(
            loro_to_value(&raw),
            Err(CrdtError::TypedValueMalformed { .. })
        ));
    }

    #[test]
    fn a_container_reference_reads_as_null() {
        let raw = LoroValue::Container(loro::ContainerID::Normal {
            peer: 1,
            counter: 0,
            container_type: loro::ContainerType::Map,
        });
        assert_eq!(loro_to_value(&raw).ok(), Some(Value::Null));
    }
}
