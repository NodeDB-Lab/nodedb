// SPDX-License-Identifier: Apache-2.0

//! Convert [`Value`] to deterministic, injection-safe SQL literal text.

use super::core::Value;
use crate::quote_literal;

impl Value {
    /// Convert this value into SQL literal text.
    ///
    /// This is total: every `Value` variant has an explicit representation and
    /// every externally-derived string is emitted through [`quote_literal`].
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Bool(value) => if *value { "TRUE" } else { "FALSE" }.into(),
            Value::Integer(value) => value.to_string(),
            Value::Float(value) => finite_float_literal(*value),
            Value::String(value)
            | Value::Uuid(value)
            | Value::Ulid(value)
            | Value::Regex(value) => quote_literal(value),
            Value::Bytes(value) => quote_literal(&format!("\\x{}", hex::encode(value))),
            Value::Array(values) | Value::Set(values) => array_literal(values),
            Value::Object(values) => object_literal(values),
            Value::DateTime(value) | Value::NaiveDateTime(value) => {
                quote_literal(&value.to_string())
            }
            Value::Duration(value) => quote_literal(&value.to_string()),
            Value::Decimal(value) => value.to_string(),
            // `SridGeometry`'s Display is total and deterministic, and avoids a
            // fallible JSON serializer on this infallible public API.
            Value::Geometry(value) => quote_literal(&value.to_string()),
            Value::Range {
                start,
                end,
                inclusive,
            } => {
                let start = start
                    .as_deref()
                    .map_or_else(|| "unbounded".into(), Value::to_sql_literal);
                let end = end
                    .as_deref()
                    .map_or_else(|| "unbounded".into(), Value::to_sql_literal);
                quote_literal(&format!(
                    "{start}{}{end}",
                    if *inclusive { "..=" } else { ".." }
                ))
            }
            Value::Record { table, id } => quote_literal(&format!("{table}:{id}")),
            Value::ArrayCell(cell) => {
                let system_time = cell
                    .system_time
                    .map_or_else(|| "NULL".into(), |value| value.to_string());
                quote_literal(&format!(
                    "ArrayCell(coords={}, attrs={}, system_time={system_time})",
                    array_literal(&cell.coords),
                    array_literal(&cell.attrs)
                ))
            }
            Value::Vector(values) => {
                let elements = values
                    .iter()
                    .map(|value| finite_float_literal(f64::from(*value)))
                    .collect::<Vec<_>>();
                format!("ARRAY[{}]", elements.join(", "))
            }
        }
    }
}

fn finite_float_literal(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        quote_literal(&value.to_string())
    }
}

fn array_literal(values: &[Value]) -> String {
    let values = values.iter().map(Value::to_sql_literal).collect::<Vec<_>>();
    format!("ARRAY[{}]", values.join(", "))
}

fn object_literal(values: &std::collections::HashMap<String, Value>) -> String {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| *key);
    let entries = entries
        .into_iter()
        .map(|(key, value)| format!("{key}: {}", value.to_sql_literal()))
        .collect::<Vec<_>>();
    quote_literal(&format!("{{{}}}", entries.join(", ")))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::array_cell::ArrayCell;
    use crate::geometry::{Geometry, Srid, SridGeometry};

    #[test]
    fn renders_every_value_variant_without_null_fallback() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Integer(-7),
            Value::Float(1.5),
            Value::String("O'Reilly\n雪".into()),
            Value::Bytes(vec![0, 0xff]),
            Value::Array(vec![Value::Integer(1)]),
            Value::Object(HashMap::from([(
                "key".into(),
                Value::String("value".into()),
            )])),
            Value::Uuid("id'quoted".into()),
            Value::Ulid("ulid'quoted".into()),
            Value::DateTime(crate::datetime::NdbDateTime::from_micros(0)),
            Value::NaiveDateTime(crate::datetime::NdbDateTime::from_micros(0)),
            Value::Duration(crate::datetime::NdbDuration::from_micros(1)),
            Value::Decimal(rust_decimal::Decimal::new(125, 2)),
            Value::Geometry(SridGeometry::wgs84(Geometry::point(1.0, 2.0))),
            Value::Set(vec![Value::Bool(false)]),
            Value::Regex("a'b".into()),
            Value::Range {
                start: Some(Box::new(Value::Integer(1))),
                end: None,
                inclusive: true,
            },
            Value::Record {
                table: "ta'ble".into(),
                id: "id'雪".into(),
            },
            Value::ArrayCell(ArrayCell {
                coords: vec![Value::Integer(1)],
                attrs: vec![Value::String("a'b".into())],
                system_time: Some(7),
            }),
            Value::Vector(Arc::from([1.0_f32, 2.0_f32])),
        ];
        for value in values {
            let literal = value.to_sql_literal();
            if !matches!(value, Value::Null) {
                assert_ne!(
                    literal, "NULL",
                    "non-null variant must not fall back to NULL: {value:?}"
                );
            }
        }
    }

    #[test]
    fn string_derived_values_use_shared_quote_literal() {
        let value = Value::Record {
            table: "t'\n雪".into(),
            id: "i'\r".into(),
        };
        assert_eq!(
            Value::String("t'\n雪".into()).to_sql_literal(),
            quote_literal("t'\n雪")
        );
        assert_eq!(value.to_sql_literal(), quote_literal("t'\n雪:i'\r"));
        assert_eq!(
            Value::Bytes(vec![0xab]).to_sql_literal(),
            quote_literal("\\xab")
        );
    }

    #[test]
    fn nonfinite_floats_are_quoted_and_nested_object_is_sorted() {
        assert_eq!(
            Value::Float(f64::NAN).to_sql_literal(),
            quote_literal("NaN")
        );
        assert_eq!(
            Value::Float(f64::INFINITY).to_sql_literal(),
            quote_literal("inf")
        );
        let object = Value::Object(HashMap::from([
            ("z'key".into(), Value::Float(f64::NEG_INFINITY)),
            ("a".into(), Value::Array(vec![Value::String("o'k".into())])),
        ]));
        assert_eq!(
            object.to_sql_literal(),
            quote_literal("{a: ARRAY['o''k'], z'key: '-inf'}")
        );
    }

    #[test]
    fn array_cell_nested_objects_are_deterministic() {
        let left = HashMap::from([
            ("z".to_string(), Value::Integer(2)),
            ("a".to_string(), Value::String("x'y".into())),
        ]);
        let right = HashMap::from([
            ("a".to_string(), Value::String("x'y".into())),
            ("z".to_string(), Value::Integer(2)),
        ]);
        let render = |object| {
            Value::ArrayCell(ArrayCell {
                coords: vec![Value::Object(object)],
                attrs: Vec::new(),
                system_time: None,
            })
            .to_sql_literal()
        };
        assert_eq!(render(left), render(right));
    }

    #[test]
    fn geometry_and_ranges_have_explicit_safe_representations() {
        assert_eq!(
            Value::Geometry(SridGeometry::wgs84(Geometry::point(1.0, 2.0))).to_sql_literal(),
            quote_literal("Point { coordinates: [1.0, 2.0] }")
        );
        assert_eq!(
            Value::Geometry(SridGeometry::new(
                Srid::new(32648).unwrap(),
                Geometry::point(1.0, 2.0)
            ))
            .to_sql_literal(),
            quote_literal("SRID=32648;Point { coordinates: [1.0, 2.0] }")
        );
        assert_eq!(
            Value::Range {
                start: Some(Box::new(Value::String("a'b".into()))),
                end: Some(Box::new(Value::Integer(2))),
                inclusive: false
            }
            .to_sql_literal(),
            quote_literal("'a''b'..2")
        );
    }
}
