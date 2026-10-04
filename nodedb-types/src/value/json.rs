// SPDX-License-Identifier: Apache-2.0

//! Conversions between `Value` and `serde_json::Value`.

use std::str::FromStr;

use super::core::Value;

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Integer(i) => serde_json::json!(i),
            Value::Float(f) => serde_json::json!(f),
            Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                serde_json::Value::String(s)
            }
            Value::Bytes(b) => serde_json::Value::String(hex::encode(b)),
            Value::Array(arr) | Value::Set(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
            Value::DateTime(dt) | Value::NaiveDateTime(dt) => {
                serde_json::Value::String(dt.to_string())
            }
            Value::Duration(d) => serde_json::Value::String(d.to_string()),
            Value::Decimal(d) => {
                // Represent as a JSON Number so clients see a numeric type,
                // not a quoted string. `from_str` handles the decimal notation
                // produced by rust_decimal's `to_string`. An integer past the
                // 64-bit range parses to a rounded float, so it keeps its
                // exact digits as a string.
                let s = d.to_string();
                match serde_json::Number::from_str(&s) {
                    Ok(n) if !(d.scale() == 0 && n.is_f64()) => serde_json::Value::Number(n),
                    _ => serde_json::Value::String(s),
                }
            }
            Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
            Value::Range { .. } | Value::Record { .. } => serde_json::Value::Null,
            Value::Vector(v) => {
                serde_json::Value::Array(v.iter().map(|f| serde_json::json!(*f)).collect())
            }
            Value::ArrayCell(cell) => {
                let mut obj = serde_json::json!({
                    "coords": cell.coords.into_iter().map(serde_json::Value::from).collect::<Vec<_>>(),
                    "attrs": cell.attrs.into_iter().map(serde_json::Value::from).collect::<Vec<_>>(),
                });
                if let Some(ts) = cell.system_time {
                    obj["_ts_system"] = serde_json::json!(ts);
                }
                obj
            }
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else if let Some(u) = n.as_u64() {
                    Value::from_u64(u)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(map) => {
                Value::Object(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_cell::ArrayCell;

    #[test]
    fn json_u64_above_i64_max_keeps_its_number() {
        let json: serde_json::Value =
            sonic_rs::from_str("18446744073709551615").expect("parse u64::MAX");
        assert_eq!(
            Value::from(json),
            Value::Decimal(rust_decimal::Decimal::from(u64::MAX))
        );
        let small: serde_json::Value = sonic_rs::from_str("42").expect("parse 42");
        assert_eq!(Value::from(small), Value::Integer(42));
    }

    #[test]
    fn integer_decimal_past_64_bits_keeps_exact_digits() {
        let big = rust_decimal::Decimal::from_i128_with_scale(2 * i128::from(u64::MAX), 0);
        let json = serde_json::Value::from(Value::Decimal(big));
        assert_eq!(
            json,
            serde_json::Value::String("36893488147419103230".into())
        );
        let max = serde_json::Value::from(Value::Decimal(rust_decimal::Decimal::from(u64::MAX)));
        assert_eq!(max, serde_json::json!(u64::MAX));
    }

    #[test]
    fn decimal_to_json_is_number_not_string() {
        let d = rust_decimal::Decimal::new(12345, 2); // 123.45
        let json = serde_json::Value::from(Value::Decimal(d));
        assert!(json.is_number(), "expected JSON Number, got {json:?}");
        assert_eq!(json.to_string(), "123.45");
    }

    // ── Documented-lossy JSON boundary conversions ────────────────────────
    //
    // These six variants lose type information when converted to JSON.
    // The tests below pin the exact lossy behavior so future drift is caught.

    #[test]
    fn json_lossy_uuid_becomes_string() {
        let v = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let json = serde_json::Value::from(v);
        assert!(json.is_string(), "Uuid must serialize as JSON string");
        // Round-trip: comes back as String, not Uuid
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::String(_)),
            "Uuid round-trips through JSON as String, got {rt:?}"
        );
    }

    #[test]
    fn json_lossy_ulid_becomes_string() {
        let v = Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into());
        let json = serde_json::Value::from(v);
        assert!(json.is_string(), "Ulid must serialize as JSON string");
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::String(_)),
            "Ulid round-trips through JSON as String, got {rt:?}"
        );
    }

    #[test]
    fn json_lossy_regex_becomes_string() {
        let v = Value::Regex(r"^\d+$".into());
        let json = serde_json::Value::from(v);
        assert!(json.is_string(), "Regex must serialize as JSON string");
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::String(_)),
            "Regex round-trips through JSON as String, got {rt:?}"
        );
    }

    #[test]
    fn json_lossy_range_becomes_null() {
        let v = Value::Range {
            start: Some(Box::new(Value::Integer(1))),
            end: Some(Box::new(Value::Integer(10))),
            inclusive: false,
        };
        let json = serde_json::Value::from(v);
        assert!(
            json.is_null(),
            "Range must serialize as JSON null, got {json:?}"
        );
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::Null),
            "Range round-trips through JSON as Null, got {rt:?}"
        );
    }

    #[test]
    fn json_lossy_record_becomes_null() {
        let v = Value::Record {
            table: "users".into(),
            id: "abc123".into(),
        };
        let json = serde_json::Value::from(v);
        assert!(
            json.is_null(),
            "Record must serialize as JSON null, got {json:?}"
        );
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::Null),
            "Record round-trips through JSON as Null, got {rt:?}"
        );
    }

    #[test]
    fn json_lossy_array_cell_becomes_object_without_discriminator() {
        let v = Value::ArrayCell(ArrayCell {
            coords: vec![Value::Integer(1), Value::Integer(2)],
            attrs: vec![Value::Float(3.5)],
            system_time: None,
        });
        let json = serde_json::Value::from(v);
        assert!(
            json.is_object(),
            "ArrayCell must serialize as JSON object, got {json:?}"
        );
        // The object has "coords" and "attrs" keys but no type discriminator.
        let obj = json.as_object().unwrap();
        assert!(
            obj.contains_key("coords"),
            "ArrayCell JSON must have 'coords' key"
        );
        assert!(
            obj.contains_key("attrs"),
            "ArrayCell JSON must have 'attrs' key"
        );
        // Round-trip comes back as Object, not ArrayCell.
        let rt = Value::from(json);
        assert!(
            matches!(rt, Value::Object(_)),
            "ArrayCell round-trips through JSON as Object, got {rt:?}"
        );
    }
}
