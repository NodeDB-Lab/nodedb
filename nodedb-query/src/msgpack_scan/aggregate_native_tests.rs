// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn native_aggregate_fields_and_expressions_keep_decimal_exactness() {
    let decimal = Value::Decimal("9007199254740993.125".parse().unwrap());
    let value = Value::Object(std::collections::HashMap::from([
        ("amount".to_owned(), decimal.clone()),
        ("nothing".to_owned(), Value::Null),
    ]));
    let doc = zerompk::to_msgpack_vec(&value).unwrap();
    let expression = SqlExpr::Column("amount".to_owned());
    let crs = CrsContext::builtin_only();
    for expr in [None, Some(&expression)] {
        assert_eq!(
            extract_sum_value(&doc, "amount", expr, &crs).unwrap(),
            Some(decimal.clone())
        );
        assert_eq!(
            extract_value(&doc, "amount", expr, &crs).unwrap(),
            Some(decimal.clone())
        );
    }
    assert_eq!(extract_non_null(&doc, "nothing", None, &crs).unwrap(), None);
    assert_eq!(extract_non_null(&doc, "missing", None, &crs).unwrap(), None);
    assert_eq!(extract_bytes(&doc, "nothing", None, &crs).unwrap(), None);
    assert_eq!(
        extract_bytes(&doc, "amount", None, &crs).unwrap(),
        extract_bytes(&doc, "amount", Some(&expression), &crs).unwrap()
    );
}

#[test]
fn native_arrays_and_plain_arrays_keep_the_same_value_identity() {
    let value = Value::Object(std::collections::HashMap::from([(
        "v".to_owned(),
        Value::Array(vec![Value::Integer(2), Value::Integer(42)]),
    )]));
    let native = zerompk::to_msgpack_vec(&value).unwrap();
    let plain = nodedb_types::value_to_msgpack(&value).unwrap();
    let crs = CrsContext::builtin_only();
    assert_eq!(
        extract_value(&native, "v", None, &crs).unwrap(),
        extract_value(&plain, "v", None, &crs).unwrap()
    );
    assert_eq!(
        extract_bytes(&native, "v", None, &crs).unwrap(),
        extract_bytes(&plain, "v", None, &crs).unwrap()
    );
}

#[test]
fn malformed_native_aggregate_arguments_return_errors() {
    let crs = CrsContext::builtin_only();
    let malformed = [0x92, 7, 0x81, 0xa1, b'v', 0x91, 2];
    assert!(extract_sum_value(&malformed, "v", None, &crs).is_err());
    assert!(
        extract_value(
            &malformed,
            "v",
            Some(&SqlExpr::Column("v".to_owned())),
            &crs
        )
        .is_err()
    );
}

fn row(value: Value) -> Vec<u8> {
    zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([(
        "v".to_owned(),
        value,
    )])))
    .unwrap()
}

/// Distinct identity agrees with GROUP BY: numbers equal by exact value,
/// signed zeros, and every NaN payload each collapse to one identity, while
/// a decimal that differs from a float by exact value stays apart.
#[test]
fn distinct_identity_follows_canonical_value_equality() {
    let crs = CrsContext::builtin_only();
    let identity = |value: Value| extract_bytes(&row(value), "v", None, &crs).unwrap();
    let five = identity(Value::Integer(5));
    assert_eq!(identity(Value::Float(5.0)), five);
    assert_eq!(identity(Value::Decimal("5.0".parse().unwrap())), five);
    assert_eq!(identity(Value::Decimal("5.00".parse().unwrap())), five);
    assert_eq!(identity(Value::Float(-0.0)), identity(Value::Float(0.0)));
    assert_eq!(
        identity(Value::Float(f64::from_bits(f64::NAN.to_bits() | 7))),
        identity(Value::Float(f64::NAN))
    );
    assert_ne!(
        identity(Value::Decimal("0.1".parse().unwrap())),
        identity(Value::Float(0.1))
    );
    assert_ne!(identity(Value::String("5".into())), five);
}
