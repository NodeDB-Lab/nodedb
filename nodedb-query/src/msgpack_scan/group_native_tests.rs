// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expr::SqlExpr;

#[test]
fn mixed_plain_and_native_rows_share_group_identity() {
    for value in [
        Value::Integer(42),
        Value::String("a\"b".to_owned()),
        Value::Array(vec![Value::Integer(2), Value::Integer(42)]),
        Value::Object(std::collections::HashMap::from([
            ("b".to_owned(), Value::Integer(2)),
            ("a".to_owned(), Value::Integer(1)),
        ])),
    ] {
        let row = Value::Object(std::collections::HashMap::from([(
            "v".to_owned(),
            value.clone(),
        )]));
        let native = zerompk::to_msgpack_vec(&row).unwrap();
        let plain = nodedb_types::value_to_msgpack(&row).unwrap();
        let crs = CrsContext::builtin_only();
        let keys = [GroupKeySpec::column("v")];
        let native_key = build_group_key(&native, &keys, &crs).unwrap();
        let plain_key = build_group_key(&plain, &keys, &crs).unwrap();
        assert_eq!(native_key, plain_key);
        assert_eq!(decode_group_key(&native_key).unwrap(), vec![value]);
        let groups = std::collections::HashSet::from([native_key, plain_key]);
        assert_eq!(groups.len(), 1);
    }
}

#[test]
fn native_group_fields_and_computed_keys_retain_types() {
    let values = vec![
        Value::Decimal("9007199254740993.125".parse().unwrap()),
        Value::Uuid("00000000-0000-0000-0000-000000000001".to_owned()),
        Value::Bytes(vec![0, 255]),
    ];
    for value in values {
        let row = Value::Object(std::collections::HashMap::from([(
            "v".to_owned(),
            value.clone(),
        )]));
        let bytes = zerompk::to_msgpack_vec(&row).unwrap();
        let crs = CrsContext::builtin_only();
        let field = [GroupKeySpec::column("v")];
        let computed = [GroupKeySpec {
            output_name: "v".to_owned(),
            field: None,
            expr: Some(SqlExpr::Column("v".to_owned())),
        }];
        let direct = build_group_key(&bytes, &field, &crs).unwrap();
        assert_eq!(build_group_key(&bytes, &computed, &crs).unwrap(), direct);
        assert_eq!(
            build_group_key_indexed(&bytes, &field, &FieldIndex::empty(), &crs).unwrap(),
            direct
        );
        assert_eq!(decode_group_key(&direct).unwrap(), vec![value]);
    }
}

#[test]
fn map_order_does_not_enter_group_identity() {
    let a = nodedb_types::json_to_msgpack(&serde_json::json!({"v": {"a": 1, "b": 2}})).unwrap();
    let b = [0x81, 0xa1, b'v', 0x82, 0xa1, b'b', 2, 0xa1, b'a', 1];
    let keys = [GroupKeySpec::column("v")];
    let crs = CrsContext::builtin_only();
    assert_eq!(
        build_group_key(&a, &keys, &crs).unwrap(),
        build_group_key(&b, &keys, &crs).unwrap()
    );
}

#[test]
fn malformed_group_bodies_return_errors() {
    let bytes = [0x92, 7, 0x81, 0xa1, b'v', 0x91, 2];
    let crs = CrsContext::builtin_only();
    assert!(build_group_key(&bytes, &[GroupKeySpec::column("v")], &crs).is_err());
    assert!(
        build_group_key_indexed(
            &bytes,
            &[GroupKeySpec::column("v")],
            &FieldIndex::empty(),
            &crs
        )
        .is_err()
    );
}

fn native_row(value: Value) -> Vec<u8> {
    zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([(
        "v".to_owned(),
        value,
    )])))
    .unwrap()
}

/// Numbers equal by exact value form one group whatever their type.
#[test]
fn equal_numbers_of_every_type_share_one_group() {
    let crs = CrsContext::builtin_only();
    let keys = [GroupKeySpec::column("v")];
    let five = |value: Value| build_group_key(&native_row(value), &keys, &crs).unwrap();
    let integer = five(Value::Integer(5));
    assert_eq!(five(Value::Decimal("5.00".parse().unwrap())), integer);
    assert_eq!(five(Value::Float(5.0)), integer);
    assert_eq!(decode_group_key(&integer).unwrap(), vec![Value::Integer(5)]);
    assert_eq!(
        five(Value::Decimal("1.50".parse().unwrap())),
        five(Value::Float(1.5))
    );
    assert_eq!(five(Value::Float(-0.0)), five(Value::Float(0.0)));
    assert_ne!(
        five(Value::Decimal("0.1".parse().unwrap())),
        five(Value::Float(0.1))
    );
    let nan = five(Value::Float(f64::NAN));
    assert_eq!(five(Value::Float(-f64::NAN)), nan);
}

/// A decoded key encodes back to the same text, so a shuffle consumer that
/// rebuilds keys from emitted group columns lands on the producer's key.
#[test]
fn decoded_keys_re_encode_identically() {
    for value in [
        Value::Float(0.1),
        Value::Float(1e-300),
        Value::Decimal("0.1".parse().unwrap()),
        Value::Decimal("18446744073709551615".parse().unwrap()),
        Value::Decimal("79228162514264337593543950335".parse().unwrap()),
        Value::Float(f64::INFINITY),
        Value::String("x".into()),
    ] {
        let key = encode_group_values(std::slice::from_ref(&value)).unwrap();
        let decoded = decode_group_key(&key).unwrap();
        assert_eq!(encode_group_values(&decoded).unwrap(), key, "{value:?}");
    }
}
