// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn native_object_retains_field_types() {
    let value = Value::Object(std::collections::HashMap::from([
        (
            "decimal".to_owned(),
            Value::Decimal(rust_decimal::Decimal::new(12345, 3)),
        ),
        (
            "uuid".to_owned(),
            Value::Uuid("00000000-0000-0000-0000-000000000001".to_owned()),
        ),
        ("bytes".to_owned(), Value::Bytes(vec![0, 255])),
    ]));
    let bytes = zerompk::to_msgpack_vec(&value).unwrap();
    let view = ObjectView::new(&bytes).unwrap();
    assert_eq!(view.format(), ObjectFormat::Native);
    assert_eq!(view.decode().unwrap(), value);
    for entry in view.fields() {
        let (key, bytes) = entry.unwrap();
        assert_eq!(
            Some(view.decode_value(bytes).unwrap()),
            value.get(key).cloned()
        );
    }
}

#[test]
fn plain_field_arrays_keep_array_meaning() {
    let bytes =
        crate::json_to_msgpack(&serde_json::json!({"v": [2, 42], "object": [7, {}]})).unwrap();
    let view = ObjectView::new(&bytes).unwrap();
    assert_eq!(view.format(), ObjectFormat::Plain);
    assert_eq!(
        view.get("v").unwrap(),
        Some(Value::Array(vec![Value::Integer(2), Value::Integer(42)]))
    );
    assert!(matches!(view.get("object").unwrap(), Some(Value::Array(_))));
    assert!(ObjectView::new(&[0x92, 2, 42]).is_err());
}

#[test]
fn malformed_native_payloads_return_errors() {
    for bytes in [
        vec![0x92, 7, 0x81, 0xa1, b'v', 0x91, 2],
        vec![0x92, 7, 0x81, 0xa1, b'v', 0x93, 2, 42, 0],
        vec![0x92, 7, 0x81, 0xa1, b'v', 0x92, 2, 0xc3],
        vec![0x92, 7, 0x81, 0xa1, b'v', 0x92, 6, 0x91, 0x91, 2],
        vec![0x92, 7, 0x80, 0xc0],
        vec![0x92, 7, 0x81, 0, 0x91, 0],
    ] {
        assert!(ObjectView::new(&bytes).is_err(), "{bytes:?}");
    }
}

#[test]
fn every_truncated_native_body_returns_an_error() {
    let value = Value::Object(std::collections::HashMap::from([(
        "v".to_owned(),
        Value::Array(vec![Value::Integer(42)]),
    )]));
    let bytes = zerompk::to_msgpack_vec(&value).unwrap();
    for end in 0..bytes.len() {
        assert!(ObjectView::new(&bytes[..end]).is_err());
    }
}
