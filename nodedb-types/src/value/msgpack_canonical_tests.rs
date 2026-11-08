// SPDX-License-Identifier: Apache-2.0

//! Canonical Object order preserves tagged container and scalar identity.

use super::Value;
use crate::array_cell::ArrayCell;
use std::collections::{BTreeMap, HashMap};

fn object(reverse: bool, capacity: usize) -> Value {
    let mut map = HashMap::with_capacity(capacity);
    let mut indices: Vec<_> = (0..32).collect();
    if reverse {
        indices.reverse();
    }
    for index in indices {
        map.insert(format!("field-{index:02}"), Value::Integer(index));
    }
    Value::Object(map)
}
fn encoded(value: &Value) -> Vec<u8> {
    zerompk::to_msgpack_vec(value).unwrap()
}
fn nested(reverse: bool, capacity: usize) -> Value {
    Value::Array(vec![
        object(reverse, capacity),
        Value::Set(vec![object(reverse, capacity), Value::Integer(2)]),
        Value::Range {
            start: Some(Box::new(object(reverse, capacity))),
            end: Some(Box::new(Value::Array(vec![object(reverse, capacity)]))),
            inclusive: true,
        },
        Value::ArrayCell(ArrayCell {
            coords: vec![object(reverse, capacity)],
            attrs: vec![Value::Set(vec![object(reverse, capacity)])],
            system_time: Some(17),
        }),
    ])
}
#[test]
fn object_bytes_match_sorted_tagged_map_across_insertion_and_capacity_orders() {
    let sorted: BTreeMap<_, _> = (0..32)
        .map(|index| (format!("field-{index:02}"), Value::Integer(index)))
        .collect();
    let expected = zerompk::to_msgpack_vec(&(7u8, sorted)).unwrap();
    for capacity in [0, 32, 256] {
        for reverse in [false, true] {
            assert_eq!(encoded(&object(reverse, capacity)), expected);
        }
    }
}
#[test]
fn nested_objects_keep_canonical_bytes_through_every_value_container() {
    let expected = encoded(&nested(false, 0));
    assert_eq!(encoded(&nested(true, 256)), expected);
    for _ in 0..8 {
        let decoded: Value = zerompk::from_msgpack(&expected).unwrap();
        assert_eq!(encoded(&decoded), expected);
    }
}
#[test]
fn object_float_values_preserve_nan_payloads_and_signed_zero_bits() {
    let bits = [0, 1u64 << 63, 0x7ff8_0000_0000_0001, 0x7ff8_0000_0000_0002];
    let mut encodings = Vec::new();
    for bits in bits {
        let value = Value::Object(HashMap::from([
            ("z".into(), Value::Null),
            ("float".into(), Value::Float(f64::from_bits(bits))),
        ]));
        let bytes = encoded(&value);
        let decoded: Value = zerompk::from_msgpack(&bytes).unwrap();
        let Value::Object(map) = decoded else {
            panic!("expected Object");
        };
        let Some(Value::Float(actual)) = map.get("float") else {
            panic!("expected Float");
        };
        assert_eq!(actual.to_bits(), bits);
        assert_eq!(encoded(&Value::Object(map)), bytes);
        assert!(!encodings.contains(&bytes));
        encodings.push(bytes);
    }
}
#[test]
fn canonical_object_order_preserves_array_element_order() {
    let forward = Value::Array(vec![object(false, 0), Value::Integer(1)]);
    let reverse = Value::Array(vec![Value::Integer(1), object(true, 256)]);
    assert_ne!(encoded(&forward), encoded(&reverse));
}

#[test]
fn empty_and_singleton_objects_keep_the_tagged_map_shape_and_nested_order() {
    let empty = Value::Object(HashMap::new());
    assert_eq!(
        encoded(&empty),
        zerompk::to_msgpack_vec(&(7u8, BTreeMap::<String, Value>::new())).unwrap()
    );
    let forward = Value::Object(HashMap::from([("nested".into(), object(false, 0))]));
    let reverse = Value::Object(HashMap::from([("nested".into(), object(true, 256))]));
    assert_eq!(encoded(&forward), encoded(&reverse));
}
