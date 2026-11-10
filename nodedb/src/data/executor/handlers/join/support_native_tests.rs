// SPDX-License-Identifier: BUSL-1.1

use super::{binary_row_project, merge_join_docs_binary};
use nodedb_types::{Value, object_msgpack::decode_object};

fn native(fields: &[(&str, Value)]) -> Vec<u8> {
    let fields = fields
        .iter()
        .map(|(key, value)| ((*key).to_owned(), value.clone()))
        .collect();
    zerompk::to_msgpack_vec(&Value::Object(fields)).expect("native row")
}

#[test]
fn native_join_preserves_rich_values_through_projection_and_multiway_prefixes() {
    let decimal = Value::Decimal("123456789.123456789".parse().expect("decimal"));
    let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
    let bytes = Value::Bytes(vec![0, 255]);
    let left = native(&[("amount", decimal.clone()), ("key", Value::Integer(42))]);
    let right = native(&[("uuid", uuid.clone()), ("bytes", bytes.clone())]);
    let merged = merge_join_docs_binary(&left, Some(&right), "a", "b").expect("merge");
    let third = native(&[("key", Value::Integer(43))]);
    let merged = merge_join_docs_binary(&merged, Some(&third), "a", "c").expect("multiway");
    let projection = ["a.amount", "b.uuid", "b.bytes", "a.key", "c.key"].map(|name| {
        nodedb_physical::physical_plan::JoinProjection {
            source: name.into(),
            output: name.into(),
        }
    });
    let projected = binary_row_project(&merged, &projection).expect("projection");
    let doc = decode_object(&projected).expect("typed output");
    assert_eq!(doc.get("a.amount"), Some(&decimal));
    assert_eq!(doc.get("b.uuid"), Some(&uuid));
    assert_eq!(doc.get("b.bytes"), Some(&bytes));
    assert_eq!(doc.get("a.key"), Some(&Value::Integer(42)));
    assert_eq!(doc.get("c.key"), Some(&Value::Integer(43)));
    assert!(doc.get("a.a.amount").is_none());
}

#[test]
fn mixed_join_keeps_plain_arrays_as_arrays() {
    let plain = nodedb_types::json_to_msgpack(&serde_json::json!({"array": [4, "text"]}))
        .expect("plain row");
    let merged = merge_join_docs_binary(
        &plain,
        Some(&native(&[("key", Value::Integer(3))])),
        "a",
        "b",
    )
    .expect("merge");
    let doc = decode_object(&merged).expect("typed output");
    assert_eq!(
        doc.get("a.array"),
        Some(&Value::Array(vec![
            Value::Integer(4),
            Value::String("text".into())
        ]))
    );
}

#[test]
fn malformed_join_objects_and_nonstring_keys_return_errors() {
    for bytes in [
        &[0x92, 7, 0x81, 0xa1, b'x', 0x92, 255, 0xc0][..],
        &[0x81, 1, 2][..],
    ] {
        assert!(merge_join_docs_binary(bytes, None, "a", "").is_err());
        assert!(binary_row_project(bytes, &[]).is_err());
    }
}
