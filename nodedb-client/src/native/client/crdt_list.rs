// SPDX-License-Identifier: Apache-2.0

//! CRDT movable-list operation implementations for `NativeClient`.
//!
//! Encodes `OpCode::CrdtListInsert` / `CrdtListDelete` / `CrdtListMove`
//! directly over the native wire — no SQL/DSL text is built for these ops,
//! since Origin's SQL grammar has no movable-list syntax; only the native
//! opcode path exists.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{OpCode, TextFields};
use nodedb_types::value::Value;

use super::core::NativeClient;
use crate::native::connection::check_error;

/// Narrow a `usize` index to the wire's `u64` representation.
///
/// A silent truncation here would land a CRDT list op at the wrong
/// position — data corruption, not a benign fallback — so the conversion
/// is checked and surfaces a typed error instead.
fn index_to_wire(index: usize, field_name: &str) -> NodeDbResult<u64> {
    u64::try_from(index).map_err(|_| {
        NodeDbError::bad_request(format!("{field_name} value {index} exceeds wire u64 range"))
    })
}

/// Extract the field map from `fields`, rejecting anything that is not a
/// `Value::Object`.
///
/// Each top-level key of the object becomes a field of the new list block.
/// A scalar or array has no keys, so it is refused here.
fn require_object_fields(fields: &Value) -> NodeDbResult<&HashMap<String, Value>> {
    match fields {
        Value::Object(obj) => Ok(obj),
        other => Err(NodeDbError::bad_request(format!(
            "list_insert: fields must be Value::Object, got {other:?}"
        ))),
    }
}

/// Build the `TextFields` payload for an `OpCode::CrdtListInsert` request.
///
/// The block fields travel as a tagged `Value::Object`, so a Decimal, a
/// UUID, an instant or bytes reaches the block with its exact type.
pub(super) fn build_list_insert_fields(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields: &Value,
) -> NodeDbResult<TextFields> {
    require_object_fields(fields)?;
    let list_index = index_to_wire(index, "list_index")?;

    Ok(TextFields {
        collection: Some(collection.to_string()),
        document_id: Some(document_id.to_string()),
        list_path: Some(list_path.to_string()),
        list_index: Some(list_index),
        list_fields: Some(fields.clone()),
        ..Default::default()
    })
}

/// Build the `TextFields` payload for an `OpCode::CrdtListDelete` request.
pub(super) fn build_list_delete_fields(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
) -> NodeDbResult<TextFields> {
    let list_index = index_to_wire(index, "list_index")?;
    Ok(TextFields {
        collection: Some(collection.to_string()),
        document_id: Some(document_id.to_string()),
        list_path: Some(list_path.to_string()),
        list_index: Some(list_index),
        ..Default::default()
    })
}

/// Build the `TextFields` payload for an `OpCode::CrdtListMove` request.
///
/// `from_index` and `to_index` are encoded into distinct wire fields
/// (`list_from_index` / `list_to_index`) — never conflated.
pub(super) fn build_list_move_fields(
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> NodeDbResult<TextFields> {
    let list_from_index = index_to_wire(from_index, "list_from_index")?;
    let list_to_index = index_to_wire(to_index, "list_to_index")?;
    Ok(TextFields {
        collection: Some(collection.to_string()),
        document_id: Some(document_id.to_string()),
        list_path: Some(list_path.to_string()),
        list_from_index: Some(list_from_index),
        list_to_index: Some(list_to_index),
        ..Default::default()
    })
}

impl NativeClient {
    pub(super) async fn list_insert_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()> {
        let request = build_list_insert_fields(collection, document_id, list_path, index, fields)?;
        let mut conn = self.pool.acquire().await?;
        let resp = conn.send(OpCode::CrdtListInsert, request).await?;
        check_error(&resp)
    }

    pub(super) async fn list_delete_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()> {
        let request = build_list_delete_fields(collection, document_id, list_path, index)?;
        let mut conn = self.pool.acquire().await?;
        let resp = conn.send(OpCode::CrdtListDelete, request).await?;
        check_error(&resp)
    }

    pub(super) async fn list_move_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()> {
        let request =
            build_list_move_fields(collection, document_id, list_path, from_index, to_index)?;
        let mut conn = self.pool.acquire().await?;
        let resp = conn.send(OpCode::CrdtListMove, request).await?;
        check_error(&resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj_fields() -> Value {
        let mut map = HashMap::new();
        map.insert("title".to_string(), Value::String("hello".to_string()));
        map.insert("done".to_string(), Value::Bool(false));
        map.insert(
            "price".to_string(),
            Value::Decimal("12.50".parse().expect("decimal literal")),
        );
        map.insert(
            "uid".to_string(),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        );
        map.insert("blob".to_string(), Value::Bytes(vec![0x01, 0xff]));
        Value::Object(map)
    }

    #[test]
    fn list_insert_fields_carries_every_field() {
        let fields = obj_fields();
        let req = build_list_insert_fields("docs", "doc-1", "items", 3, &fields)
            .expect("object fields must build");
        assert_eq!(req.collection.as_deref(), Some("docs"));
        assert_eq!(req.document_id.as_deref(), Some("doc-1"));
        assert_eq!(req.list_path.as_deref(), Some("items"));
        assert_eq!(req.list_index, Some(3));
        assert_eq!(
            req.list_fields,
            Some(fields),
            "every block field travels with its exact type"
        );
        // Move/delete-only fields must stay unset on an insert request.
        assert!(req.list_from_index.is_none());
        assert!(req.list_to_index.is_none());
    }

    #[test]
    fn list_insert_rejects_non_object_fields() {
        let err = build_list_insert_fields("docs", "doc-1", "items", 0, &Value::Integer(1))
            .expect_err("scalar fields must be rejected");
        assert!(format!("{err}").to_lowercase().contains("object"));
    }

    #[test]
    fn list_delete_fields_carries_index_without_block_fields() {
        let req = build_list_delete_fields("docs", "doc-2", "notes", 7)
            .expect("delete request must build");
        assert_eq!(req.collection.as_deref(), Some("docs"));
        assert_eq!(req.document_id.as_deref(), Some("doc-2"));
        assert_eq!(req.list_path.as_deref(), Some("notes"));
        assert_eq!(req.list_index, Some(7));
        assert!(req.list_fields.is_none());
    }

    #[test]
    fn list_move_keeps_from_and_to_index_distinct_and_unswapped() {
        let req = build_list_move_fields("docs", "doc-1", "items", 1, 5)
            .expect("move request must build");
        assert_eq!(req.list_from_index, Some(1));
        assert_eq!(req.list_to_index, Some(5));
        assert_ne!(req.list_from_index, req.list_to_index);
        // Field-swap regression guard: constructing with reversed
        // arguments must produce the reversed wire values, proving the
        // builder does not accidentally always fill the same field.
        let swapped = build_list_move_fields("docs", "doc-1", "items", 5, 1)
            .expect("move request must build");
        assert_eq!(swapped.list_from_index, Some(5));
        assert_eq!(swapped.list_to_index, Some(1));
    }
}
