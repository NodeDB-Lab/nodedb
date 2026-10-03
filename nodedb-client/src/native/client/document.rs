// SPDX-License-Identifier: Apache-2.0

//! Document operation implementations for `NativeClient`.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use super::core::NativeClient;
use crate::document_identity::{check_identity_field, is_identity_cell};
use crate::native::connection::check_error;

impl NativeClient {
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::PointGet,
                TextFields {
                    collection: Some(collection.to_string()),
                    document_id: Some(id.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        point_get_response_to_document(collection, id, resp)
    }

    pub(super) async fn document_put_impl(
        &self,
        collection: &str,
        doc: Document,
    ) -> NodeDbResult<()> {
        check_identity_field(collection, &doc.id, &doc.fields)?;
        let data = sonic_rs::to_vec(&doc.fields)
            .map_err(|e| NodeDbError::serialization("json", format!("doc serialize: {e}")))?;
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::PointPut,
                TextFields {
                    collection: Some(collection.to_string()),
                    document_id: Some(doc.id.clone()),
                    data: Some(data),
                    ..Default::default()
                },
            )
            .await?;
        check_error(&resp)
    }

    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::PointDelete,
                TextFields {
                    collection: Some(collection.to_string()),
                    document_id: Some(id.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        check_error(&resp)
    }
}

/// Interpret a `PointGet` response frame as "the document" or "no such
/// document".
///
/// The server answers a point get in exactly one of two shapes:
///
/// * **hit** — one row, with `columns` naming the stored document's fields
///   and the row holding one already-typed cell per column, in the same
///   order. Field structure is preserved (nested objects and arrays arrive
///   as `Value::Object` / `Value::Array`), so the document is rebuilt by
///   zipping the two lists; there is no JSON text to re-parse.
/// * **miss** — no rows at all. The Data Plane replies with an empty payload
///   for an absent row, and an empty payload shapes into a frame with no
///   rows, so absence is carried by the frame's structure rather than by any
///   sentinel cell value a real document could also hold.
///
/// A frame matching neither shape is a protocol fault and is reported as
/// one, naming the collection, the id, and what actually arrived. Returning
/// a field-less `Document` there would hand the caller data the server never
/// sent, indistinguishable from a genuinely empty stored document.
fn point_get_response_to_document(
    collection: &str,
    id: &str,
    resp: NativeResponse,
) -> NodeDbResult<Option<Document>> {
    // An error frame arrives as a successful `send`. Left unchecked it has
    // no rows and would be read as "document absent".
    check_error(&resp)?;

    let columns = resp.columns.unwrap_or_default();
    let mut rows = resp.rows.unwrap_or_default().into_iter();

    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let extra = rows.count();
    if extra > 0 {
        return Err(malformed(
            collection,
            id,
            format!("expected at most one row, got {}", extra + 1),
        ));
    }
    if columns.len() != row.len() {
        return Err(malformed(
            collection,
            id,
            format!(
                "row has {} cells but the frame names {} columns",
                row.len(),
                columns.len()
            ),
        ));
    }

    let mut doc = Document::new(id);
    for (name, value) in columns.into_iter().zip(row) {
        if !is_identity_cell(&name, &value, id) {
            doc.set(name, value);
        }
    }
    Ok(Some(doc))
}

fn malformed(collection: &str, id: &str, detail: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization(
        "native",
        format!("document_get response for '{collection}'/'{id}': {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::ResponseStatus;
    use nodedb_types::value::Value;

    fn hit(columns: &[&str], cells: Vec<Value>) -> NativeResponse {
        let mut resp = NativeResponse::ok(1);
        resp.columns = Some(columns.iter().map(|c| (*c).to_string()).collect());
        resp.rows = Some(vec![cells]);
        resp
    }

    #[test]
    fn hit_rebuilds_every_field_with_its_wire_type() {
        let resp = hit(
            &["name", "age", "active"],
            vec![
                Value::String("alice".into()),
                Value::Integer(30),
                Value::Bool(true),
            ],
        );

        let doc = point_get_response_to_document("users", "u-1", resp)
            .expect("a well-formed hit must parse")
            .expect("a hit must yield a document");

        assert_eq!(doc.id, "u-1");
        assert_eq!(doc.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(doc.get("age"), Some(&Value::Integer(30)));
        assert_eq!(doc.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn the_stored_identity_cell_is_the_document_id_not_a_field() {
        let resp = hit(
            &["id", "name"],
            vec![Value::String("u-1".into()), Value::String("alice".into())],
        );
        let doc = point_get_response_to_document("users", "u-1", resp)
            .expect("a well-formed hit must parse")
            .expect("a hit must yield a document");
        assert_eq!(doc.id, "u-1");
        assert_eq!(doc.fields.len(), 1);
        assert_eq!(doc.get("name"), Some(&Value::String("alice".into())));
    }

    #[test]
    fn hit_preserves_nested_structure() {
        let mut inner = std::collections::HashMap::new();
        inner.insert("city".to_string(), Value::String("KL".into()));
        let resp = hit(&["address"], vec![Value::Object(inner)]);

        let doc = point_get_response_to_document("users", "u-1", resp)
            .expect("a well-formed hit must parse")
            .expect("a hit must yield a document");

        match doc.get("address") {
            Some(Value::Object(obj)) => {
                assert_eq!(obj.get("city"), Some(&Value::String("KL".into())));
            }
            other => panic!("nested object must survive the wire, got {other:?}"),
        }
    }

    #[test]
    fn miss_yields_none_not_an_error() {
        // The absent-row frame: OK status, no rows, no columns.
        let resp = NativeResponse::ok(1);
        assert_eq!(
            point_get_response_to_document("users", "missing", resp)
                .expect("a miss is not a fault"),
            None
        );
    }

    #[test]
    fn empty_row_list_yields_none() {
        let mut resp = NativeResponse::ok(1);
        resp.columns = Some(Vec::new());
        resp.rows = Some(Vec::new());
        assert_eq!(
            point_get_response_to_document("users", "missing", resp)
                .expect("a miss is not a fault"),
            None
        );
    }

    #[test]
    fn error_frame_is_an_error_not_a_miss() {
        let resp = NativeResponse::error(1, "42P01", "collection 'users' not found");
        assert_eq!(resp.status, ResponseStatus::Error);
        let err = point_get_response_to_document("users", "u-1", resp)
            .expect_err("a server error must never be reported as an absent document");
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn cell_count_mismatch_errors_instead_of_inventing_a_document() {
        // One named column, two cells: the frame cannot be mapped to fields.
        let resp = hit(&["name"], vec![Value::Integer(0), Value::Integer(1)]);
        let err = point_get_response_to_document("users", "u-1", resp)
            .expect_err("a shape the client cannot map must be an error");
        let msg = err.to_string();
        assert!(
            msg.contains("users"),
            "error must name the collection: {msg}"
        );
        assert!(msg.contains("u-1"), "error must name the id: {msg}");
        assert!(msg.contains("cells"), "error must say what arrived: {msg}");
    }

    #[test]
    fn multi_row_frame_errors_instead_of_taking_the_first() {
        let mut resp = hit(&["name"], vec![Value::String("alice".into())]);
        resp.rows = Some(vec![
            vec![Value::String("alice".into())],
            vec![Value::String("bob".into())],
        ]);
        let err = point_get_response_to_document("users", "u-1", resp)
            .expect_err("a point get must never silently pick one of several rows");
        assert!(err.to_string().contains("one row"));
    }

    #[test]
    fn document_with_no_fields_is_a_hit_not_a_miss() {
        // A stored document that has no fields arrives as one row with zero
        // cells — still a row, so it must not collapse into `Ok(None)`.
        let resp = hit(&[], Vec::new());
        let doc = point_get_response_to_document("users", "u-1", resp)
            .expect("an empty document is a well-formed hit")
            .expect("a row present means the document exists");
        assert_eq!(doc.id, "u-1");
        assert!(doc.fields.is_empty());
    }
}
