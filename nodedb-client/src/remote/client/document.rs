// SPDX-License-Identifier: Apache-2.0

//! Document operation implementations for `NodeDbRemote`.
//!
//! A document's fields are the row's top-level columns, the shape the native
//! client and Lite store. The collection's identity column holds the
//! document id.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;
use tokio_postgres::types::Type;

use crate::document_identity::is_identity_cell;
use crate::pg_cell::decode_text_cell;
use crate::remote_parse::json_to_value;
use crate::sql_escape::{quote_identifier, quote_string_literal};

use super::core::NodeDbRemote;
use super::identity::{declared_types, identity_column_of};

/// Output column holding the whole row as one JSON object.
const DOCUMENT_COLUMN: &str = "document";

impl NodeDbRemote {
    /// Read every top-level field of the row whose identity column holds
    /// `id`, each with its stored type.
    ///
    /// `to_jsonb(*)` returns the whole row as one JSON object. Each field
    /// decodes to the `Value` kind the native client returns. A key lookup
    /// stays a point read: the server evaluates the item on the one row the
    /// lookup returns.
    ///
    /// JSON has no DECIMAL, UUID, timestamp or bytes: the server renders
    /// each as a string of its exact text, bytes as base64. The collection's `DESCRIBE`, the same read
    /// that names the key, declares which fields hold those types, so each
    /// declared field decodes from its text to its typed value. An
    /// undeclared field of a schemaless document carries no type, and a
    /// string there stays a string.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        let (describe_columns, describe_rows) = self.describe(collection).await?;
        let key = identity_column_of(collection, &describe_columns, &describe_rows)?;
        let declared = declared_types(&describe_columns, &describe_rows);
        let sql = format!(
            "SELECT to_jsonb(*) AS {DOCUMENT_COLUMN} FROM {} WHERE {} = {}",
            quote_identifier(collection),
            quote_identifier(&key),
            quote_string_literal(id)
        );
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        document_from_rows(collection, id, &columns, rows, &declared)
    }

    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let key = self.identity_column(collection).await?;
        let sql = format!(
            "DELETE FROM {} WHERE {} = $1",
            quote_identifier(collection),
            quote_identifier(&key)
        );
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}

/// The document a `to_jsonb(*)` point read answered with, or `None` when no
/// row matched.
///
/// The one cell is the JSON text of the row object. More than one row, a
/// missing column, or a cell that is not a JSON object is an error naming
/// the document. A field `declared` names, read as a JSON string, decodes
/// from its text by its declared type. Text that does not decode as that
/// type is an error naming the field, never a string.
fn document_from_rows(
    collection: &str,
    id: &str,
    columns: &[String],
    rows: Vec<Vec<Value>>,
    declared: &[(String, Type)],
) -> NodeDbResult<Option<Document>> {
    let mut rows = rows.into_iter();
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
    let index = columns
        .iter()
        .position(|c| c == DOCUMENT_COLUMN)
        .ok_or_else(|| {
            malformed(
                collection,
                id,
                format!("no '{DOCUMENT_COLUMN}' column; columns are {columns:?}"),
            )
        })?;
    let text = match row.into_iter().nth(index) {
        Some(Value::String(text)) => text,
        other => {
            return Err(malformed(
                collection,
                id,
                format!("'{DOCUMENT_COLUMN}' cell is not JSON text: {other:?}"),
            ));
        }
    };
    let json: serde_json::Value = sonic_rs::from_str(&text)
        .map_err(|e| malformed(collection, id, format!("row JSON does not parse: {e}")))?;
    let serde_json::Value::Object(fields) = json else {
        return Err(malformed(
            collection,
            id,
            format!("row JSON is not an object: {text}"),
        ));
    };

    let mut doc = Document::new(id);
    for (name, field) in &fields {
        let declared_type = declared
            .iter()
            .find(|(declared_name, _)| declared_name == name)
            .map(|(_, ty)| ty);
        let value = match (declared_type, field) {
            // JSON renders bytes as unpadded base64 text.
            (Some(&Type::BYTEA), serde_json::Value::String(text)) => decode_base64(name, text),
            (Some(ty), serde_json::Value::String(text)) => decode_text_cell(name, ty, text),
            _ => json_to_value(field),
        }
        .map_err(|e| malformed(collection, id, format!("field '{name}': {e}")))?;
        if !is_identity_cell(name, &value, id) {
            doc.set(name.clone(), value);
        }
    }
    Ok(Some(doc))
}

/// The bytes of a declared `BYTEA` field, from the unpadded base64 text JSON
/// renders them as.
fn decode_base64(field: &str, text: &str) -> NodeDbResult<Value> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(text.trim_end_matches('='))
        .map(Value::Bytes)
        .map_err(|e| {
            NodeDbError::serialization(
                "json",
                format!("field '{field}' of type bytea: cannot decode {text:?} as base64: {e}"),
            )
        })
}

fn malformed(collection: &str, id: &str, detail: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization(
        "pgwire",
        format!("document_get '{collection}'/'{id}': {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::super::identity::DeclaredTypes;
    use super::*;

    fn read(cells: Vec<Vec<Value>>) -> NodeDbResult<Option<Document>> {
        document_from_rows("docs", "d1", &[DOCUMENT_COLUMN.to_string()], cells, &[])
    }

    /// A `DESCRIBE` result of `fields`, each `(name, declared type)`.
    fn describe(fields: &[(&str, &str)]) -> DeclaredTypes {
        let columns = vec![
            "field".to_string(),
            "type".to_string(),
            "nullable".to_string(),
            "primary_key".to_string(),
        ];
        let rows: Vec<Vec<Value>> = fields
            .iter()
            .map(|(name, ty)| {
                vec![
                    Value::String((*name).into()),
                    Value::String((*ty).into()),
                    Value::String("true".into()),
                    Value::String("f".into()),
                ]
            })
            .collect();
        declared_types(&columns, &rows)
    }

    /// A declared DECIMAL, UUID, timestamp or BYTEA field arrives as the
    /// JSON string of its exact text (bytes as base64) and decodes to its
    /// typed value, scale included. An undeclared string stays a string.
    #[test]
    fn declared_fields_decode_from_their_exact_text() {
        let declared = describe(&[
            ("id", "TEXT"),
            ("amount", "DECIMAL(10,2) NOT NULL"),
            ("big", "NUMERIC"),
            ("uid", "UUID"),
            ("seen", "TIMESTAMPTZ"),
            ("at", "TIMESTAMP"),
            ("count", "BIGINT"),
            ("blob", "BYTEA"),
        ]);
        let cells = json_cell(
            r#"{"id":"d1","amount":"1.50","big":"12345678901234567.89",
                "uid":"550e8400-e29b-41d4-a716-446655440000",
                "seen":"2023-11-14T22:13:20.000000Z","at":"2023-11-14T22:13:20.000000Z",
                "count":7,"note":"1.50","blob":"AQL/"}"#,
        );
        let doc = document_from_rows(
            "docs",
            "d1",
            &[DOCUMENT_COLUMN.to_string()],
            cells,
            &declared,
        )
        .expect("a well-formed row parses")
        .expect("a row is a document");
        let amount = match doc.get("amount") {
            Some(Value::Decimal(d)) => *d,
            other => panic!("amount is a Decimal, got {other:?}"),
        };
        assert_eq!(amount.to_string(), "1.50");
        assert_eq!(
            doc.get("big"),
            Some(&Value::Decimal("12345678901234567.89".parse().unwrap()))
        );
        assert_eq!(
            doc.get("uid"),
            Some(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))
        );
        assert!(matches!(doc.get("seen"), Some(Value::DateTime(_))));
        assert!(matches!(doc.get("at"), Some(Value::NaiveDateTime(_))));
        assert_eq!(doc.get("count"), Some(&Value::Integer(7)));
        assert_eq!(doc.get("note"), Some(&Value::String("1.50".into())));
        assert_eq!(doc.get("blob"), Some(&Value::Bytes(vec![0x01, 0x02, 0xff])));
    }

    /// A declared field whose text is not its type is an error naming the
    /// field, never a string.
    #[test]
    fn a_declared_field_with_bad_text_is_an_error() {
        let declared = describe(&[("amount", "DECIMAL")]);
        let err = document_from_rows(
            "docs",
            "d1",
            &[DOCUMENT_COLUMN.to_string()],
            json_cell(r#"{"id":"d1","amount":"one fifty"}"#),
            &declared,
        )
        .expect_err("bad DECIMAL text is refused");
        assert!(err.to_string().contains("amount"), "{err}");
    }

    fn json_cell(text: &str) -> Vec<Vec<Value>> {
        vec![vec![Value::String(text.into())]]
    }

    #[test]
    fn every_field_keeps_its_type() {
        let doc = read(json_cell(
            r#"{"id":"d1","count":5,"ratio":1.5,"whole":2.0,"active":true,"name":"n",
                "tags":["a",2],"missing":null,"meta":{"k":"v"}}"#,
        ))
        .expect("a well-formed row parses")
        .expect("a row is a document");
        assert_eq!(doc.id, "d1");
        assert_eq!(doc.get("count"), Some(&Value::Integer(5)));
        assert_eq!(doc.get("ratio"), Some(&Value::Float(1.5)));
        assert_eq!(doc.get("whole"), Some(&Value::Float(2.0)));
        assert_eq!(doc.get("active"), Some(&Value::Bool(true)));
        assert_eq!(doc.get("name"), Some(&Value::String("n".into())));
        assert_eq!(
            doc.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Integer(2)
            ]))
        );
        assert_eq!(doc.get("missing"), Some(&Value::Null));
        assert!(matches!(doc.get("meta"), Some(Value::Object(_))));
        assert_eq!(doc.get("id"), None, "the identity cell is not a field");
    }

    #[test]
    fn no_row_is_no_document() {
        assert_eq!(read(Vec::new()).expect("a miss is not a fault"), None);
    }

    #[test]
    fn a_cell_that_is_not_a_json_object_is_an_error() {
        let err = read(json_cell("[1,2]")).expect_err("an array is not a row");
        assert!(err.to_string().contains("not an object"), "{err}");
        let err = read(vec![vec![Value::Null]]).expect_err("a NULL cell is not a row");
        assert!(err.to_string().contains("not JSON text"), "{err}");
        let err = read(json_cell("{")).expect_err("broken JSON");
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn several_rows_are_an_error() {
        let err = read(vec![
            vec![Value::String("{}".into())],
            vec![Value::String("{}".into())],
        ])
        .expect_err("a point read never picks one of several rows");
        assert!(err.to_string().contains("one row"), "{err}");
    }
}
