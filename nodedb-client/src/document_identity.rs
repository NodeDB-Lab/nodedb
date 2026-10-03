// SPDX-License-Identifier: Apache-2.0

//! The identity column of a stored schemaless document.
//!
//! A schemaless row stores its document id under `id`, because SQL reads a
//! row's identity from that field. The server writes it on every native put.
//! A pgwire put writes it as the key column. A read returns it as
//! `Document::id`, never as one of `Document::fields`.

use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

/// Whether a stored cell is the document's own identity column.
///
/// An `id` cell that holds any other value is a field the writer set, and
/// stays a field.
pub(crate) fn is_identity_cell(name: &str, value: &Value, document_id: &str) -> bool {
    name == DEFAULT_IDENTITY_COLUMN && matches!(value, Value::String(s) if s == document_id)
}

/// Refuse a document whose `id` field names a different document.
///
/// The stored `id` cell is the row's identity for SQL. A field `id` that
/// differs from `Document::id` makes SQL and key reads disagree on which
/// document the row is.
pub(crate) fn check_identity_field(
    collection: &str,
    document_id: &str,
    fields: &std::collections::HashMap<String, Value>,
) -> NodeDbResult<()> {
    match fields.get(DEFAULT_IDENTITY_COLUMN) {
        None => Ok(()),
        Some(value) if is_identity_cell(DEFAULT_IDENTITY_COLUMN, value, document_id) => Ok(()),
        Some(other) => Err(NodeDbError::bad_request(format!(
            "document_put '{collection}'/'{document_id}': field 'id' holds {other:?}, \
             which differs from the document id; remove the field or set it to the document id"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn only_the_matching_id_cell_is_the_identity() {
        assert!(is_identity_cell("id", &Value::String("d1".into()), "d1"));
        assert!(!is_identity_cell(
            "id",
            &Value::String("other".into()),
            "d1"
        ));
        assert!(!is_identity_cell("id", &Value::Integer(1), "d1"));
        assert!(!is_identity_cell("body", &Value::String("d1".into()), "d1"));
    }

    #[test]
    fn an_id_field_naming_another_document_is_refused() {
        let mut fields = HashMap::new();
        assert!(check_identity_field("c", "d1", &fields).is_ok());
        fields.insert("id".to_string(), Value::String("d1".into()));
        assert!(check_identity_field("c", "d1", &fields).is_ok());
        fields.insert("id".to_string(), Value::String("d2".into()));
        let err = check_identity_field("c", "d1", &fields).expect_err("conflicting id");
        assert!(err.to_string().contains("d1"));
    }
}
