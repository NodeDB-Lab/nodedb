// SPDX-License-Identifier: BUSL-1.1

//! Construction of the `Value` document a DML `RETURNING` projection reads.
//!
//! Every `RETURNING` path — point and bulk UPDATE/DELETE, `UPDATE ... FROM`,
//! MERGE, CRDT document DML — builds its rows from a stored row body, and two
//! invariants are shared by all of them:
//!
//! - **The body must be decoded with the collection's storage mode.** A strict
//!   collection stores Binary Tuples. The MessagePack decoder does not reject
//!   one: it reads the tuple's leading byte as a scalar and *succeeds*, yielding
//!   a document with every real column missing. A storage-mode-blind decode
//!   therefore ships plausible-looking garbage to the client instead of failing.
//! - **The identity lands under the collection's identity column, and only
//!   when the body lacks that column.** The identity column is the declared
//!   key, else `id`, per `CoreLoop::identity_column`. A body that holds its key
//!   is authoritative for it, and a declared-key row never gains an `id`
//!   beside its key.

use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

use crate::data::executor::doc_format;
use crate::data::executor::strict_format;
use crate::engine::document::store::RowIdentity;

/// Set `identity_column` to the row's client-visible identity unless the
/// document already carries that column.
///
/// For callers that already hold the decoded document (the update paths
/// re-project the image they just built rather than re-reading storage).
pub(in crate::data::executor) fn attach_row_id(
    doc: &mut Value,
    identity: &RowIdentity,
    identity_column: &str,
) {
    if let Value::Object(obj) = doc
        && !obj.contains_key(identity_column)
    {
        obj.insert(
            identity_column.to_string(),
            Value::String(identity.as_str().to_string()),
        );
    }
}

/// Decode a STORED row body — a pre-image or a re-encoded post-image — into the
/// typed document a `RETURNING` projection reads and the write gate judges.
///
/// `strict_schema` is `Some` exactly when the collection stores Binary Tuples;
/// passing `None` for a strict collection is the silent-misdecode failure this
/// module exists to prevent.
///
/// Fails when the bytes do not decode under the resolved mode. A `RETURNING`
/// row that quietly disappears from the response is indistinguishable from a
/// statement that matched fewer rows, so the client would read a wrong
/// row count as the truth.
pub(in crate::data::executor) fn from_stored(
    body: &[u8],
    identity: &RowIdentity,
    strict_schema: Option<&StrictSchema>,
    identity_column: &str,
) -> crate::Result<Value> {
    let mut doc = match strict_schema {
        Some(schema) => strict_format::binary_tuple_to_row_value(body, schema)
            .ok_or_else(|| undecodable(identity, body.len()))?,
        None => doc_format::decode_document_value(body)?,
    };
    if !matches!(doc, Value::Object(_)) {
        return Err(crate::Error::Serialization {
            format: "document".into(),
            detail: format!("RETURNING row {identity}: stored body is not a document object"),
        });
    }
    attach_row_id(&mut doc, identity, identity_column);
    Ok(doc)
}

/// [`from_stored`] for the JSON document the update pipeline patches and the
/// secondary-index diff reads.
///
/// The index diff compares this image's rendered field values against the
/// entries the forward write path derived from the same JSON decoders, so a
/// pre-image must come from those decoders too — a binary field rendered by
/// a different converter would leave its index entry behind.
pub(in crate::data::executor) fn from_stored_json(
    body: &[u8],
    identity: &RowIdentity,
    strict_schema: Option<&StrictSchema>,
    identity_column: &str,
) -> crate::Result<serde_json::Value> {
    from_stored(body, identity, strict_schema, identity_column).map(serde_json::Value::from)
}

fn undecodable(identity: &RowIdentity, body_len: usize) -> crate::Error {
    crate::Error::Serialization {
        format: "binary_tuple".to_string(),
        detail: format!(
            "RETURNING row {identity}: stored body ({body_len} bytes) is not a Binary Tuple \
             readable under the collection's strict schema"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_returning_attaches_identity_without_losing_source_fields() {
        let decimal = Value::Decimal("123456789.123456789".parse().expect("decimal"));
        let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let value = Value::Object(
            [
                ("decimal".into(), decimal.clone()),
                ("uuid".into(), uuid.clone()),
                (
                    "nested".into(),
                    Value::Array(vec![Value::Bytes(vec![0, 255])]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let bytes = zerompk::to_msgpack_vec(&value).expect("native body");
        let identity = RowIdentity::from_user_key("declared-key");
        let row = from_stored(&bytes, &identity, None, "sku").expect("returning native");
        let fields = row.as_object().expect("object");
        assert_eq!(
            fields.get("sku"),
            Some(&Value::String("declared-key".into()))
        );
        assert!(!fields.contains_key("id"));
        for (name, value) in value.as_object().expect("source") {
            assert_eq!(fields.get(name), Some(value));
        }
        let json = from_stored_json(&bytes, &identity, None, "sku").expect("returning public");
        assert!(json.is_object());
        assert_eq!(json["sku"], "declared-key");
        assert_eq!(json["uuid"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn native_returning_preserves_the_stored_declared_key() {
        let value = Value::Object(
            [("sku".into(), Value::String("stored-key".into()))]
                .into_iter()
                .collect(),
        );
        let bytes = zerompk::to_msgpack_vec(&value).expect("native body");
        let identity = RowIdentity::from_user_key("surrogate");
        assert_eq!(
            from_stored(&bytes, &identity, None, "sku").expect("native returning"),
            value
        );
    }
}
