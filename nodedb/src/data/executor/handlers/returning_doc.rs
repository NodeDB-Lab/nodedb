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
//! - **The storage key becomes `id` only when the body carries no `id`.** A
//!   collection with a declared `id` primary key is authoritative for its own
//!   key; overwriting it with the surrogate hex storage key returns a value the
//!   client never wrote and cannot use to address the row.

use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

use crate::data::executor::doc_format;
use crate::data::executor::strict_format;
use crate::engine::document::store::RowIdentity;

/// Set `id` to the row's client-visible identity unless the document already
/// carries one.
///
/// For callers that already hold the decoded document (the update paths
/// re-project the image they just built rather than re-reading storage).
pub(in crate::data::executor) fn attach_row_id(doc: &mut Value, identity: &RowIdentity) {
    if let Value::Object(obj) = doc
        && !obj.contains_key("id")
    {
        obj.insert(
            "id".to_string(),
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
) -> crate::Result<Value> {
    let mut doc = match strict_schema {
        Some(schema) => strict_format::binary_tuple_to_row_value(body, schema)
            .ok_or_else(|| undecodable(identity, body.len()))?,
        None => doc_format::decode_document_value(&with_identity(body, identity))?,
    };
    attach_row_id(&mut doc, identity);
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
) -> crate::Result<serde_json::Value> {
    let mut doc = match strict_schema {
        Some(schema) => strict_format::binary_tuple_to_json(body, schema)
            .ok_or_else(|| undecodable(identity, body.len()))?,
        None => doc_format::decode_document(&with_identity(body, identity))?,
    };
    if let serde_json::Value::Object(obj) = &mut doc
        && !obj.contains_key("id")
    {
        obj.insert(
            "id".to_string(),
            serde_json::Value::String(identity.as_str().to_string()),
        );
    }
    Ok(doc)
}

/// The schemaless body with the storage identity injected as `id`.
/// `inject_str_field` honours an existing `id` and wraps a non-map body as
/// `{id, value}`, which is the shape schemaless callers have always emitted
/// for a body that is not a document map.
fn with_identity(body: &[u8], identity: &RowIdentity) -> Vec<u8> {
    nodedb_query::msgpack_scan::inject_str_field(body, "id", identity.as_str())
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
