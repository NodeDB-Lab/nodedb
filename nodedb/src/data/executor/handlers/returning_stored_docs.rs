// SPDX-License-Identifier: BUSL-1.1

//! The full source documents of stored rows, before any `RETURNING`
//! projection or read policy. Each engine converts through the helper its
//! scan uses, so a row reads the same as a `SELECT` of it.

use super::returning_doc;
use super::returning_rows::{KvStoredRow, StoredRow, VectorRowColumns, VectorStoredRow};
use crate::data::executor::doc_format;
use crate::data::executor::scan_normalize::{kv_row_to_doc, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

/// The documents of stored document or CRDT rows. `strict_schema` is `Some`
/// exactly when the collection stores Binary Tuples. A decode failure fails
/// the statement, never a bare `{id}`.
pub(in crate::data::executor) fn stored_docs(
    strict_schema: Option<&StrictSchema>,
    identity_column: &str,
    rows: &[StoredRow<'_>],
) -> crate::Result<Vec<Value>> {
    rows.iter()
        .map(|(doc_id, body)| {
            returning_doc::from_stored(body, doc_id, strict_schema, identity_column)
        })
        .collect::<crate::Result<Vec<_>>>()
}

/// The documents of stored KV rows, via [`kv_row_to_doc`], the helper KV
/// scans use. A body that fails to decode fails the statement.
pub(in crate::data::executor) fn kv_stored_docs(
    rows: &[KvStoredRow<'_>],
) -> crate::Result<Vec<Value>> {
    rows.iter()
        .map(|(key, value)| {
            let (_key_str, body) = kv_row_to_doc(key, value)?;
            doc_format::decode_document_value(&body)
        })
        .collect::<crate::Result<Vec<_>>>()
}

/// The documents of vector-primary rows, via [`sparse_row_to_doc`] against
/// [`SparseBodyFormatRef::VectorSidecar`], with each row's vector under
/// `columns.vector`. An unreadable sidecar fails the statement: an empty row
/// set would report that a write which landed affected nothing.
pub(in crate::data::executor) fn vector_stored_docs(
    columns: VectorRowColumns<'_>,
    rows: &[VectorStoredRow<'_>],
) -> crate::Result<Vec<Value>> {
    rows.iter()
        .map(|(row_key, sidecar, vector)| {
            let (_id, mp) = sparse_row_to_doc(
                row_key,
                sidecar,
                SparseBodyFormatRef::VectorSidecar,
                columns.identity,
            )?;
            let mut doc = doc_format::decode_document_value(&mp)?;
            if let Value::Object(map) = &mut doc {
                map.insert(
                    columns.vector.to_string(),
                    super::vector_row_column::vector_value(vector),
                );
            }
            Ok(doc)
        })
        .collect::<crate::Result<Vec<_>>>()
}
