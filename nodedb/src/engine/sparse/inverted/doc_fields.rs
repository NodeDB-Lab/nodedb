// SPDX-License-Identifier: BUSL-1.1

//! Per-document field sets: the field indexes a document holds postings in.
//!
//! A re-index sees only the new text's fields. A field the previous version
//! filled and the new one does not would keep its postings without this
//! record. The whole-document index is not listed: every indexed document
//! is tracked there by its `DOC_LENGTHS` row.

use std::collections::BTreeSet;

use redb::{ReadableTable as _, WriteTransaction};

use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::fts_redb::bytes::{put_doc_fields_row, remove_doc_fields_row};
use crate::engine::sparse::fts_redb::keys::doc_fields_key;
use crate::engine::sparse::fts_redb::tables::DOC_FIELDS;
use crate::engine::storage_bytes::StorageDelta;

/// The non-empty field names the document holds postings in. Empty when no
/// row is stored.
pub(super) fn read(txn: &WriteTransaction, doc: IndexDocScope<'_>) -> crate::Result<Vec<String>> {
    let table = txn
        .open_table(DOC_FIELDS)
        .map_err(|e| inverted_err("open doc_fields", e))?;
    let key = doc_fields_key(
        doc.database_id,
        doc.tid.as_u64(),
        doc.collection,
        doc.surrogate,
    );
    match table
        .get(key)
        .map_err(|e| inverted_err("read doc_fields", e))?
    {
        Some(v) => zerompk::from_msgpack::<Vec<String>>(v.value())
            .map_err(|e| inverted_err("decode doc_fields", e)),
        None => Ok(Vec::new()),
    }
}

/// Record `fields` as the document's field set. An empty set removes the
/// row. `delta` records the stored bytes the write changes.
pub(super) fn put(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    fields: &BTreeSet<&str>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    if fields.is_empty() {
        return clear(txn, doc, delta);
    }
    let encoded: Vec<&str> = fields.iter().copied().collect();
    let bytes =
        zerompk::to_msgpack_vec(&encoded).map_err(|e| inverted_err("serialize doc_fields", e))?;
    let mut table = txn
        .open_table(DOC_FIELDS)
        .map_err(|e| inverted_err("open doc_fields", e))?;
    put_doc_fields_row(
        &mut table,
        doc_fields_key(
            doc.database_id,
            doc.tid.as_u64(),
            doc.collection,
            doc.surrogate,
        ),
        bytes.as_slice(),
        delta,
    )
    .map_err(|e| inverted_err("insert doc_fields", e))
}

/// Drop the document's field-set row. `delta` records the bytes it held.
pub(super) fn clear(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(DOC_FIELDS)
        .map_err(|e| inverted_err("open doc_fields", e))?;
    remove_doc_fields_row(
        &mut table,
        doc_fields_key(
            doc.database_id,
            doc.tid.as_u64(),
            doc.collection,
            doc.surrogate,
        ),
        delta,
    )
    .map_err(|e| inverted_err("remove doc_fields", e))
}
