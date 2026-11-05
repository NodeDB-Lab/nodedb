// SPDX-License-Identifier: BUSL-1.1

//! Stored bytes of the counted full-text rows, and the counted row writes
//! the document-shaped tables share.
//!
//! A tuple key counts 8 bytes per `u64` or `i64`, 4 per `u32`, and the UTF-8
//! length of each `&str`. Every counted full-text key leads with
//! `(database_id, tenant_id)`: 16 bytes.

use redb::{StorageError, Table};

use super::keys::{DocFieldsKey, FieldDocKey, FieldStrKey, PostingKey};
use super::scan::{DocFieldsKeyDef, DocKeyDef, StrKeyDef};
use crate::engine::storage_bytes::StorageDelta;

/// Key bytes of the leading `(database_id, tenant_id)`.
const SCOPE_BYTES: usize = 16;
/// Key bytes of a `u32` surrogate.
const SURROGATE_BYTES: usize = 4;
/// Bytes of an `i64` ordinal, in a key or as a value.
const ORDINAL_BYTES: usize = 8;

/// Key bytes of a `text.postings` key.
pub(crate) fn posting_key_len(key: &PostingKey<'_>) -> usize {
    SCOPE_BYTES + key.2.len() + key.3.len() + key.4.len() + SURROGATE_BYTES
}

/// Key bytes of a `text.doc_lengths` or `text.doc_terms` key.
pub(crate) fn doc_key_len(key: &FieldDocKey<'_>) -> usize {
    SCOPE_BYTES + key.2.len() + key.3.len() + SURROGATE_BYTES
}

/// Key bytes of a `text.doc_fields` key.
pub(crate) fn doc_fields_key_len(key: &DocFieldsKey<'_>) -> usize {
    SCOPE_BYTES + key.2.len() + SURROGATE_BYTES
}

/// Key bytes of a `text.segments` key.
pub(crate) fn segment_key_len(key: &FieldStrKey<'_>) -> usize {
    SCOPE_BYTES + key.2.len() + key.3.len() + key.4.len()
}

/// Stored bytes of one `text.live_from` row: its key and its `i64` start.
pub(crate) fn live_from_row_len(collection: &str, field: &str) -> usize {
    SCOPE_BYTES + collection.len() + field.len() + SURROGATE_BYTES + ORDINAL_BYTES
}

/// Stored bytes of one `text.live_by_from` row: its key and its `u8` value.
pub(crate) fn live_by_from_row_len(collection: &str, field: &str) -> usize {
    SCOPE_BYTES + collection.len() + field.len() + ORDINAL_BYTES + SURROGATE_BYTES + 1
}

/// Key bytes of a `text.ended` key.
pub(crate) fn ended_key_len(collection: &str, field: &str) -> usize {
    SCOPE_BYTES + collection.len() + field.len() + 2 * ORDINAL_BYTES + SURROGATE_BYTES
}

/// Insert `value` under `key` into a `text.doc_lengths`-shaped table, and
/// record the bytes it adds in `delta`.
pub(crate) fn put_doc_row(
    table: &mut Table<'_, DocKeyDef, &'static [u8]>,
    key: FieldDocKey<'_>,
    value: &[u8],
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    let old = table.insert(key, value)?.map(|prior| prior.value().len());
    delta.replaced(key.0, key.1, key.2, doc_key_len(&key), old, value.len());
    Ok(())
}

/// Remove `key` from a `text.doc_lengths`-shaped table, and record the
/// bytes it held in `delta`. Returns whether a row was there.
pub(crate) fn remove_doc_row(
    table: &mut Table<'_, DocKeyDef, &'static [u8]>,
    key: FieldDocKey<'_>,
    delta: &mut StorageDelta,
) -> Result<bool, StorageError> {
    let old = table.remove(key)?.map(|prior| prior.value().len());
    if let Some(len) = old {
        delta.removed(key.0, key.1, key.2, doc_key_len(&key), len);
    }
    Ok(old.is_some())
}

/// Insert `value` under `key` into `text.doc_fields`, and record the bytes
/// it adds in `delta`.
pub(crate) fn put_doc_fields_row(
    table: &mut Table<'_, DocFieldsKeyDef, &'static [u8]>,
    key: DocFieldsKey<'_>,
    value: &[u8],
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    let old = table.insert(key, value)?.map(|prior| prior.value().len());
    delta.replaced(
        key.0,
        key.1,
        key.2,
        doc_fields_key_len(&key),
        old,
        value.len(),
    );
    Ok(())
}

/// Remove `key` from `text.doc_fields`, and record the bytes it held in
/// `delta`.
pub(crate) fn remove_doc_fields_row(
    table: &mut Table<'_, DocFieldsKeyDef, &'static [u8]>,
    key: DocFieldsKey<'_>,
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    if let Some(len) = table.remove(key)?.map(|prior| prior.value().len()) {
        delta.removed(key.0, key.1, key.2, doc_fields_key_len(&key), len);
    }
    Ok(())
}

/// Insert `value` under `key` into `text.segments`, and record the bytes it
/// adds in `delta`.
pub(crate) fn put_segment_row(
    table: &mut Table<'_, StrKeyDef, &'static [u8]>,
    key: FieldStrKey<'_>,
    value: &[u8],
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    let old = table.insert(key, value)?.map(|prior| prior.value().len());
    delta.replaced(key.0, key.1, key.2, segment_key_len(&key), old, value.len());
    Ok(())
}

/// Remove `key` from `text.segments`, and record the bytes it held in
/// `delta`.
pub(crate) fn remove_segment_row(
    table: &mut Table<'_, StrKeyDef, &'static [u8]>,
    key: FieldStrKey<'_>,
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    if let Some(len) = table.remove(key)?.map(|prior| prior.value().len()) {
        delta.removed(key.0, key.1, key.2, segment_key_len(&key), len);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tuple_key_counts_each_component() {
        assert_eq!(
            posting_key_len(&(1, 2, "docs", "", "term", 7)),
            16 + 4 + 4 + 4
        );
        assert_eq!(doc_key_len(&(1, 2, "docs", "title", 7)), 16 + 4 + 5 + 4);
        assert_eq!(doc_fields_key_len(&(1, 2, "docs", 7)), 16 + 4 + 4);
        assert_eq!(segment_key_len(&(1, 2, "docs", "", "L0:1")), 16 + 4 + 4);
        assert_eq!(live_from_row_len("docs", ""), 16 + 4 + 4 + 8);
        assert_eq!(live_by_from_row_len("docs", ""), 16 + 4 + 8 + 4 + 1);
        assert_eq!(ended_key_len("docs", ""), 16 + 4 + 16 + 4);
    }
}
