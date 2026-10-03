// SPDX-License-Identifier: BUSL-1.1

//! Owned-key scans over the FTS tables.
//!
//! Each scan starts at a [`KeyOwner`]'s lower bound and stops at the first
//! key the owner does not own. A failed read surfaces as an error: no entry
//! is skipped.

use redb::{ReadableTable, StorageError};

use super::keys::KeyOwner;

/// Key definition of POSTINGS / INDEX_META / SEGMENTS.
pub(crate) type StrKeyDef = (u64, u64, &'static str, &'static str, &'static str);
/// Key definition of DOC_LENGTHS / DOC_TERMS.
pub(crate) type DocKeyDef = (u64, u64, &'static str, &'static str, u32);
/// Key definition of STATS.
pub(crate) type StatsKeyDef = (u64, u64, &'static str, &'static str);
/// Key definition of DOC_FIELDS.
pub(crate) type DocFieldsKeyDef = (u64, u64, &'static str, u32);

/// One row of a string-suffixed table within one collection:
/// `(field, suffix, value)`.
pub(crate) struct StrRow {
    pub(crate) field: String,
    pub(crate) suffix: String,
    pub(crate) value: Vec<u8>,
}

/// One row of a document table within one collection:
/// `(field, surrogate, value)`.
pub(crate) struct DocRow {
    pub(crate) field: String,
    pub(crate) surrogate: u32,
    pub(crate) value: Vec<u8>,
}

/// Every POSTINGS / INDEX_META / SEGMENTS row of one collection, with values.
pub(crate) fn str_rows<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> Result<Vec<StrRow>, StorageError>
where
    T: ReadableTable<StrKeyDef, &'static [u8]>,
{
    let owner = KeyOwner::collection(database_id, tid, collection);
    let mut out = Vec::new();
    for entry in table.range(owner.str_start()..)? {
        let (key, value) = entry?;
        let (db, tid, collection, field, suffix) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push(StrRow {
            field: field.to_string(),
            suffix: suffix.to_string(),
            value: value.value().to_vec(),
        });
    }
    Ok(out)
}

/// Every `(collection, field, suffix)` key `owner` owns in a string-suffixed table.
pub(crate) fn str_keys<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<(String, String, String)>, StorageError>
where
    T: ReadableTable<StrKeyDef, &'static [u8]>,
{
    let mut out = Vec::new();
    for entry in table.range(owner.str_start()..)? {
        let (key, _) = entry?;
        let (db, tid, collection, field, suffix) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push((
            collection.to_string(),
            field.to_string(),
            suffix.to_string(),
        ));
    }
    Ok(out)
}

/// Whether `owner` owns any row of a string-suffixed table.
pub(crate) fn has_str_rows<T>(table: &T, owner: KeyOwner<'_>) -> Result<bool, StorageError>
where
    T: ReadableTable<StrKeyDef, &'static [u8]>,
{
    match table.range(owner.str_start()..)?.next() {
        Some(entry) => {
            let (key, _) = entry?;
            let (db, tid, collection, field, _) = key.value();
            Ok(owner.owns(db, tid, collection, Some(field)))
        }
        None => Ok(false),
    }
}

/// Whether `owner` owns any row of a document table.
pub(crate) fn has_doc_rows<T>(table: &T, owner: KeyOwner<'_>) -> Result<bool, StorageError>
where
    T: ReadableTable<DocKeyDef, &'static [u8]>,
{
    match table.range(owner.doc_start()..)?.next() {
        Some(entry) => {
            let (key, _) = entry?;
            let (db, tid, collection, field, _) = key.value();
            Ok(owner.owns(db, tid, collection, Some(field)))
        }
        None => Ok(false),
    }
}

/// Every DOC_LENGTHS / DOC_TERMS row of one collection, with values.
pub(crate) fn doc_rows<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> Result<Vec<DocRow>, StorageError>
where
    T: ReadableTable<DocKeyDef, &'static [u8]>,
{
    let owner = KeyOwner::collection(database_id, tid, collection);
    let mut out = Vec::new();
    for entry in table.range(owner.doc_start()..)? {
        let (key, value) = entry?;
        let (db, tid, collection, field, surrogate) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push(DocRow {
            field: field.to_string(),
            surrogate,
            value: value.value().to_vec(),
        });
    }
    Ok(out)
}

/// Every `(collection, field, surrogate)` key `owner` owns in a document table.
pub(crate) fn doc_keys<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<(String, String, u32)>, StorageError>
where
    T: ReadableTable<DocKeyDef, &'static [u8]>,
{
    let mut out = Vec::new();
    for entry in table.range(owner.doc_start()..)? {
        let (key, _) = entry?;
        let (db, tid, collection, field, surrogate) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push((collection.to_string(), field.to_string(), surrogate));
    }
    Ok(out)
}

/// Every `(collection, field)` STATS key `owner` owns.
pub(crate) fn stats_keys<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<(String, String)>, StorageError>
where
    T: ReadableTable<StatsKeyDef, &'static [u8]>,
{
    let mut out = Vec::new();
    for entry in table.range(owner.stats_start()..)? {
        let (key, _) = entry?;
        let (db, tid, collection, field) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push((collection.to_string(), field.to_string()));
    }
    Ok(out)
}

/// Every `(collection, surrogate)` DOC_FIELDS key `owner` owns. A field
/// owner covers its whole collection here: DOC_FIELDS has no field component.
pub(crate) fn doc_fields_keys<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<(String, u32)>, StorageError>
where
    T: ReadableTable<DocFieldsKeyDef, &'static [u8]>,
{
    let mut out = Vec::new();
    for entry in table.range(owner.doc_fields_start()..)? {
        let (key, _) = entry?;
        let (db, tid, collection, surrogate) = key.value();
        if !owner.owns(db, tid, collection, None) {
            break;
        }
        out.push((collection.to_string(), surrogate));
    }
    Ok(out)
}
