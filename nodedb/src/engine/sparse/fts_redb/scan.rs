// SPDX-License-Identifier: BUSL-1.1

//! Owned-key scans over the FTS tables.
//!
//! Each scan starts at a [`KeyOwner`]'s lower bound and stops at the first
//! key the owner does not own. A failed read surfaces as an error: no entry
//! is skipped.

use std::ops::Bound;

use redb::{ReadableTable, StorageError};

use super::keys::KeyOwner;

/// Key definition of POSTINGS.
pub(crate) type PostingKeyDef = (u64, u64, &'static str, &'static str, &'static str, u32);
/// Key definition of INDEX_META / SEGMENTS.
pub(crate) type StrKeyDef = (u64, u64, &'static str, &'static str, &'static str);
/// Key definition of DOC_LENGTHS / DOC_TERMS.
pub(crate) type DocKeyDef = (u64, u64, &'static str, &'static str, u32);
/// Key definition of STATS.
pub(crate) type StatsKeyDef = (u64, u64, &'static str, &'static str);
/// Key definition of DOC_FIELDS.
pub(crate) type DocFieldsKeyDef = (u64, u64, &'static str, u32);

/// One POSTINGS row within one collection: `(field, term, surrogate, value)`.
pub(crate) struct PostingRow {
    pub(crate) field: String,
    pub(crate) term: String,
    pub(crate) surrogate: u32,
    pub(crate) value: Vec<u8>,
}

/// One POSTINGS key `owner` owns: `(collection, field, term, surrogate)`.
pub(crate) type PostingKeyRow = (String, String, String, u32);

/// One row of a document table within one collection:
/// `(field, surrogate, value)`.
pub(crate) struct DocRow {
    pub(crate) field: String,
    pub(crate) surrogate: u32,
    pub(crate) value: Vec<u8>,
}

/// Every POSTINGS row of one collection, with values, in key order: by
/// field, then term, then surrogate.
pub(crate) fn posting_rows<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> Result<Vec<PostingRow>, StorageError>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    let owner = KeyOwner::collection(database_id, tid, collection);
    let mut out = Vec::new();
    for entry in table.range(owner.posting_start()..)? {
        let (key, value) = entry?;
        let (db, tid, collection, field, term, surrogate) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push(PostingRow {
            field: field.to_string(),
            term: term.to_string(),
            surrogate,
            value: value.value().to_vec(),
        });
    }
    Ok(out)
}

/// Every POSTINGS key `owner` owns.
pub(crate) fn posting_keys<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<PostingKeyRow>, StorageError>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    let mut out = Vec::new();
    for entry in table.range(owner.posting_start()..)? {
        let (key, _) = entry?;
        let (db, tid, collection, field, term, surrogate) = key.value();
        if !owner.owns(db, tid, collection, Some(field)) {
            break;
        }
        out.push((
            collection.to_string(),
            field.to_string(),
            term.to_string(),
            surrogate,
        ));
    }
    Ok(out)
}

/// Every distinct `(field, term)` `owner` holds postings for, in key order.
///
/// Reads one row per term: after a term's first row, the scan seeks past the
/// term's highest possible key, so the cost follows the term count, not the
/// posting count.
pub(crate) fn posting_terms<T>(
    table: &T,
    owner: KeyOwner<'_>,
) -> Result<Vec<(String, String)>, StorageError>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    let mut out: Vec<(String, String)> = Vec::new();
    let mut last: Option<(String, String, String)> = None;
    loop {
        let lower = match &last {
            None => Bound::Included(owner.posting_start()),
            Some((collection, field, term)) => Bound::Excluded((
                owner.database_id(),
                owner.tid(),
                collection.as_str(),
                field.as_str(),
                term.as_str(),
                u32::MAX,
            )),
        };
        let next = match table.range((lower, Bound::Unbounded))?.next() {
            Some(entry) => {
                let (key, _) = entry?;
                let (db, tid, collection, field, term, _) = key.value();
                owner
                    .owns(db, tid, collection, Some(field))
                    .then(|| (collection.to_string(), field.to_string(), term.to_string()))
            }
            None => None,
        };
        let Some(found) = next else {
            return Ok(out);
        };
        out.push((found.1.clone(), found.2.clone()));
        last = Some(found);
    }
}

/// Whether `owner` owns any POSTINGS row.
pub(crate) fn has_posting_rows<T>(table: &T, owner: KeyOwner<'_>) -> Result<bool, StorageError>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    match table.range(owner.posting_start()..)?.next() {
        Some(entry) => {
            let (key, _) = entry?;
            let (db, tid, collection, field, _, _) = key.value();
            Ok(owner.owns(db, tid, collection, Some(field)))
        }
        None => Ok(false),
    }
}

/// Every `(collection, field, suffix)` key `owner` owns in an INDEX_META or
/// SEGMENTS table.
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
