// SPDX-License-Identifier: BUSL-1.1

//! Row-level access to the `POSTINGS` table inside a caller's transaction.
//!
//! Each row holds one document's posting under one term. Its key is
//! `(database_id, tenant_id, collection, field, term, surrogate)` and its
//! value is the MessagePack `(term_freq, positions)` pair. A term's posting
//! list is the key range [`term_bounds`] names, read in surrogate order.
//! Writing or removing one document's posting touches one row, so its cost
//! does not depend on how many documents share the term.

use nodedb_fts::IndexScope;
use nodedb_fts::posting::Posting;
use nodedb_types::Surrogate;
use redb::{ReadableTable, Table};

use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::{posting_key, term_bounds};
use crate::engine::sparse::fts_redb::scan::PostingKeyDef;

/// The writable POSTINGS table of one write transaction.
pub(crate) type PostingTable<'txn> = Table<'txn, PostingKeyDef, &'static [u8]>;

/// The value bytes of one row: the posting without its surrogate, which the
/// key carries.
pub(crate) fn encode(posting: &Posting) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(posting.term_freq, posting.positions.as_slice()))
        .map_err(|e| redb_err("serialize posting", e))
}

/// The posting one row holds.
pub(crate) fn decode(surrogate: u32, bytes: &[u8]) -> crate::Result<Posting> {
    let (term_freq, positions): (u32, Vec<u32>) =
        zerompk::from_msgpack(bytes).map_err(|e| redb_err("deserialize posting", e))?;
    Ok(Posting {
        doc_id: Surrogate::new(surrogate),
        term_freq,
        positions,
    })
}

/// The posting list of `term` in one index, in surrogate order. Empty when
/// the term has no posting.
pub(crate) fn read_list<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<Vec<Posting>>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    let (low, high) = term_bounds(database_id, tid, index, term);
    let mut list = Vec::new();
    for entry in table
        .range(low..=high)
        .map_err(|e| redb_err("postings range", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("read posting", e))?;
        list.push(decode(key.value().5, value.value())?);
    }
    Ok(list)
}

/// The posting of `surrogate` under `term` in one index, if any.
pub(crate) fn read_one<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    surrogate: Surrogate,
) -> crate::Result<Option<Posting>>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    table
        .get(posting_key(database_id, tid, index, term, surrogate))
        .map_err(|e| redb_err("read posting", e))?
        .map(|value| decode(surrogate.as_u32(), value.value()))
        .transpose()
}

/// The number of documents holding a posting under `term` in one index.
/// Counts keys without decoding values.
pub(crate) fn count<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<usize>
where
    T: ReadableTable<PostingKeyDef, &'static [u8]>,
{
    let (low, high) = term_bounds(database_id, tid, index, term);
    let mut n = 0;
    for entry in table
        .range(low..=high)
        .map_err(|e| redb_err("postings range", e))?
    {
        entry.map_err(|e| redb_err("read posting", e))?;
        n += 1;
    }
    Ok(n)
}

/// Write `posting` under `term` in one index, replacing the document's prior
/// posting there. One row insert.
pub(crate) fn put(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    posting: &Posting,
) -> crate::Result<()> {
    let bytes = encode(posting)?;
    table
        .insert(
            posting_key(database_id, tid, index, term, posting.doc_id),
            bytes.as_slice(),
        )
        .map_err(|e| redb_err("insert posting", e))?;
    Ok(())
}

/// Remove the posting of `surrogate` under `term` in one index. One row
/// remove. Returns whether a posting was there.
pub(crate) fn remove(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    surrogate: Surrogate,
) -> crate::Result<bool> {
    let removed = table
        .remove(posting_key(database_id, tid, index, term, surrogate))
        .map_err(|e| redb_err("remove posting", e))?;
    Ok(removed.is_some())
}

/// The surrogates holding a posting under `term` in one index, in order.
fn list_surrogates(
    table: &PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<Vec<u32>> {
    let (low, high) = term_bounds(database_id, tid, index, term);
    let mut out = Vec::new();
    for entry in table
        .range(low..=high)
        .map_err(|e| redb_err("postings range", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read posting", e))?;
        out.push(key.value().5);
    }
    Ok(out)
}

/// Remove every posting of `term` in one index. Returns how many rows went.
pub(crate) fn remove_list(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<usize> {
    let surrogates = list_surrogates(table, database_id, tid, index, term)?;
    for &surrogate in &surrogates {
        remove(
            table,
            database_id,
            tid,
            index,
            term,
            Surrogate::new(surrogate),
        )?;
    }
    Ok(surrogates.len())
}

/// Make `postings` the whole posting list of `term` in one index: rows of
/// documents absent from `postings` are removed, and one row per posting is
/// written. A later posting of the same document replaces an earlier one.
pub(crate) fn replace_list(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    postings: &[Posting],
) -> crate::Result<()> {
    let keep: std::collections::HashSet<u32> = postings.iter().map(|p| p.doc_id.as_u32()).collect();
    for surrogate in list_surrogates(table, database_id, tid, index, term)? {
        if !keep.contains(&surrogate) {
            remove(
                table,
                database_id,
                tid,
                index,
                term,
                Surrogate::new(surrogate),
            )?;
        }
    }
    for posting in postings {
        put(table, database_id, tid, index, term, posting)?;
    }
    Ok(())
}
