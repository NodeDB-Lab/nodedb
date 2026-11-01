// SPDX-License-Identifier: BUSL-1.1

//! Row-level access to the `POSTINGS` table inside a caller's transaction.
//!
//! Each row holds one document's posting under one term. Its key is
//! `(database_id, tenant_id, collection, field, term, surrogate)` and its
//! value is the MessagePack `(term_freq, doc_len, positions)` triple. The
//! document length rides in the row so scoring a term reads no second table. A term's posting
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
/// key carries, and the token count of the document it belongs to.
pub(crate) fn encode(posting: &Posting, doc_len: u32) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(posting.term_freq, doc_len, posting.positions.as_slice()))
        .map_err(|e| redb_err("serialize posting", e))
}

/// The posting one row holds and the token count of its document.
pub(crate) fn decode(surrogate: u32, bytes: &[u8]) -> crate::Result<(Posting, u32)> {
    let (term_freq, doc_len, positions): (u32, u32, Vec<u32>) =
        zerompk::from_msgpack(bytes).map_err(|e| redb_err("deserialize posting", e))?;
    let posting = Posting {
        doc_id: Surrogate::new(surrogate),
        term_freq,
        positions,
    };
    Ok((posting, doc_len))
}

/// The posting list of `term` in one index, in surrogate order, each posting
/// with the token count of its document. Empty when the term has no posting.
pub(crate) fn read_list_with_lengths<T>(
    table: &T,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<Vec<(Posting, u32)>>
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
    Ok(
        read_list_with_lengths(table, database_id, tid, index, term)?
            .into_iter()
            .map(|(posting, _)| posting)
            .collect(),
    )
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
        .map(|value| decode(surrogate.as_u32(), value.value()).map(|(posting, _)| posting))
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
/// posting there. `doc_len` is the token count of the posting's document.
/// One row insert.
pub(crate) fn put(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    posting: &Posting,
    doc_len: u32,
) -> crate::Result<()> {
    let bytes = encode(posting, doc_len)?;
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
/// `doc_lens` holds the token count of each posting's document, parallel to
/// `postings`.
pub(crate) fn replace_list(
    table: &mut PostingTable<'_>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    postings: &[Posting],
    doc_lens: &[u32],
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
    for (posting, doc_len) in postings.iter().zip(doc_lens) {
        put(table, database_id, tid, index, term, posting, *doc_len)?;
    }
    Ok(())
}
