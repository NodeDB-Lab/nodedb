// SPDX-License-Identifier: BUSL-1.1

//! Per-document term sets: the record of which posting lists of one index a
//! document is currently a member of.
//!
//! ## Why this exists
//!
//! `write_index_data` only ever touches the terms present in the text it was
//! handed. That makes an insert correct and a same-text re-index idempotent,
//! but it makes an UPDATE wrong: the terms the PREVIOUS version of the
//! document contributed are invisible to it, so a word removed by the update
//! keeps its posting, the document keeps matching a word it no longer
//! contains, and that term's document frequency stays inflated — which skews
//! the IDF of every other query hitting the same term.
//!
//! Fixing that needs the old term set. Two ways to obtain it:
//!
//! * Range-scan every posting list in the index and strip the surrogate
//!   from any list that still names it. Needs no extra storage, but costs
//!   O(terms in the index) on every single re-index — an unbounded,
//!   corpus-sized cost on the write path.
//! * Persist the document's own term set beside its `DOC_LENGTHS` row, so a
//!   re-index removes exactly the terms that dropped out: O(terms in the old
//!   document).
//!
//! The second wins: the cost scales with the document being written rather
//! than with the corpus it lands in. Storage cost is one row per indexed
//! document per index holding its DISTINCT analyzed terms — bounded by the
//! document's own text (post-stemming, deduplicated, so typically well under
//! the raw field bytes) and independent of corpus size.
//!
//! The scan survives only as the bounded fallback for a document whose term
//! set row is missing: it runs at most once per such document (the re-index
//! that triggers it writes the term set, so the next one takes the targeted
//! path) and never for a document that is not already in the index —
//! `stored` is consulted only when `DOC_LENGTHS` proves a prior index exists.

use std::collections::BTreeSet;

use redb::{ReadableTable as _, WriteTransaction};

use nodedb_fts::IndexScope;
use nodedb_fts::posting::Posting;

use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::fts_redb::keys::{KeyOwner, doc_key, posting_key};
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::{DOC_TERMS, POSTINGS};

/// The term set a previous index of this surrogate recorded in `index`, or
/// `None` when none is stored.
pub(super) fn stored(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
) -> crate::Result<Option<Vec<String>>> {
    let table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    let key = doc_key(doc.database_id, doc.tid.as_u64(), index, doc.surrogate);
    match table
        .get(key)
        .map_err(|e| inverted_err("read doc_terms", e))?
    {
        Some(v) => zerompk::from_msgpack::<Vec<String>>(v.value())
            .map(Some)
            .map_err(|e| inverted_err("decode doc_terms", e)),
        None => Ok(None),
    }
}

/// Record `terms` as the document's current term set in `index`, replacing
/// any prior one. Must be called in the same transaction as the posting
/// writes it describes, so a crash can never leave the set disagreeing with
/// POSTINGS.
pub(super) fn put(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
    terms: &BTreeSet<&str>,
) -> crate::Result<()> {
    let encoded: Vec<&str> = terms.iter().copied().collect();
    let bytes =
        zerompk::to_msgpack_vec(&encoded).map_err(|e| inverted_err("serialize doc_terms", e))?;
    let mut table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    table
        .insert(
            doc_key(doc.database_id, doc.tid.as_u64(), index, doc.surrogate),
            bytes.as_slice(),
        )
        .map_err(|e| inverted_err("insert doc_terms", e))?;
    Ok(())
}

/// Drop the document's term-set row in `index`. Paired with the removal of
/// its `DOC_LENGTHS` row so the two never disagree about whether the
/// document is in the index.
pub(super) fn clear(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    table
        .remove(doc_key(
            doc.database_id,
            doc.tid.as_u64(),
            index,
            doc.surrogate,
        ))
        .map_err(|e| inverted_err("remove doc_terms", e))?;
    Ok(())
}

/// Every term `index` currently has a posting list for.
///
/// The fallback source of "which lists might name this surrogate" for a
/// document that has no stored term set. Callers must gate it on the document
/// actually being indexed — see the module docs for why this is not the
/// general path.
pub(super) fn index_terms(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
) -> crate::Result<Vec<String>> {
    let table = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;
    let keys = scan::str_keys(
        &table,
        KeyOwner::index(doc.database_id, doc.tid.as_u64(), index),
    )
    .map_err(|e| inverted_err("postings range", e))?;
    Ok(keys.into_iter().map(|(_, _, term)| term).collect())
}

/// Remove the surrogate's posting from each of `terms` in `index`, deleting
/// a list that this empties so the term's `df` returns to its true value
/// instead of counting a document that no longer contains it.
pub(super) fn strip_postings(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
    terms: &[String],
) -> crate::Result<()> {
    if terms.is_empty() {
        return Ok(());
    }
    let t = doc.tid.as_u64();
    let mut table = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;

    // Collect the edits before applying them: `get` borrows the table
    // immutably for as long as its result lives, so the writes cannot be
    // interleaved with the reads.
    let mut updates: Vec<(&str, Option<Vec<u8>>)> = Vec::new();
    for term in terms {
        let key = posting_key(doc.database_id, t, index, term.as_str());
        let existing = match table
            .get(key)
            .map_err(|e| inverted_err("read postings", e))?
        {
            Some(v) => zerompk::from_msgpack::<Vec<Posting>>(v.value())
                .map_err(|e| inverted_err("decode postings", e))?,
            None => continue,
        };
        let mut remaining = existing;
        let before = remaining.len();
        remaining.retain(|p| p.doc_id != doc.surrogate);
        if remaining.len() == before {
            continue;
        }
        if remaining.is_empty() {
            updates.push((term.as_str(), None));
        } else {
            let bytes = zerompk::to_msgpack_vec(&remaining)
                .map_err(|e| inverted_err("serialize postings", e))?;
            updates.push((term.as_str(), Some(bytes)));
        }
    }

    for (term, new_value) in updates {
        let key = posting_key(doc.database_id, t, index, term);
        match new_value {
            None => {
                table
                    .remove(key)
                    .map_err(|e| inverted_err("remove posting", e))?;
            }
            Some(bytes) => {
                table
                    .insert(key, bytes.as_slice())
                    .map_err(|e| inverted_err("update posting", e))?;
            }
        }
    }
    Ok(())
}

/// Remove every surrogate `removals` names from its term's posting list in
/// `index`, rewriting each list once. A list this empties is deleted.
pub(super) fn strip_postings_many(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    removals: &std::collections::BTreeMap<
        String,
        std::collections::HashSet<nodedb_types::Surrogate>,
    >,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;
    for (term, docs) in removals {
        let key = posting_key(database_id, tid, index, term.as_str());
        let remaining = match table
            .get(key)
            .map_err(|e| inverted_err("read postings", e))?
        {
            Some(v) => {
                let mut postings = zerompk::from_msgpack::<Vec<Posting>>(v.value())
                    .map_err(|e| inverted_err("decode postings", e))?;
                postings.retain(|p| !docs.contains(&p.doc_id));
                postings
            }
            None => continue,
        };
        if remaining.is_empty() {
            table
                .remove(key)
                .map_err(|e| inverted_err("remove posting", e))?;
        } else {
            let bytes = zerompk::to_msgpack_vec(&remaining)
                .map_err(|e| inverted_err("serialize postings", e))?;
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| inverted_err("update posting", e))?;
        }
    }
    Ok(())
}

/// The terms a document currently occupies in `index`, for a caller that is
/// about to remove it from some or all of them.
///
/// Returns the stored term set when there is one, and otherwise falls back to
/// the index's full term list — bounded to documents that a prior index
/// actually recorded, which the caller proves by passing `previously_indexed`.
pub(super) fn occupied_terms(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
    previously_indexed: bool,
) -> crate::Result<Vec<String>> {
    if !previously_indexed {
        return Ok(Vec::new());
    }
    match stored(txn, doc, index)? {
        Some(terms) => Ok(terms),
        None => index_terms(txn, doc, index),
    }
}
