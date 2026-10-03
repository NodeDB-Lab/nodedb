// SPDX-License-Identifier: BUSL-1.1

//! The off-core half of a collection rebuild: read the pinned snapshot and
//! derive the canonical rows of every index of the collection from it.
//!
//! Every type here is `Send` and touches no `!Send` engine state. The redb
//! read transaction was opened on the owning core by
//! `InvertedIndex::begin_rebuild`, in the same step that opened the write
//! journal, so the snapshot and the journal meet with no gap.

use std::collections::{BTreeMap, BTreeSet};

use redb::ReadTransaction;

use nodedb_fts::posting::Posting;

use super::errors::inverted_err;
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, POSTINGS};

/// A pinned snapshot of one collection's index, ready to read.
pub struct FtsRebuildTicket {
    token: u64,
    database_id: u64,
    tid: u64,
    collection: String,
    txn: ReadTransaction,
}

/// One index's postings and document lengths as the snapshot holds them.
struct SnapshotScope {
    postings: Vec<(String, Vec<Posting>)>,
    doc_lengths: Vec<(u32, u32)>,
}

/// Every index of one collection as the snapshot holds it, keyed by field
/// key (empty for the whole-document index).
pub struct FtsSnapshot {
    token: u64,
    database_id: u64,
    tid: u64,
    collection: String,
    scopes: BTreeMap<String, SnapshotScope>,
}

/// The canonical rows of one index, derived from a snapshot.
pub(super) struct RebuiltScope {
    /// Field key: empty for the whole-document index.
    pub(super) field: String,
    /// One list per term, one posting per document, ordered by surrogate.
    pub(super) postings: Vec<(String, Vec<Posting>)>,
    /// `(surrogate, token count)` per indexed document.
    pub(super) doc_lengths: Vec<(u32, u32)>,
    /// `(surrogate, distinct terms)` per document with a posting.
    pub(super) doc_terms: Vec<(u32, Vec<String>)>,
    /// Number of indexed documents.
    pub(super) doc_count: u32,
    /// Sum of the indexed documents' token counts.
    pub(super) total_tokens: u64,
}

/// The canonical rows of every index of one collection.
pub struct FtsRebuilt {
    pub(super) token: u64,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: String,
    /// One entry per index, whole-document index first.
    pub(super) scopes: Vec<RebuiltScope>,
    /// `(surrogate, field names)` per document holding any field index.
    pub(super) doc_fields: Vec<(u32, Vec<String>)>,
}

impl FtsRebuildTicket {
    pub(super) fn new(
        token: u64,
        database_id: u64,
        tid: u64,
        collection: String,
        txn: ReadTransaction,
    ) -> Self {
        Self {
            token,
            database_id,
            tid,
            collection,
            txn,
        }
    }

    /// The token that ties this rebuild to its journal.
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Read every index's postings and document lengths of the collection
    /// from the pinned snapshot. Runs on any thread. The snapshot is released
    /// on return.
    pub fn read(self) -> crate::Result<FtsSnapshot> {
        let (db, t) = (self.database_id, self.tid);
        let mut scopes: BTreeMap<String, SnapshotScope> = BTreeMap::new();

        let postings_table = self
            .txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("rebuild open postings", e))?;
        for row in scan::str_rows(&postings_table, db, t, &self.collection)
            .map_err(|e| inverted_err("rebuild postings range", e))?
        {
            let list: Vec<Posting> = zerompk::from_msgpack(&row.value)
                .map_err(|e| inverted_err("rebuild decode postings", e))?;
            scope_entry(&mut scopes, row.field)
                .postings
                .push((row.suffix, list));
        }

        let lengths_table = self
            .txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("rebuild open doc_lengths", e))?;
        for row in scan::doc_rows(&lengths_table, db, t, &self.collection)
            .map_err(|e| inverted_err("rebuild doc_lengths range", e))?
        {
            let len: u32 = zerompk::from_msgpack(&row.value)
                .map_err(|e| inverted_err("rebuild decode doc_length", e))?;
            scope_entry(&mut scopes, row.field)
                .doc_lengths
                .push((row.surrogate, len));
        }

        Ok(FtsSnapshot {
            token: self.token,
            database_id: self.database_id,
            tid: self.tid,
            collection: self.collection,
            scopes,
        })
    }
}

fn scope_entry(scopes: &mut BTreeMap<String, SnapshotScope>, field: String) -> &mut SnapshotScope {
    scopes.entry(field).or_insert_with(|| SnapshotScope {
        postings: Vec::new(),
        doc_lengths: Vec::new(),
    })
}

impl FtsSnapshot {
    /// The token that ties this rebuild to its journal.
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Derive the canonical rows of every index: one posting per document per
    /// term, the term set of each document, corpus stats counted from the
    /// document lengths, and each document's field set. Runs on any thread.
    pub fn compact(self) -> FtsRebuilt {
        let mut fields_by_doc: BTreeMap<u32, Vec<String>> = BTreeMap::new();
        let mut scopes = Vec::with_capacity(self.scopes.len());
        // The BTreeMap yields the empty (whole-document) key first.
        for (field, scope) in self.scopes {
            if !field.is_empty() {
                for &(doc, _) in &scope.doc_lengths {
                    fields_by_doc.entry(doc).or_default().push(field.clone());
                }
            }
            scopes.push(compact_scope(field, scope));
        }
        FtsRebuilt {
            token: self.token,
            database_id: self.database_id,
            tid: self.tid,
            collection: self.collection,
            scopes,
            doc_fields: fields_by_doc.into_iter().collect(),
        }
    }
}

/// Canonical rows of one index.
fn compact_scope(field: String, scope: SnapshotScope) -> RebuiltScope {
    let mut postings = Vec::with_capacity(scope.postings.len());
    let mut terms_by_doc: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    for (term, mut list) in scope.postings {
        list.sort_unstable_by(|a, b| {
            a.doc_id
                .as_u32()
                .cmp(&b.doc_id.as_u32())
                .then(b.term_freq.cmp(&a.term_freq))
        });
        list.dedup_by_key(|p| p.doc_id.as_u32());
        if list.is_empty() {
            continue;
        }
        for posting in &list {
            terms_by_doc
                .entry(posting.doc_id.as_u32())
                .or_default()
                .insert(term.clone());
        }
        postings.push((term, list));
    }
    let doc_terms = terms_by_doc
        .into_iter()
        .map(|(doc, terms)| (doc, terms.into_iter().collect()))
        .collect();
    let doc_count = u32::try_from(scope.doc_lengths.len()).unwrap_or(u32::MAX);
    let total_tokens = scope
        .doc_lengths
        .iter()
        .map(|&(_, len)| u64::from(len))
        .sum();
    RebuiltScope {
        field,
        postings,
        doc_lengths: scope.doc_lengths,
        doc_terms,
        doc_count,
        total_tokens,
    }
}

impl FtsRebuilt {
    /// The token that ties this rebuild to its journal.
    pub fn token(&self) -> u64 {
        self.token
    }

    /// The collection this rebuild covers.
    pub fn collection(&self) -> &str {
        &self.collection
    }
}
