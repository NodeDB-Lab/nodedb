// SPDX-License-Identifier: BUSL-1.1

//! Write journals for collection rebuilds of the inverted index.
//!
//! A rebuild reads a collection's index from a redb read snapshot on
//! another thread. Every document write to the collection after that
//! snapshot notes the document's surrogate here. At cutover the owning
//! core reads each noted document's live footprint and writes it over the
//! rebuilt state in the same transaction, so the rebuilt index holds every
//! write the live index took.
//!
//! Noting a surrogate whose write later aborts is harmless: the cutover
//! reads the live footprint, which the abort left unchanged.
//!
//! The cutover replays at most `max_docs` noted documents on the core. A
//! journal past that bound is overflowed. It still records every surrogate
//! in a bitmap, at most one bit per document of the collection. Its
//! rebuild then runs a catch-up round off the core: see `rebuild_round`.

use std::time::Instant;

use redb::ReadableDatabase as _;
use roaring::RoaringBitmap;

use nodedb_types::TenantId;

use super::core::InvertedIndex;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use super::rebuild_snapshot::FtsRebuildTicket;
use crate::engine::sparse::fts_redb::keys::KeyOwner;
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, POSTINGS};

/// Default bound on the distinct documents one cutover replays on the core.
pub const FTS_REBUILD_JOURNAL_MAX_DOCS: usize = 1 << 20;

/// Default bound on the catch-up rounds of one rebuild.
pub const FTS_REBUILD_MAX_ROUNDS: u32 = 8;

/// The bounds of one collection rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FtsRebuildBounds {
    /// Distinct documents one cutover replays on the core at most.
    pub max_docs: usize,
    /// Catch-up rounds the rebuild runs at most before it fails.
    pub max_rounds: u32,
}

impl FtsRebuildBounds {
    /// The bounds every production rebuild runs under.
    pub const DEFAULT: Self = Self {
        max_docs: FTS_REBUILD_JOURNAL_MAX_DOCS,
        max_rounds: FTS_REBUILD_MAX_ROUNDS,
    };
}

/// Where a journal stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JournalState {
    Recording,
    /// More than `max_docs` distinct documents were written. The journal
    /// still records each one.
    Overflowed,
    /// The collection or its tenant was purged.
    Purged,
}

/// Documents written to one collection since its rebuild snapshot.
#[derive(Debug)]
pub(super) struct FtsJournal {
    pub(super) token: u64,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: String,
    pub(super) bounds: FtsRebuildBounds,
    pub(super) touched: RoaringBitmap,
    pub(super) state: JournalState,
    /// The highest text-history purge horizon applied to the collection
    /// since the rebuild's first snapshot. That snapshot still holds the
    /// purged versions, so the cutover purges the rebuilt history at this
    /// horizon too.
    pub(super) purged_below: Option<i64>,
    /// Catch-up rounds the rebuild ran so far.
    pub(super) rounds: u32,
    /// When the snapshot of the current pass was pinned.
    pub(super) pinned_at: Instant,
}

impl FtsJournal {
    pub(super) fn is_for(&self, database_id: u64, tid: u64, collection: &str) -> bool {
        self.database_id == database_id && self.tid == tid && self.collection == collection
    }

    fn note(&mut self, surrogate: u32) {
        if self.state == JournalState::Purged {
            return;
        }
        self.touched.insert(surrogate);
        if self.touched.len() > self.bounds.max_docs as u64 {
            self.state = JournalState::Overflowed;
        }
    }
}

/// The open journals of one inverted index.
#[derive(Debug, Default)]
pub(super) struct FtsJournals {
    next_token: u64,
    pub(super) active: Vec<FtsJournal>,
}

impl InvertedIndex {
    /// Open a rebuild of `collection`: pin a read snapshot and start its
    /// write journal in the same step.
    ///
    /// The returned ticket reads the snapshot on any thread. Errors when a
    /// rebuild of the collection already runs, or when redb cannot open the
    /// snapshot.
    pub fn begin_rebuild(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        bounds: FtsRebuildBounds,
    ) -> crate::Result<FtsRebuildTicket> {
        let t = tid.as_u64();
        let mut journals = self.journals.borrow_mut();
        if journals
            .active
            .iter()
            .any(|j| j.is_for(database_id, t, collection))
        {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("full-text index of collection \"{collection}\""),
                detail: "a rebuild of it is already running".to_string(),
            });
        }
        let txn = self
            .inner
            .backend()
            .db()
            .begin_read()
            .map_err(|e| inverted_err("rebuild snapshot", e))?;
        journals.next_token += 1;
        let token = journals.next_token;
        journals.active.push(FtsJournal {
            token,
            database_id,
            tid: t,
            collection: collection.to_string(),
            bounds,
            touched: RoaringBitmap::new(),
            state: JournalState::Recording,
            purged_below: None,
            rounds: 0,
            // no-determinism: the clock only sizes the write rate a refusal
            // names. The rebuilt rows do not depend on it.
            pinned_at: Instant::now(),
        });
        Ok(FtsRebuildTicket::new(
            token,
            database_id,
            t,
            collection.to_string(),
            txn,
            0,
        ))
    }

    /// Whether `collection` holds any posting or document-length row, i.e.
    /// whether it has a full-text index to rebuild.
    pub fn has_collection_rows(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<bool> {
        let t = tid.as_u64();
        let txn = self
            .inner
            .backend()
            .db()
            .begin_read()
            .map_err(|e| inverted_err("rebuild probe txn", e))?;
        let owner = KeyOwner::collection(database_id, t, collection);
        let postings = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("rebuild probe postings", e))?;
        if scan::has_posting_rows(&postings, owner)
            .map_err(|e| inverted_err("rebuild probe postings range", e))?
        {
            return Ok(true);
        }
        let lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("rebuild probe doc_lengths", e))?;
        scan::has_doc_rows(&lengths, owner)
            .map_err(|e| inverted_err("rebuild probe doc_lengths range", e))
    }

    /// Close the journal of rebuild `token`. The index is unchanged.
    pub fn abort_rebuild(&self, token: u64) {
        self.take_journal(token);
    }

    /// Whether the journal of rebuild `token` still records: the rebuild
    /// can still cut over, after catch-up rounds when the journal passed
    /// its bound.
    pub fn rebuild_is_open(&self, token: u64) -> bool {
        self.journals
            .borrow()
            .active
            .iter()
            .any(|j| j.token == token && j.state != JournalState::Purged)
    }

    /// Whether a rebuild of `collection` is open. Every write to the
    /// collection then notes its document, whatever fields it declares.
    pub fn has_open_rebuild(&self, database_id: u64, tid: TenantId, collection: &str) -> bool {
        let t = tid.as_u64();
        self.journals
            .borrow()
            .active
            .iter()
            .any(|j| j.is_for(database_id, t, collection) && j.state != JournalState::Purged)
    }

    /// Remove and return the journal of rebuild `token`.
    pub(super) fn take_journal(&self, token: u64) -> Option<FtsJournal> {
        let mut journals = self.journals.borrow_mut();
        let pos = journals.active.iter().position(|j| j.token == token)?;
        Some(journals.active.swap_remove(pos))
    }

    /// Note a write of the document `scope` names.
    pub(super) fn note_doc_write(&self, scope: IndexDocScope<'_>) {
        let mut journals = self.journals.borrow_mut();
        let t = scope.tid.as_u64();
        for journal in journals
            .active
            .iter_mut()
            .filter(|j| j.is_for(scope.database_id, t, scope.collection))
        {
            journal.note(scope.surrogate.as_u32());
        }
    }

    /// Note a text-history purge: `horizon_of(database, tenant, collection)`
    /// names each collection's horizon, `None` for a collection it keeps.
    /// Each open journal keeps the highest horizon of its collection.
    pub(super) fn note_history_purge(&self, horizon_of: &dyn Fn(u64, u64, &str) -> Option<i64>) {
        let mut journals = self.journals.borrow_mut();
        for journal in journals
            .active
            .iter_mut()
            .filter(|j| j.state != JournalState::Purged)
        {
            if let Some(horizon) = horizon_of(journal.database_id, journal.tid, &journal.collection)
            {
                journal.purged_below =
                    Some(journal.purged_below.map_or(horizon, |h| h.max(horizon)));
            }
        }
    }

    /// Mark the journals a purge invalidates: one collection, or every
    /// collection of the tenant when `collection` is `None`.
    pub(super) fn note_purge(&self, database_id: u64, tid: u64, collection: Option<&str>) {
        let mut journals = self.journals.borrow_mut();
        for journal in journals.active.iter_mut().filter(|j| {
            j.database_id == database_id
                && j.tid == tid
                && collection.is_none_or(|c| j.collection == c)
        }) {
            journal.state = JournalState::Purged;
            journal.touched = RoaringBitmap::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal(max_docs: usize) -> FtsJournal {
        FtsJournal {
            token: 1,
            database_id: 0,
            tid: 1,
            collection: "docs".to_string(),
            bounds: FtsRebuildBounds {
                max_docs,
                max_rounds: 1,
            },
            touched: RoaringBitmap::new(),
            state: JournalState::Recording,
            purged_below: None,
            rounds: 0,
            pinned_at: Instant::now(),
        }
    }

    #[test]
    fn a_repeat_write_does_not_count_twice() {
        let mut j = journal(2);
        j.note(1);
        j.note(1);
        j.note(2);
        assert_eq!(j.state, JournalState::Recording);
        assert_eq!(j.touched.len(), 2);
    }

    #[test]
    fn a_write_past_the_bound_overflows_and_keeps_the_set() {
        let mut j = journal(2);
        j.note(1);
        j.note(2);
        j.note(3);
        assert_eq!(j.state, JournalState::Overflowed);
        assert_eq!(j.touched.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }
}
