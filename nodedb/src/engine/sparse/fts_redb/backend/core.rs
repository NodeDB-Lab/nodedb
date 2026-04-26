//! `RedbFtsBackend` struct, lifecycle, and the `FtsBackend` trait impl.
//!
//! Every trait method delegates to a topic-specific module
//! (`postings`, `doc_lengths`, `meta`, `stats`, `segments`, `purge`) so
//! this file stays focused on wiring rather than business logic.

use std::sync::Arc;

use redb::Database;

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::posting::Posting;
use nodedb_types::Surrogate;

use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, INDEX_META, POSTINGS, SEGMENTS, STATS};

/// Redb-backed FTS backend.
///
/// All persistent tables are keyed by the structural tuple
/// `(tenant_id, collection, …)` — tenant isolation is enforced by the
/// table schema, never by lexical-prefix ordering.
pub struct RedbFtsBackend {
    pub(super) db: Arc<Database>,
}

impl RedbFtsBackend {
    /// Open or create redb tables for FTS.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| redb_err("init tables", e))?;
        {
            write_txn
                .open_table(POSTINGS)
                .map_err(|e| redb_err("create postings table", e))?;
            write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| redb_err("create doc_lengths table", e))?;
            write_txn
                .open_table(INDEX_META)
                .map_err(|e| redb_err("create index_meta table", e))?;
            write_txn
                .open_table(STATS)
                .map_err(|e| redb_err("create stats table", e))?;
            write_txn
                .open_table(SEGMENTS)
                .map_err(|e| redb_err("create segments table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit init", e))?;

        Ok(Self { db })
    }

    /// Access the underlying database.
    pub fn db(&self) -> &Database {
        &self.db
    }
}

impl FtsBackend for RedbFtsBackend {
    type Error = crate::Error;

    fn read_postings(&self, tid: u32, collection: &str, term: &str) -> crate::Result<Vec<Posting>> {
        super::postings::read(self, tid, collection, term)
    }

    fn write_postings(
        &self,
        tid: u32,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> crate::Result<()> {
        super::postings::write(self, tid, collection, term, postings)
    }

    fn remove_postings(&self, tid: u32, collection: &str, term: &str) -> crate::Result<()> {
        super::postings::remove(self, tid, collection, term)
    }

    fn read_doc_length(
        &self,
        tid: u32,
        collection: &str,
        doc_id: Surrogate,
    ) -> crate::Result<Option<u32>> {
        super::doc_lengths::read(self, tid, collection, doc_id)
    }

    fn write_doc_length(
        &self,
        tid: u32,
        collection: &str,
        doc_id: Surrogate,
        length: u32,
    ) -> crate::Result<()> {
        super::doc_lengths::write(self, tid, collection, doc_id, length)
    }

    fn remove_doc_length(
        &self,
        tid: u32,
        collection: &str,
        doc_id: Surrogate,
    ) -> crate::Result<()> {
        super::doc_lengths::remove(self, tid, collection, doc_id)
    }

    fn collection_terms(&self, tid: u32, collection: &str) -> crate::Result<Vec<String>> {
        super::postings::collection_terms(self, tid, collection)
    }

    fn collection_stats(&self, tid: u32, collection: &str) -> crate::Result<(u32, u64)> {
        super::stats::read(self, tid, collection)
    }

    fn increment_stats(&self, tid: u32, collection: &str, doc_len: u32) -> crate::Result<()> {
        super::stats::increment(self, tid, collection, doc_len)
    }

    fn decrement_stats(&self, tid: u32, collection: &str, doc_len: u32) -> crate::Result<()> {
        super::stats::decrement(self, tid, collection, doc_len)
    }

    fn read_meta(
        &self,
        tid: u32,
        collection: &str,
        subkey: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        super::meta::read(self, tid, collection, subkey)
    }

    fn write_meta(
        &self,
        tid: u32,
        collection: &str,
        subkey: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        super::meta::write(self, tid, collection, subkey, value)
    }

    fn write_segment(
        &self,
        tid: u32,
        collection: &str,
        segment_id: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        super::segments::write(self, tid, collection, segment_id, data)
    }

    fn read_segment(
        &self,
        tid: u32,
        collection: &str,
        segment_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        super::segments::read(self, tid, collection, segment_id)
    }

    fn list_segments(&self, tid: u32, collection: &str) -> crate::Result<Vec<String>> {
        super::segments::list(self, tid, collection)
    }

    fn remove_segment(&self, tid: u32, collection: &str, segment_id: &str) -> crate::Result<()> {
        super::segments::remove(self, tid, collection, segment_id)
    }

    fn purge_collection(&self, tid: u32, collection: &str) -> crate::Result<usize> {
        super::purge::collection(self, tid, collection)
    }

    fn purge_tenant(&self, tid: u32) -> crate::Result<usize> {
        super::purge::tenant(self, tid)
    }
}
