// SPDX-License-Identifier: BUSL-1.1

//! Corpus-statistics accessors exposing the same `df` / `total_docs` /
//! `avg_doc_len` the base BM25 search reads from the durable index, so the
//! in-transaction FTS overlay merge (read-your-own-writes) can score a
//! staged, not-yet-durable document against the IDENTICAL corpus stats the
//! base search used — a staged doc must not shift the corpus itself, only
//! be scored against it.

use nodedb_fts::IndexScope;
use nodedb_fts::backend::FtsBackend;
use nodedb_types::TenantId;

use super::core::InvertedIndex;

impl InvertedIndex {
    /// Total document count and average document length of one index, as
    /// read by the base BM25 search (`FtsIndex::index_stats`).
    pub fn corpus_stats<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
    ) -> crate::Result<(u32, f32)> {
        self.inner.index_stats(database_id, tid.as_u64(), index)
    }

    /// Document frequency (number of documents containing `term`) for a
    /// single already-analyzed term in one index, read from the same POSTINGS
    /// table the base search scores against.
    pub fn term_df<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
        term: &str,
    ) -> crate::Result<u32> {
        let postings =
            self.inner
                .backend()
                .read_postings(database_id, tid.as_u64(), index.into(), term)?;
        Ok(postings.len() as u32)
    }

    /// Every document one index holds, read in one transaction. An index
    /// holds a document exactly when it records the document's length.
    pub fn index_members<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
    ) -> crate::Result<std::collections::HashSet<nodedb_types::Surrogate>> {
        self.inner
            .backend()
            .index_members(database_id, tid.as_u64(), index.into())
    }

    /// Whether any document currently holds text in one index.
    pub fn has_text<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
    ) -> crate::Result<bool> {
        let (count, _) =
            self.inner
                .backend()
                .collection_stats(database_id, tid.as_u64(), index.into())?;
        Ok(count > 0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::Database;

    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::sparse::inverted::test_support::body;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(crate::engine::durability_gate::GatedDatabase::new(
            Database::create(&path).unwrap(),
        ));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    #[test]
    fn index_members_are_the_documents_with_a_recorded_length() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(7), &body("charlie"))
            .unwrap();
        idx.index_document(DB, T, "other", Surrogate::new(3), &body("delta"))
            .unwrap();

        let members = idx.index_members(DB, T, "docs").unwrap();
        assert_eq!(
            members,
            [Surrogate::new(1), Surrogate::new(7)]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        );
        assert!(idx.index_members(DB, T, "absent").unwrap().is_empty());
    }

    /// STATS (doc count, avg doc length) must not double-count when the SAME
    /// surrogate is indexed more than once — this is exactly what happens on
    /// WAL replay, which re-invokes `index_document` for already-durable
    /// `FtsIndex` records (see `data/executor/wal_replay_fts.rs`). Before the
    /// fix, `update_stats_in_txn` unconditionally did `count += 1; total +=
    /// len` on every call, so a replayed doc was counted twice, skewing avgdl
    /// and therefore every BM25 score in the collection.
    #[test]
    fn reindex_same_surrogate_identical_content_does_not_double_count_stats() {
        let (idx, _dir) = open_temp();

        // "alpha bravo charlie" tokenizes to 3 terms.
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("alpha bravo charlie"),
        )
        .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1, "first index must count the doc once");
        assert_eq!(avg_len, 3.0, "avg doc len == the single doc's length");

        // Re-index the SAME surrogate with IDENTICAL content, simulating a WAL
        // replay of an already-durable FtsIndex record. Doc count and total
        // token sum must be unchanged (net zero), not doubled.
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("alpha bravo charlie"),
        )
        .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(
            count, 1,
            "replaying an already-indexed doc must not bump doc count"
        );
        assert_eq!(
            avg_len, 3.0,
            "replaying an already-indexed doc must not change total token sum"
        );
    }

    /// A genuine re-index of a surrogate whose content actually changed (not a
    /// replay of identical content) must still leave `count` untouched but
    /// adjust `total` by the length delta, keeping avgdl correct for the new
    /// content.
    #[test]
    fn reindex_same_surrogate_different_length_adjusts_total_by_delta() {
        let (idx, _dir) = open_temp();

        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("alpha bravo charlie"),
        )
        .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1);
        assert_eq!(avg_len, 3.0);

        // Same surrogate, longer content (5 tokens): count stays at 1 (still
        // one logical document), total moves to 5 (not 3 + 5 = 8).
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("alpha bravo charlie delta echo"),
        )
        .unwrap();
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1, "re-indexing must not create a second doc count");
        assert_eq!(
            avg_len, 5.0,
            "total must reflect the new length, not the sum of old + new"
        );
    }
}
