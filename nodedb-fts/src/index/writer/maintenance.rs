// SPDX-License-Identifier: Apache-2.0

//! Segment publication and collection purging.

use super::FtsIndex;
use crate::{
    backend::FtsBackend,
    index::error::FtsIndexError,
    lsm::{compaction, segment::writer as seg_writer},
    scope::IndexScope,
};
use std::sync::atomic::Ordering;
use tracing::debug;

impl<B: FtsBackend> FtsIndex<B> {
    /// Flush one index's memtable postings to an immutable segment of that
    /// index in the backend. Every other index keeps its memtable state.
    ///
    /// The postings leave the memtable only after the segment is written, so
    /// a failed flush loses nothing.
    ///
    /// Calling this before serializing the index guarantees that all posting
    /// data written since the last spill threshold is captured in the backend's
    /// segment storage rather than the in-memory memtable.  Callers that
    /// checkpoint the index (e.g., NodeDB-Lite flush) must call this once per
    /// active index before persisting.
    pub fn flush_memtable<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let index = index.into();
        let segment = self
            .memtable
            .with_scope_postings(database_id, tid, index, |postings| {
                (!postings.is_empty()).then(|| seg_writer::flush_postings_to_segment(postings))
            })
            .flatten();
        let Some(segment_bytes) = segment else {
            self.memtable.drain_scope(database_id, tid, index);
            return Ok(());
        };
        let segment_bytes = segment_bytes?;

        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let id = compaction::segment_id(seg_id, 0);
        self.backend
            .write_segment(database_id, tid, index, &id, &segment_bytes)
            .map_err(FtsIndexError::backend)?;
        self.memtable.drain_scope(database_id, tid, index);

        debug!(
            database_id,
            tid,
            collection = index.collection(),
            field = index.field_key(),
            seg_id,
            bytes = segment_bytes.len(),
            "flushed memtable to segment"
        );
        Ok(())
    }

    /// Flush every index the memtable holds, each to its own segment.
    pub fn flush_all_memtables(&self) -> Result<(), FtsIndexError<B::Error>> {
        for scope in self.memtable.scopes() {
            self.flush_memtable(scope.database_id, scope.tid, scope.index())?;
        }
        Ok(())
    }

    /// Purge every index of a collection. Returns count of removed entries.
    pub fn purge_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<usize, B::Error> {
        self.memtable.drain_collection(database_id, tid, collection);
        self.backend.purge_collection(database_id, tid, collection)
    }

    /// Purge all entries for a `(database_id, tenant)` across every collection.
    pub fn purge_tenant(&self, database_id: u64, tid: u64) -> Result<usize, B::Error> {
        self.memtable.drain_tenant(database_id, tid);
        self.backend.purge_tenant(database_id, tid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::test_support::test_governor;
    use crate::{
        FtsSearchParams,
        block::CompactPosting,
        lsm::memtable::{Memtable, MemtableConfig},
        posting::{Bm25Params, QueryMode},
    };
    use nodedb_types::Surrogate;
    use std::sync::atomic::AtomicU64;
    const DB: u64 = 0;
    const T: u64 = 1;
    const DOCS: IndexScope<'static> = IndexScope::document("docs");
    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new(), test_governor())
    }

    fn hits(idx: &FtsIndex<MemoryBackend>, index: IndexScope<'_>, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = idx
            .search(
                DB,
                T,
                index,
                FtsSearchParams {
                    query,
                    top_k: usize::MAX,
                    fuzzy_enabled: false,
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|r| r.doc_id.0)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn flush_propagates_term_too_long_as_typed_error() {
        let backend = MemoryBackend::new();
        let idx = FtsIndex {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig {
                max_postings: 1,
                max_terms: 1,
            }),
            next_segment_id: AtomicU64::new(1),
            governor: test_governor(),
        };

        // Insert a single posting under a term whose byte length exceeds the u16 segment-format cap. Bypasses the analyzer (which would tokenize away most pathological inputs); we want to exercise the flush-path boundary check directly.
        let oversize_term = "x".repeat(crate::lsm::segment::format::MAX_TERM_LEN + 1);
        idx.memtable.insert(
            DB,
            T,
            DOCS,
            &oversize_term,
            CompactPosting {
                doc_id: Surrogate(1),
                term_freq: 1,
                fieldnorm: 1,
                positions: vec![0],
            },
        );
        idx.memtable.record_doc(DB, T, DOCS, Surrogate(1), 1);

        let err = idx
            .flush_memtable(DB, T, "docs")
            .expect_err("flush must reject oversize term");
        match err {
            FtsIndexError::TermTooLong { len, max } => {
                assert_eq!(len, oversize_term.len());
                assert_eq!(max, crate::lsm::segment::format::MAX_TERM_LEN);
            }
            other => panic!("expected TermTooLong, got {other:?}"),
        }
        assert_eq!(
            idx.memtable.get_postings(DB, T, DOCS, &oversize_term).len(),
            1,
            "a failed flush keeps the postings in the memtable"
        );
    }
    #[test]
    fn memtable_flush_on_threshold() {
        let backend = MemoryBackend::new();
        let idx = FtsIndex {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig {
                max_postings: 5,
                max_terms: 100,
            }),
            next_segment_id: AtomicU64::new(1),
            governor: test_governor(),
        };

        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate(1),
            "alpha bravo charlie delta echo foxtrot",
        )
        .unwrap();

        assert!(idx.memtable.is_empty());
        let segments = idx.backend.list_segments(DB, T, DOCS).unwrap();
        assert!(!segments.is_empty(), "segment should have been written");
        assert_eq!(hits(&idx, DOCS, "charlie"), vec![1]);
    }
    #[test]
    fn purge_collection_preserves_others() {
        let idx = make_index();
        let a = IndexScope::document("col_a");
        let a_title = IndexScope::field("col_a", "title").unwrap();
        let b = IndexScope::document("col_b");
        idx.index_document(DB, T, a, Surrogate(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, a_title, Surrogate(1), "alpha")
            .unwrap();
        idx.index_document(DB, T, b, Surrogate(1), "delta echo")
            .unwrap();

        idx.purge_collection(DB, T, "col_a").unwrap();
        assert_eq!(idx.backend.collection_stats(DB, T, a).unwrap(), (0, 0));
        assert_eq!(
            idx.backend.collection_stats(DB, T, a_title).unwrap(),
            (0, 0)
        );
        assert!(idx.backend.collection_stats(DB, T, b).unwrap().0 > 0);

        assert!(!idx.memtable.get_postings(DB, T, b, "delta").is_empty());
        assert!(idx.memtable.get_postings(DB, T, a, "alpha").is_empty());
        assert!(
            idx.memtable
                .get_postings(DB, T, a_title, "alpha")
                .is_empty()
        );
    }

    /// Flushing one collection's index writes only that index's postings
    /// to its segment. The other collection keeps its memtable postings and
    /// stats, and both stay searchable.
    #[test]
    fn flush_of_one_collection_leaves_the_other_intact() {
        let idx = make_index();
        let a = IndexScope::document("col_a");
        let b = IndexScope::document("col_b");
        idx.index_document(DB, T, a, Surrogate(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, b, Surrogate(2), "alpha charlie")
            .unwrap();
        let b_stats = idx.memtable.stats(DB, T, b);

        idx.flush_memtable(DB, T, a).unwrap();

        assert!(idx.memtable.terms(DB, T, a).is_empty());
        assert_eq!(idx.backend.list_segments(DB, T, a).unwrap().len(), 1);
        assert!(idx.backend.list_segments(DB, T, b).unwrap().is_empty());
        assert_eq!(idx.memtable.get_postings(DB, T, b, "alpha").len(), 1);
        assert_eq!(idx.memtable.stats(DB, T, b), b_stats);
        assert_eq!(b_stats, (1, 2));

        assert_eq!(hits(&idx, a, "alpha"), vec![1]);
        assert_eq!(hits(&idx, b, "alpha"), vec![2]);
        assert!(hits(&idx, a, "charlie").is_empty());
        assert_eq!(idx.index_stats(DB, T, a).unwrap().0, 1);
        assert_eq!(idx.index_stats(DB, T, b).unwrap().0, 1);
    }

    /// Stats of a field index count only the documents that hold that field.
    #[test]
    fn stats_are_per_index() {
        let idx = make_index();
        let title = IndexScope::field("docs", "title").unwrap();
        idx.index_document(DB, T, DOCS, Surrogate(1), "rust book")
            .unwrap();
        idx.index_document(DB, T, DOCS, Surrogate(2), "java book")
            .unwrap();
        idx.index_document(DB, T, title, Surrogate(1), "rust")
            .unwrap();

        assert_eq!(idx.index_stats(DB, T, DOCS).unwrap(), (2, 2.0));
        assert_eq!(idx.index_stats(DB, T, title).unwrap(), (1, 1.0));
        assert_eq!(idx.memtable.stats(DB, T, DOCS), (2, 4));
        assert_eq!(idx.memtable.stats(DB, T, title), (1, 1));
    }

    /// A low spill threshold flushes every index to its own segment, and
    /// every index stays searchable.
    #[test]
    fn low_threshold_spills_each_index_to_its_own_segment() {
        let idx = FtsIndex::with_memtable_config(
            MemoryBackend::new(),
            MemtableConfig {
                max_postings: 4,
                max_terms: 100,
            },
            test_governor(),
        );
        let title = IndexScope::field("docs", "title").unwrap();
        let other = IndexScope::document("other");
        idx.index_document(DB, T, title, Surrogate(1), "rust")
            .unwrap();
        idx.index_document(DB, T, other, Surrogate(2), "rust")
            .unwrap();
        idx.index_document(DB, T, DOCS, Surrogate(1), "rust handbook")
            .unwrap();

        assert!(idx.memtable.is_empty());
        for index in [title, other, DOCS] {
            assert_eq!(idx.backend.list_segments(DB, T, index).unwrap().len(), 1);
        }
        assert_eq!(hits(&idx, title, "rust"), vec![1]);
        assert_eq!(hits(&idx, other, "rust"), vec![2]);
        assert_eq!(hits(&idx, DOCS, "handbook"), vec![1]);
        assert!(hits(&idx, title, "handbook").is_empty());
    }

    #[test]
    fn with_config_sets_bm25_params_and_memtable_thresholds() {
        let params = Bm25Params { k1: 2.0, b: 0.5 };
        let idx = FtsIndex::with_config(
            MemoryBackend::new(),
            params,
            MemtableConfig {
                max_postings: 1,
                max_terms: 100,
            },
            test_governor(),
        );
        assert_eq!(idx.bm25_params.k1, 2.0);
        assert_eq!(idx.bm25_params.b, 0.5);

        idx.index_document(DB, T, DOCS, Surrogate(1), "alpha")
            .unwrap();
        assert!(idx.memtable.is_empty());
        assert_eq!(idx.backend.list_segments(DB, T, DOCS).unwrap().len(), 1);
        assert_eq!(hits(&idx, DOCS, "alpha"), vec![1]);
    }
}
