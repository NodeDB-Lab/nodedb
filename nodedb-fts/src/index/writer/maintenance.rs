// SPDX-License-Identifier: Apache-2.0

//! Segment publication and collection purging.

use super::{FtsIndex, memtable_collection_prefix, memtable_tenant_prefix};
use crate::{
    backend::FtsBackend,
    index::error::FtsIndexError,
    lsm::{compaction, segment::writer as seg_writer},
};
use std::sync::atomic::Ordering;
use tracing::debug;

impl<B: FtsBackend> FtsIndex<B> {
    /// Flush the active memtable to an immutable segment in the backend.
    ///
    /// Calling this before serializing the index guarantees that all posting data written since the last spill threshold is captured in the backend's segment storage rather than the in-memory memtable.  Callers that checkpoint the index (e.g., NodeDB-Lite flush) must call this once per active index before persisting.
    pub fn flush_memtable(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let drained = self.memtable.drain();
        if drained.is_empty() {
            return Ok(());
        }

        let segment_bytes = seg_writer::flush_to_segment(drained)?;
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let id = compaction::segment_id(seg_id, 0);
        self.backend
            .write_segment(database_id, tid, collection, &id, &segment_bytes)
            .map_err(FtsIndexError::backend)?;

        debug!(database_id, tid, %collection, seg_id, bytes = segment_bytes.len(), "flushed memtable to segment");
        Ok(())
    }

    /// Purge all entries for a collection. Returns count of removed entries.
    pub fn purge_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<usize, B::Error> {
        self.memtable
            .drain_collection(&memtable_collection_prefix(database_id, tid, collection));
        self.backend.purge_collection(database_id, tid, collection)
    }

    /// Purge all entries for a `(database_id, tenant)` across every collection.
    pub fn purge_tenant(&self, database_id: u64, tid: u64) -> Result<usize, B::Error> {
        self.memtable
            .drain_collection(&memtable_tenant_prefix(database_id, tid));
        self.backend.purge_tenant(database_id, tid)
    }
}

#[cfg(test)]
mod tests {
    use super::super::memtable_key;
    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::test_support::test_governor;
    use crate::{
        block::CompactPosting,
        lsm::memtable::{Memtable, MemtableConfig},
        posting::Bm25Params,
    };
    use nodedb_types::Surrogate;
    use std::sync::atomic::AtomicU64;
    const DB: u64 = 0;
    const T: u64 = 1;
    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new(), test_governor())
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
            &memtable_key(DB, T, "docs", &oversize_term),
            CompactPosting {
                doc_id: Surrogate(1),
                term_freq: 1,
                fieldnorm: 1,
                positions: vec![0],
            },
        );
        idx.memtable.record_doc(Surrogate(1), 1);

        let err = idx
            .flush_memtable(DB, T, "docs")
            .expect_err("flush must reject oversize term");
        let key_overhead = memtable_key(DB, T, "docs", "").len();
        match err {
            FtsIndexError::TermTooLong { len, max } => {
                assert_eq!(len, oversize_term.len() + key_overhead);
                assert_eq!(max, crate::lsm::segment::format::MAX_TERM_LEN);
            }
            other => panic!("expected TermTooLong, got {other:?}"),
        }
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
        let segments = idx.backend.list_segments(DB, T, "docs").unwrap();
        assert!(!segments.is_empty(), "segment should have been written");
    }
    #[test]
    fn purge_collection_preserves_others() {
        let idx = make_index();
        idx.index_document(DB, T, "col_a", Surrogate(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "col_b", Surrogate(1), "delta echo")
            .unwrap();

        idx.purge_collection(DB, T, "col_a").unwrap();
        assert_eq!(
            idx.backend.collection_stats(DB, T, "col_a").unwrap(),
            (0, 0)
        );
        assert!(idx.backend.collection_stats(DB, T, "col_b").unwrap().0 > 0);

        assert!(
            !idx.memtable
                .get_postings(&memtable_key(DB, T, "col_b", "delta"))
                .is_empty()
        );
        assert!(
            idx.memtable
                .get_postings(&memtable_key(DB, T, "col_a", "alpha"))
                .is_empty()
        );
    }
}
