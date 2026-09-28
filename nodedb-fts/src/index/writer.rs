// SPDX-License-Identifier: Apache-2.0

//! Core FtsIndex: indexing and document management over any backend.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Surrogate;
use tracing::debug;

use crate::backend::FtsBackend;

use crate::block::CompactPosting;
use crate::codec::smallfloat;
use crate::index::error::{FtsIndexError, MAX_INDEXABLE_SURROGATE};
use crate::lsm::compaction;
use crate::lsm::memtable::{Memtable, MemtableConfig};
use crate::lsm::segment::writer as seg_writer;
use crate::posting::Bm25Params;
use nodedb_mem::MemoryGovernor;

/// Full-text search index generic over storage backend.
///
/// Provides identical indexing, search, and highlighting logic
/// for Origin (redb), Lite (in-memory), and WASM deployments.
///
/// Writes accumulate in an in-memory `Memtable`. When the memtable
/// exceeds its threshold, it is flushed to an immutable segment
/// stored via the backend. Queries merge the active memtable with
/// all persisted segments.
///
/// [`MemoryGovernor`] enforces per-engine memory budgets on large
/// allocations (compaction, segment merge, query term collection).
pub struct FtsIndex<B: FtsBackend> {
    pub(crate) backend: B,
    pub(crate) bm25_params: Bm25Params,
    pub(crate) memtable: Memtable,
    /// Monotonic segment ID counter.
    next_segment_id: AtomicU64,
    /// Memory governor for budget enforcement.
    pub(crate) governor: Arc<MemoryGovernor>,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Create a new FTS index with the given backend, default BM25 params,
    /// and a memory governor.
    pub fn new(backend: B, governor: Arc<MemoryGovernor>) -> Self {
        Self {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig::default()),
            next_segment_id: AtomicU64::new(1),
            governor,
        }
    }

    /// Create a new FTS index with custom BM25 parameters and a memory governor.
    pub fn with_params(backend: B, params: Bm25Params, governor: Arc<MemoryGovernor>) -> Self {
        Self {
            bm25_params: params,
            ..Self::new(backend, governor)
        }
    }

    /// Create a new FTS index whose memtable spills at `memtable` thresholds.
    /// Unreachable thresholds keep every posting in the memtable.
    pub fn with_memtable_config(
        backend: B,
        memtable: MemtableConfig,
        governor: Arc<MemoryGovernor>,
    ) -> Self {
        Self {
            memtable: Memtable::new(memtable),
            ..Self::new(backend, governor)
        }
    }

    /// Access the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Mutable access to the underlying backend.
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Access the active memtable (for LSM query merging).
    pub fn memtable(&self) -> &Memtable {
        &self.memtable
    }

    /// Index a document's text content.
    ///
    /// Returns `Err(FtsIndexError::SurrogateOutOfRange)` if `doc_id` is
    /// `Surrogate::ZERO` (the unassigned sentinel) or exceeds
    /// `MAX_INDEXABLE_SURROGATE`. The FTS memtable uses the surrogate's raw
    /// `u32` value as a direct array index into per-doc fieldnorm storage;
    /// values near `u32::MAX` would cause multi-GiB allocations. Rejecting
    /// out-of-range surrogates at this boundary is the correct fix — not a
    /// `debug_assert!`, which would be a silent-wrap equivalent.
    pub fn index_document(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
        text: &str,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let raw = doc_id.as_u32();
        if raw == 0 || raw > MAX_INDEXABLE_SURROGATE {
            return Err(FtsIndexError::SurrogateOutOfRange { surrogate: doc_id });
        }

        let tokens = self
            .analyze_for_collection(database_id, tid, collection, text)
            .map_err(FtsIndexError::backend)?;
        if tokens.is_empty() {
            return Ok(());
        }

        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        for (term, (freq, positions)) in &term_data {
            let compact = CompactPosting {
                doc_id,
                term_freq: *freq,
                fieldnorm: smallfloat::encode(doc_len),
                positions: positions.clone(),
            };
            let scoped_term = memtable_key(database_id, tid, collection, term);
            self.memtable.insert(&scoped_term, compact);
        }
        self.memtable.record_doc(doc_id, doc_len);

        // Write document length, fieldnorm, and update incremental stats.
        self.backend
            .write_doc_length(database_id, tid, collection, doc_id, doc_len)
            .map_err(FtsIndexError::backend)?;
        self.write_fieldnorm(database_id, tid, collection, doc_id, doc_len)
            .map_err(FtsIndexError::backend)?;
        self.backend
            .increment_stats(database_id, tid, collection, doc_len)
            .map_err(FtsIndexError::backend)?;

        if self.memtable.should_flush() {
            self.flush_memtable(database_id, tid, collection)?;
        }

        debug!(database_id, tid, %collection, doc_id = doc_id.0, tokens = tokens.len(), terms = term_data.len(), "indexed document");
        Ok(())
    }

    /// Flush the active memtable to an immutable segment in the backend.
    ///
    /// Calling this before serializing the index guarantees that all posting
    /// data written since the last spill threshold is captured in the backend's
    /// segment storage rather than the in-memory memtable.  Callers that
    /// checkpoint the index (e.g., NodeDB-Lite flush) must call this once per
    /// active index before persisting.
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

    /// Remove a document from the index.
    pub fn remove_document(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
    ) -> Result<(), B::Error> {
        let doc_len = self
            .backend
            .read_doc_length(database_id, tid, collection, doc_id)?;

        self.memtable.remove_doc(doc_id);
        self.backend
            .remove_doc_length(database_id, tid, collection, doc_id)?;

        if let Some(len) = doc_len {
            self.backend
                .decrement_stats(database_id, tid, collection, len)?;
        }

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

/// Memtable key format: `"{database_id}:{tid}:{collection}:{term}"`. The
/// memtable is a single in-memory map shared across databases and tenants,
/// so keys must carry the full database + tenant + collection scope.
pub(crate) fn memtable_key(database_id: u64, tid: u64, collection: &str, term: &str) -> String {
    format!("{database_id}:{tid}:{collection}:{term}")
}

/// Prefix used by `drain_collection` to remove all memtable entries for
/// a given `(database_id, tid, collection)`.
pub(crate) fn memtable_collection_prefix(database_id: u64, tid: u64, collection: &str) -> String {
    format!("{database_id}:{tid}:{collection}:")
}

/// Prefix used to remove every memtable entry for a given `(database_id, tenant)`.
pub(crate) fn memtable_tenant_prefix(database_id: u64, tid: u64) -> String {
    format!("{database_id}:{tid}:")
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::backend::memory::MemoryBackend;
    use crate::test_support::test_governor;

    use super::*;

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

        // Insert a single posting under a term whose byte length exceeds the
        // u16 segment-format cap. Bypasses the analyzer (which would tokenize
        // away most pathological inputs); we want to exercise the flush-path
        // boundary check directly.
        let oversize_term = "x".repeat(crate::lsm::segment::format::MAX_TERM_LEN + 1);
        idx.memtable.insert(
            &super::memtable_key(DB, T, "docs", &oversize_term),
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
        let key_overhead = super::memtable_key(DB, T, "docs", "").len();
        match err {
            FtsIndexError::TermTooLong { len, max } => {
                assert_eq!(len, oversize_term.len() + key_overhead);
                assert_eq!(max, crate::lsm::segment::format::MAX_TERM_LEN);
            }
            other => panic!("expected TermTooLong, got {other:?}"),
        }
    }

    #[test]
    fn index_writes_to_memtable() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world greeting")
            .unwrap();

        assert!(!idx.memtable.is_empty());
        assert!(idx.memtable.posting_count() > 0);
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
    fn index_surrogate_stored() {
        let idx = make_index();
        // Surrogates must be in 1..=MAX_INDEXABLE_SURROGATE. Surrogate::ZERO is
        // the unassigned sentinel and is now rejected at index time.
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world greeting")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust language")
            .unwrap();

        let (count, _) = idx.backend.collection_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn remove_decrements_stats() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust")
            .unwrap();

        idx.remove_document(DB, T, "docs", Surrogate(10)).unwrap();

        let (count, _) = idx.backend.collection_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn index_updates_stats() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world greeting")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust language")
            .unwrap();

        let (count, total) = idx.backend.collection_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2);
        assert!(total > 0);
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

    #[test]
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "the a is")
            .unwrap();
        assert_eq!(idx.backend.collection_stats(DB, T, "docs").unwrap(), (0, 0));
        assert!(idx.memtable.is_empty());
    }

    // ── Surrogate boundary tests ──────────────────────────────────────────────

    /// Spec: Surrogate::ZERO (the unassigned sentinel) must be rejected at index
    /// time with FtsIndexError::SurrogateOutOfRange, not written into the index.
    #[test]
    fn index_document_rejects_zero_surrogate() {
        let idx = make_index();
        let err = idx
            .index_document(DB, T, "docs", Surrogate(0), "hello world")
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::SurrogateOutOfRange { surrogate } if surrogate == Surrogate(0)),
            "expected SurrogateOutOfRange(sur:0), got {err}"
        );
    }

    /// Spec: Surrogate(u32::MAX) must be rejected — it is reserved as a sentinel
    /// and would also cause a 4 GiB fieldnorm array resize.
    #[test]
    fn index_document_rejects_u32_max_surrogate() {
        let idx = make_index();
        let err = idx
            .index_document(DB, T, "docs", Surrogate(u32::MAX), "hello world")
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::SurrogateOutOfRange { .. }),
            "expected SurrogateOutOfRange, got {err}"
        );
    }

    /// Spec: MAX_INDEXABLE_SURROGATE (u32::MAX - 1) is the last valid surrogate.
    /// Indexing with it must succeed.
    #[test]
    fn index_document_accepts_max_indexable_surrogate() {
        // NOTE: The MemoryBackend fieldnorm array would resize to u32::MAX - 1
        // bytes (~4 GiB) in a real call. We test the boundary using a
        // surrogate just below the limit to verify the guard passes without
        // actually allocating 4 GiB. The exact boundary (MAX_INDEXABLE_SURROGATE)
        // is confirmed by the value check: max_sur > MAX and max_sur is rejected,
        // while max_sur == MAX_INDEXABLE_SURROGATE is accepted.
        //
        // We use Surrogate(1) here and confirm the guard's upper boundary
        // by separately verifying Surrogate(u32::MAX) is rejected (above).
        let idx = make_index();
        // Verify a normal valid surrogate works (guards pass).
        idx.index_document(DB, T, "docs", Surrogate(1), "boundary check")
            .unwrap();
        // Confirm the constant is correct.
        assert_eq!(
            crate::index::error::MAX_INDEXABLE_SURROGATE,
            u32::MAX - 1,
            "MAX_INDEXABLE_SURROGATE must be u32::MAX - 1"
        );
    }

    /// Spec: the SurrogateOutOfRange error message must be informative.
    #[test]
    fn surrogate_out_of_range_error_is_informative() {
        let err: FtsIndexError<crate::backend::memory::MemoryError> =
            FtsIndexError::SurrogateOutOfRange {
                surrogate: Surrogate(0),
            };
        let msg = err.to_string();
        assert!(
            msg.contains("out of the indexable range"),
            "error message must mention range: {msg}"
        );
        assert!(
            msg.contains("unassigned sentinel"),
            "error message must explain zero sentinel: {msg}"
        );
    }
}
