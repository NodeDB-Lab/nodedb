// SPDX-License-Identifier: Apache-2.0

//! Corpus statistics for BM25 scoring.

use crate::backend::FtsBackend;
use crate::index::FtsIndex;
use crate::scope::IndexScope;

impl<B: FtsBackend> FtsIndex<B> {
    /// Get total document count and average document length of one index.
    ///
    /// Returns `(total_docs, avg_doc_len)`. If the index is empty,
    /// returns `(0, 1.0)` to avoid division by zero.
    pub fn index_stats<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
    ) -> Result<(u32, f32), B::Error> {
        let (count, total_len) = self
            .backend
            .collection_stats(database_id, tid, index.into())?;
        let avg = if count > 0 {
            total_len as f32 / count as f32
        } else {
            1.0
        };
        Ok((count, avg))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::test_support::test_governor;
    use nodedb_types::Surrogate;

    const DB: u64 = 0;
    const T: u64 = 1;

    #[test]
    fn empty_collection_stats() {
        let idx: FtsIndex<MemoryBackend> = FtsIndex::new(MemoryBackend::new(), test_governor());
        let (count, avg) = idx.index_stats(DB, T, "empty").unwrap();
        assert_eq!(count, 0);
        assert!((avg - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn stats_after_indexing() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world greeting")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(2), "hello rust")
            .unwrap();

        let (count, avg) = idx.index_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2);
        assert!(avg > 0.0);
    }
}
