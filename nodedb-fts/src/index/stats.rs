//! Corpus statistics for BM25 scoring.

use crate::backend::FtsBackend;
use crate::index::FtsIndex;

impl<B: FtsBackend> FtsIndex<B> {
    /// Get total document count and average document length for a collection.
    ///
    /// Returns `(total_docs, avg_doc_len)`. If the collection is empty,
    /// returns `(0, 1.0)` to avoid division by zero.
    pub fn index_stats(&self, collection: &str) -> Result<(u32, f32), B::Error> {
        let (count, total_len) = self.backend.collection_stats(collection)?;
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

    #[test]
    fn empty_collection_stats() {
        let idx: FtsIndex<MemoryBackend> = FtsIndex::new(MemoryBackend::new());
        let (count, avg) = idx.index_stats("empty").unwrap();
        assert_eq!(count, 0);
        assert!((avg - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn stats_after_indexing() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        let (count, avg) = idx.index_stats("docs").unwrap();
        assert_eq!(count, 2);
        assert!(avg > 0.0);
    }
}
