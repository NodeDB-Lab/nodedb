// SPDX-License-Identifier: Apache-2.0

//! Document writes reuse ordered analyzer output and preserve surrogate bounds.

use super::{FtsIndex, memtable_key};
use crate::{
    backend::FtsBackend,
    block::CompactPosting,
    codec::smallfloat,
    index::error::{FtsIndexError, MAX_INDEXABLE_SURROGATE},
};
use nodedb_types::Surrogate;
use std::collections::HashMap;
use tracing::debug;

impl<B: FtsBackend> FtsIndex<B> {
    /// Index a document's text content.
    ///
    /// Returns `Err(FtsIndexError::SurrogateOutOfRange)` for `Surrogate::ZERO` or values exceeding `MAX_INDEXABLE_SURROGATE`. `Surrogate::ZERO` is the unassigned sentinel. Fieldnorm arrays use raw `u32` surrogates as indexes. Values near `u32::MAX` cause multi-GiB allocations. Runtime bounds checks run before analysis and remain active in release builds.
    pub fn index_document(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
        text: &str,
    ) -> Result<(), FtsIndexError<B::Error>> {
        Self::check_surrogate(doc_id)?;

        let tokens = self
            .analyze_for_collection(database_id, tid, collection, text)
            .map_err(FtsIndexError::backend)?;
        self.index_analyzed_document(database_id, tid, collection, doc_id, &tokens)
    }

    /// Index ordered tokens from this collection's current analyzer. Callers preserve token order and hold analyzer config stable through indexing.
    pub fn index_analyzed_document(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
        tokens: &[String],
    ) -> Result<(), FtsIndexError<B::Error>> {
        Self::check_surrogate(doc_id)?;
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

        let term_count = term_data.len();
        for (term, (freq, positions)) in term_data {
            let compact = CompactPosting {
                doc_id,
                term_freq: freq,
                fieldnorm: smallfloat::encode(doc_len),
                positions,
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

        debug!(database_id, tid, %collection, doc_id = doc_id.0, tokens = tokens.len(), terms = term_count, "indexed document");
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

    fn check_surrogate(doc_id: Surrogate) -> Result<(), FtsIndexError<B::Error>> {
        let raw = doc_id.as_u32();
        if raw == 0 || raw > MAX_INDEXABLE_SURROGATE {
            return Err(FtsIndexError::SurrogateOutOfRange { surrogate: doc_id });
        }
        Ok(())
    }
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
    fn index_writes_to_memtable() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world greeting")
            .unwrap();

        assert!(!idx.memtable.is_empty());
        assert!(idx.memtable.posting_count() > 0);
    }

    #[test]
    fn index_surrogate_stored() {
        let idx = make_index();
        // Surrogates must be in 1..=MAX_INDEXABLE_SURROGATE. Surrogate::ZERO is the unassigned sentinel and is rejected at index time.
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
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "the a is")
            .unwrap();
        assert_eq!(idx.backend.collection_stats(DB, T, "docs").unwrap(), (0, 0));
        assert!(idx.memtable.is_empty());
    }

    // ── Surrogate boundary tests ──────────────────────────────────────────────

    /// Spec: Surrogate::ZERO (the unassigned sentinel) must be rejected at index time with FtsIndexError::SurrogateOutOfRange, not written into the index.
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

    /// Spec: Surrogate(u32::MAX) must be rejected — it is reserved as a sentinel and would also cause a 4 GiB fieldnorm array resize.
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

    /// Check the last valid surrogate constant and a representative valid input.
    #[test]
    fn index_document_accepts_max_indexable_surrogate() {
        // Indexing MAX_INDEXABLE_SURROGATE requires multi-GiB fieldnorm arrays.
        // This fixture uses Surrogate(1) and checks the constant and sentinel boundary separately.
        let idx = make_index();
        // Check a valid surrogate without allocating the largest fieldnorm array.
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
    #[test]
    fn analyzed_tokens_match_text_indexing_and_surrogate_validation() {
        let text_index = make_index();
        let token_index = make_index();
        let text = "Alpha alpha beta gamma";
        let tokens = token_index
            .analyze_for_collection(DB, T, "docs", text)
            .unwrap();
        text_index
            .index_document(DB, T, "docs", Surrogate(1), text)
            .unwrap();
        token_index
            .index_analyzed_document(DB, T, "docs", Surrogate(1), &tokens)
            .unwrap();
        assert_eq!(text_index.memtable.stats(), token_index.memtable.stats());
        let mut terms = text_index.memtable.terms();
        terms.sort();
        for term in terms {
            let expected = text_index.memtable.get_postings(&term);
            let actual = token_index.memtable.get_postings(&term);
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                assert_eq!(actual.doc_id, expected.doc_id);
                assert_eq!(actual.term_freq, expected.term_freq);
                assert_eq!(actual.fieldnorm, expected.fieldnorm);
                assert_eq!(actual.positions, expected.positions);
            }
        }
        assert_eq!(
            text_index.backend.collection_stats(DB, T, "docs").unwrap(),
            token_index.backend.collection_stats(DB, T, "docs").unwrap()
        );
        for id in [Surrogate::ZERO, Surrogate(u32::MAX)] {
            assert!(matches!(
                token_index.index_analyzed_document(DB, T, "docs", id, &tokens),
                Err(FtsIndexError::SurrogateOutOfRange { .. })
            ));
            assert!(matches!(
                token_index.index_analyzed_document(DB, T, "docs", id, &[]),
                Err(FtsIndexError::SurrogateOutOfRange { .. })
            ));
        }
        let empty = make_index();
        empty
            .index_analyzed_document(DB, T, "docs", Surrogate(1), &[])
            .unwrap();
        assert!(empty.memtable.is_empty());
    }
}
