//! Core FtsIndex: indexing and document management over any backend.

use std::collections::HashMap;

use tracing::debug;

use crate::analyzer::pipeline::analyze;
use crate::backend::FtsBackend;
use crate::posting::{Bm25Params, Posting};

/// Full-text search index generic over storage backend.
///
/// Provides identical indexing, search, and highlighting logic
/// for Origin (redb), Lite (in-memory), and WASM deployments.
pub struct FtsIndex<B: FtsBackend> {
    pub(crate) backend: B,
    pub(crate) bm25_params: Bm25Params,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Create a new FTS index with the given backend and default BM25 params.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            bm25_params: Bm25Params::default(),
        }
    }

    /// Create a new FTS index with custom BM25 parameters.
    pub fn with_params(backend: B, params: Bm25Params) -> Self {
        Self {
            backend,
            bm25_params: params,
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

    /// Index a document's text content.
    ///
    /// Analyzes `text` into tokens, builds a posting list per term,
    /// and stores via the backend. If the document already exists,
    /// call `remove_document` first to avoid duplicate postings.
    pub fn index_document(
        &self,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), B::Error> {
        let tokens = analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }

        // Build per-term frequency and position data.
        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        // Write postings for each term.
        for (term, (freq, positions)) in &term_data {
            let posting = Posting {
                doc_id: doc_id.to_string(),
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing = self.backend.read_postings(collection, term)?;
            existing.retain(|p| p.doc_id != doc_id);
            existing.push(posting);

            self.backend.write_postings(collection, term, &existing)?;
        }

        // Write document length and update incremental stats.
        self.backend.write_doc_length(collection, doc_id, doc_len)?;
        self.backend.increment_stats(collection, doc_len)?;

        debug!(%collection, %doc_id, tokens = tokens.len(), terms = term_data.len(), "indexed document");
        Ok(())
    }

    /// Remove a document from the index.
    ///
    /// Scans all terms in the collection and removes the document's postings.
    /// Also removes the document length entry and decrements stats.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> Result<(), B::Error> {
        // Read doc length before removing (needed for stats decrement).
        let doc_len = self.backend.read_doc_length(collection, doc_id)?;

        // Get all terms in the collection and remove this doc from each.
        let terms = self.backend.collection_terms(collection)?;

        for term in &terms {
            let mut postings = self.backend.read_postings(collection, term)?;
            let before = postings.len();
            postings.retain(|p| p.doc_id != doc_id);
            if postings.len() != before {
                if postings.is_empty() {
                    self.backend.remove_postings(collection, term)?;
                } else {
                    self.backend.write_postings(collection, term, &postings)?;
                }
            }
        }

        self.backend.remove_doc_length(collection, doc_id)?;

        // Decrement incremental stats.
        if let Some(len) = doc_len {
            self.backend.decrement_stats(collection, len)?;
        }

        Ok(())
    }

    /// Purge all entries for a collection. Returns count of removed entries.
    pub fn purge_collection(&self, collection: &str) -> Result<usize, B::Error> {
        self.backend.purge_collection(collection)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;

    use super::*;

    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new())
    }

    #[test]
    fn index_updates_stats() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document("docs", "d2", "hello rust language")
            .unwrap();

        let (count, total) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count, 2);
        assert!(total > 0);
    }

    #[test]
    fn remove_decrements_stats() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world").unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        let (count_before, total_before) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count_before, 2);

        idx.remove_document("docs", "d1").unwrap();

        let (count_after, total_after) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count_after, 1);
        assert!(total_after < total_before);

        // Verify d1's postings are gone.
        let postings = idx.backend.read_postings("docs", "hello").unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].doc_id, "d2");
    }

    #[test]
    fn purge_collection() {
        let idx = make_index();
        idx.index_document("col_a", "d1", "alpha bravo").unwrap();
        idx.index_document("col_b", "d1", "delta echo").unwrap();

        let removed = idx.purge_collection("col_a").unwrap();
        assert!(removed > 0);
        assert_eq!(idx.backend.collection_stats("col_a").unwrap(), (0, 0));
        assert!(idx.backend.collection_stats("col_b").unwrap().0 > 0);
    }

    #[test]
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document("docs", "d1", "the a is").unwrap();
        assert_eq!(idx.backend.collection_stats("docs").unwrap(), (0, 0));
    }

    #[test]
    fn stats_o1_lookup() {
        let idx = make_index();
        // Index many documents.
        for i in 0..100 {
            idx.index_document("docs", &format!("d{i}"), &format!("word{i} common term"))
                .unwrap();
        }
        let (count, _) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count, 100);

        // Remove half.
        for i in 0..50 {
            idx.remove_document("docs", &format!("d{i}")).unwrap();
        }
        let (count, _) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count, 50);
    }
}
