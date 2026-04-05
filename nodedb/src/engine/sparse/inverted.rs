//! Full-text inverted index for Origin, backed by redb.
//!
//! Wraps `nodedb_fts::FtsIndex<RedbFtsBackend>` to provide persistent
//! full-text search with BM25 scoring. All scoring, tokenization, and
//! fuzzy logic lives in `nodedb-fts`; this module provides the Origin-specific
//! integration (redb backend, transaction support, tenant purge).

use std::sync::Arc;

use redb::{Database, ReadableTable, WriteTransaction};
use tracing::debug;

use nodedb_fts::index::FtsIndex;

pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};

use super::fts_redb::RedbFtsBackend;
use super::fts_redb::tables::{DOC_LENGTHS, INDEX_META, POSTINGS};

/// Full-text inverted index backed by redb via `nodedb-fts`.
pub struct InvertedIndex {
    inner: FtsIndex<RedbFtsBackend>,
}

impl InvertedIndex {
    /// Open or create an inverted index at the given redb database.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let backend = RedbFtsBackend::open(db)?;
        Ok(Self {
            inner: FtsIndex::new(backend),
        })
    }

    /// Purge all inverted index entries for a tenant.
    pub fn purge_tenant(&self, tenant_id: u32) -> crate::Result<usize> {
        let prefix = format!("{tenant_id}:");
        let end = format!("{tenant_id}:\u{ffff}");

        let db = self.inner.backend().db();
        let write_txn = db
            .begin_write()
            .map_err(|e| inverted_err("purge write txn", e))?;
        let mut removed = 0;

        {
            let mut postings = write_txn
                .open_table(POSTINGS)
                .map_err(|e| inverted_err("open postings", e))?;
            let keys: Vec<String> = postings
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| inverted_err("postings range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            removed += keys.len();
            for key in &keys {
                let _ = postings.remove(key.as_str());
            }
        }

        {
            let mut doc_lengths = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| inverted_err("open doc_lengths", e))?;
            let keys: Vec<String> = doc_lengths
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| inverted_err("doc_lengths range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            removed += keys.len();
            for key in &keys {
                let _ = doc_lengths.remove(key.as_str());
            }
        }

        write_txn
            .commit()
            .map_err(|e| inverted_err("commit purge", e))?;
        Ok(removed)
    }

    /// Index a document's text content.
    pub fn index_document(&self, collection: &str, doc_id: &str, text: &str) -> crate::Result<()> {
        let tokens = nodedb_fts::analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_index_data(&write_txn, collection, doc_id, &tokens)?;
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit index", e))?;
        Ok(())
    }

    /// Index a document within an externally-owned write transaction.
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = nodedb_fts::analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }
        self.write_index_data(txn, collection, doc_id, &tokens)
    }

    /// Core indexing logic: writes postings and doc length within a transaction.
    fn write_index_data(
        &self,
        txn: &WriteTransaction,
        collection: &str,
        doc_id: &str,
        tokens: &[String],
    ) -> crate::Result<()> {
        use std::collections::HashMap;

        let mut term_postings: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_postings
                .entry(token.as_str())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let scoped_doc_id = format!("{collection}:{doc_id}");
        let doc_len = tokens.len() as u32;

        let mut postings_table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        for (term, (freq, positions)) in &term_postings {
            let term_key = format!("{collection}:{term}");
            let posting = Posting {
                doc_id: scoped_doc_id.clone(),
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing: Vec<Posting> = postings_table
                .get(term_key.as_str())
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack(v.value()).ok())
                .unwrap_or_default();

            existing.retain(|p| p.doc_id != scoped_doc_id);
            existing.push(posting);

            let bytes = zerompk::to_msgpack_vec(&existing)
                .map_err(|e| inverted_err("serialize postings", e))?;
            postings_table
                .insert(term_key.as_str(), bytes.as_slice())
                .map_err(|e| inverted_err("insert posting", e))?;
        }
        drop(postings_table);

        let mut lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("open doc_lengths", e))?;
        let len_bytes =
            zerompk::to_msgpack_vec(&doc_len).map_err(|e| inverted_err("serialize doc_len", e))?;
        lengths
            .insert(scoped_doc_id.as_str(), len_bytes.as_slice())
            .map_err(|e| inverted_err("insert doc_len", e))?;

        // Update incremental stats in the same transaction.
        drop(lengths);
        Self::update_stats_in_txn(txn, collection, doc_len as i64)?;

        debug!(%collection, %doc_id, tokens = tokens.len(), terms = term_postings.len(), "indexed document");
        Ok(())
    }

    /// Atomically update `(doc_count, total_token_sum)` in INDEX_META within a transaction.
    /// `delta` is positive for insert, negative for remove.
    fn update_stats_in_txn(
        txn: &WriteTransaction,
        collection: &str,
        delta: i64,
    ) -> crate::Result<()> {
        let meta_key = format!("stats:{collection}");
        let mut meta = txn
            .open_table(INDEX_META)
            .map_err(|e| inverted_err("open index_meta", e))?;
        let (mut count, mut total) = meta
            .get(meta_key.as_str())
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<(u32, u64)>(v.value()).ok())
            .unwrap_or((0, 0));

        if delta > 0 {
            count += 1;
            total += delta as u64;
        } else {
            count = count.saturating_sub(1);
            total = total.saturating_sub((-delta) as u64);
        }

        let bytes = zerompk::to_msgpack_vec(&(count, total))
            .map_err(|e| inverted_err("serialize stats", e))?;
        meta.insert(meta_key.as_str(), bytes.as_slice())
            .map_err(|e| inverted_err("insert stats", e))?;
        Ok(())
    }

    /// Remove a document from the inverted index.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> crate::Result<()> {
        let scoped_doc_id = format!("{collection}:{doc_id}");

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        {
            let mut postings_table = write_txn
                .open_table(POSTINGS)
                .map_err(|e| inverted_err("open postings", e))?;

            let prefix = format!("{collection}:");
            let end = format!("{collection}:\u{ffff}");
            let keys: Vec<String> = postings_table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| inverted_err("range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();

            let mut updates: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for key in &keys {
                if let Ok(Some(val)) = postings_table.get(key.as_str()) {
                    let mut list: Vec<Posting> =
                        zerompk::from_msgpack(val.value()).unwrap_or_default();
                    let before = list.len();
                    list.retain(|p| p.doc_id != scoped_doc_id);
                    if list.len() != before {
                        if list.is_empty() {
                            updates.push((key.clone(), None));
                        } else {
                            let bytes = zerompk::to_msgpack_vec(&list).unwrap_or_default();
                            updates.push((key.clone(), Some(bytes)));
                        }
                    }
                }
            }

            for (key, new_val) in &updates {
                match new_val {
                    None => {
                        let _ = postings_table.remove(key.as_str());
                    }
                    Some(bytes) => {
                        let _ = postings_table.insert(key.as_str(), bytes.as_slice());
                    }
                }
            }

            let mut lengths = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| inverted_err("open doc_lengths", e))?;

            // Read doc length before removing (for stats decrement).
            let old_len = lengths
                .get(scoped_doc_id.as_str())
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack::<u32>(v.value()).ok())
                .unwrap_or(0);

            let _ = lengths.remove(scoped_doc_id.as_str());
            drop(lengths);

            // Decrement stats in the same transaction.
            if old_len > 0 {
                Self::update_stats_in_txn(&write_txn, collection, -(old_len as i64))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit remove", e))?;

        Ok(())
    }

    /// Search the inverted index using BM25 scoring.
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner.search(collection, query, top_k, fuzzy_enabled)
    }

    /// Search with explicit boolean mode (AND or OR).
    pub fn search_with_mode(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner
            .search_with_mode(collection, query, top_k, fuzzy_enabled, mode)
    }

    /// Generate highlighted text with matched query terms wrapped in tags.
    pub fn highlight(&self, text: &str, query: &str, prefix: &str, suffix: &str) -> String {
        self.inner.highlight(text, query, prefix, suffix)
    }

    /// Return byte offsets of matched query terms in the original text.
    pub fn offsets(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        self.inner.offsets(text, query)
    }
}

fn inverted_err(ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("{ctx}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(Database::create(&path).unwrap());
        let idx = InvertedIndex::open(db).unwrap();
        (idx, dir)
    }

    #[test]
    fn index_and_search() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "The quick brown fox jumps over the lazy dog")
            .unwrap();
        idx.index_document("docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document("docs", "d3", "Rust programming language for systems")
            .unwrap();

        let results = idx.search("docs", "brown fox", 10, false).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "docs:d1");
    }

    #[test]
    fn search_with_stemming() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document("docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search("docs", "database distribution", 10, false)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "docs:d1");
    }

    #[test]
    fn fuzzy_search() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search("docs", "databse", 10, true).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn remove_document() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "hello world").unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        idx.remove_document("docs", "d1").unwrap();

        let results = idx.search("docs", "hello", 10, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "docs:d2");
    }

    #[test]
    fn empty_query() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "some text here").unwrap();

        let results = idx.search("docs", "the a is", 10, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let (idx, _dir) = open_temp();
        idx.index_document("col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document("col_b", "d1", "delta echo foxtrot")
            .unwrap();

        let results = idx.search("col_a", "alpha", 10, false).unwrap();
        assert_eq!(results.len(), 1);

        let results = idx.search("col_b", "alpha", 10, false).unwrap();
        assert!(results.is_empty());
    }
}
