//! Full-text inverted index for Origin, backed by redb.
//!
//! Wraps `nodedb_fts::FtsIndex<RedbFtsBackend>` to provide persistent
//! full-text search with BM25 scoring. All scoring, tokenization, and
//! fuzzy logic lives in `nodedb-fts`; this module adds Origin-specific
//! features: transaction-participating indexing and structural tenant
//! purge that bypass the LSM memtable.
//!
//! The public API takes `TenantId` as a first-class parameter. Every
//! persistent redb table is keyed by the structural tuple
//! `(tenant_id, collection, …)` — per-tenant drops are a tuple range scan,
//! not a lexical-prefix scan.

use std::sync::Arc;

use redb::{Database, ReadableTable, WriteTransaction};
use tracing::debug;

use nodedb_fts::index::FtsIndex;
use nodedb_types::TenantId;

pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};

use super::fts_redb::RedbFtsBackend;
use super::fts_redb::tables::{DOC_LENGTHS, INDEX_META, POSTINGS, STATS};

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

    /// Purge all inverted index entries for a tenant. Structural drop via
    /// tuple ranges on every FTS table.
    pub fn purge_tenant(&self, tid: TenantId) -> crate::Result<usize> {
        self.inner
            .purge_tenant(tid.as_u32())
            .map_err(into_result_err)
    }

    /// Purge all inverted index entries for a single `(tenant, collection)`.
    /// Structural drop via tuple ranges on every FTS table.
    pub fn purge_collection(&self, tid: TenantId, collection: &str) -> crate::Result<usize> {
        self.inner
            .purge_collection(tid.as_u32(), collection)
            .map_err(into_result_err)
    }

    /// Index a document's text content.
    pub fn index_document(
        &self,
        tid: TenantId,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = nodedb_fts::analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_index_data(&write_txn, tid, collection, doc_id, &tokens)?;
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit index", e))?;
        Ok(())
    }

    /// Index a document within an externally-owned write transaction.
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        tid: TenantId,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = nodedb_fts::analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }
        self.write_index_data(txn, tid, collection, doc_id, &tokens)
    }

    /// Core indexing logic: writes postings, doc length, and stats within
    /// a transaction. Bypasses the LSM memtable so Origin transactions can
    /// stay atomic with the document write.
    fn write_index_data(
        &self,
        txn: &WriteTransaction,
        tid: TenantId,
        collection: &str,
        doc_id: &str,
        tokens: &[String],
    ) -> crate::Result<()> {
        use std::collections::HashMap;

        let t = tid.as_u32();

        let mut term_postings: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_postings
                .entry(token.as_str())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        let mut postings_table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        for (term, (freq, positions)) in &term_postings {
            let posting = Posting {
                doc_id: doc_id.to_string(),
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing: Vec<Posting> = postings_table
                .get((t, collection, *term))
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack(v.value()).ok())
                .unwrap_or_default();

            existing.retain(|p| p.doc_id != doc_id);
            existing.push(posting);

            let bytes = zerompk::to_msgpack_vec(&existing)
                .map_err(|e| inverted_err("serialize postings", e))?;
            postings_table
                .insert((t, collection, *term), bytes.as_slice())
                .map_err(|e| inverted_err("insert posting", e))?;
        }
        drop(postings_table);

        let mut lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("open doc_lengths", e))?;
        let len_bytes =
            zerompk::to_msgpack_vec(&doc_len).map_err(|e| inverted_err("serialize doc_len", e))?;
        lengths
            .insert((t, collection, doc_id), len_bytes.as_slice())
            .map_err(|e| inverted_err("insert doc_len", e))?;
        drop(lengths);

        Self::update_stats_in_txn(txn, tid, collection, doc_len as i64)?;

        debug!(tid = t, %collection, %doc_id, tokens = tokens.len(), terms = term_postings.len(), "indexed document");
        Ok(())
    }

    /// Atomically update `(doc_count, total_token_sum)` in STATS.
    fn update_stats_in_txn(
        txn: &WriteTransaction,
        tid: TenantId,
        collection: &str,
        delta: i64,
    ) -> crate::Result<()> {
        let t = tid.as_u32();
        let mut stats = txn
            .open_table(STATS)
            .map_err(|e| inverted_err("open stats", e))?;
        let (mut count, mut total) = stats
            .get((t, collection))
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
        stats
            .insert((t, collection), bytes.as_slice())
            .map_err(|e| inverted_err("insert stats", e))?;
        Ok(())
    }

    /// Remove a document from the inverted index.
    pub fn remove_document(
        &self,
        tid: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<()> {
        let t = tid.as_u32();

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        {
            let mut postings_table = write_txn
                .open_table(POSTINGS)
                .map_err(|e| inverted_err("open postings", e))?;

            let terms: Vec<String> = postings_table
                .range((t, collection, "")..=(t, collection, "\u{10ffff}"))
                .map_err(|e| inverted_err("range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
                .collect();

            let mut updates: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for term in &terms {
                if let Ok(Some(val)) = postings_table.get((t, collection, term.as_str())) {
                    let mut list: Vec<Posting> =
                        zerompk::from_msgpack(val.value()).unwrap_or_default();
                    let before = list.len();
                    list.retain(|p| p.doc_id != doc_id);
                    if list.len() != before {
                        if list.is_empty() {
                            updates.push((term.clone(), None));
                        } else {
                            let bytes = zerompk::to_msgpack_vec(&list).unwrap_or_default();
                            updates.push((term.clone(), Some(bytes)));
                        }
                    }
                }
            }

            for (term, new_val) in &updates {
                match new_val {
                    None => {
                        let _ = postings_table.remove((t, collection, term.as_str()));
                    }
                    Some(bytes) => {
                        let _ =
                            postings_table.insert((t, collection, term.as_str()), bytes.as_slice());
                    }
                }
            }

            let mut lengths = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| inverted_err("open doc_lengths", e))?;

            let old_len = lengths
                .get((t, collection, doc_id))
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack::<u32>(v.value()).ok())
                .unwrap_or(0);

            let _ = lengths.remove((t, collection, doc_id));
            drop(lengths);

            if old_len > 0 {
                Self::update_stats_in_txn(&write_txn, tid, collection, -(old_len as i64))?;
            }

            // Tombstone in docmap so future searches don't surface the removed doc.
            let mut meta = write_txn
                .open_table(INDEX_META)
                .map_err(|e| inverted_err("open index_meta", e))?;
            let docmap_bytes = meta
                .get((t, collection, "docmap"))
                .ok()
                .flatten()
                .map(|v| v.value().to_vec());
            if let Some(bytes) = docmap_bytes
                && let Some(mut map) = nodedb_fts::DocIdMap::from_bytes(&bytes)
            {
                map.remove(doc_id);
                let _ = meta.insert((t, collection, "docmap"), map.to_bytes().as_slice());
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
        tid: TenantId,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner.search(
            tid.as_u32(),
            collection,
            query,
            top_k,
            fuzzy_enabled,
            prefilter,
        )
    }

    /// Search with explicit boolean mode (AND or OR).
    #[allow(clippy::too_many_arguments)]
    pub fn search_with_mode(
        &self,
        tid: TenantId,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner.search_with_mode(
            tid.as_u32(),
            collection,
            query,
            top_k,
            fuzzy_enabled,
            mode,
            prefilter,
        )
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

fn into_result_err(e: crate::Error) -> crate::Error {
    e
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: TenantId = TenantId::new(1);

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
        idx.index_document(
            T,
            "docs",
            "d1",
            "The quick brown fox jumps over the lazy dog",
        )
        .unwrap();
        idx.index_document(T, "docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document(T, "docs", "d3", "Rust programming language for systems")
            .unwrap();

        let results = idx.search(T, "docs", "brown fox", 10, false, None).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn search_with_stemming() {
        let (idx, _dir) = open_temp();
        idx.index_document(T, "docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document(T, "docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search(T, "docs", "database distribution", 10, false, None)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn fuzzy_search() {
        let (idx, _dir) = open_temp();
        idx.index_document(T, "docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search(T, "docs", "databse", 10, true, None).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn remove_document() {
        let (idx, _dir) = open_temp();
        idx.index_document(T, "docs", "d1", "hello world").unwrap();
        idx.index_document(T, "docs", "d2", "hello rust").unwrap();

        idx.remove_document(T, "docs", "d1").unwrap();

        let results = idx.search(T, "docs", "hello", 10, false, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d2");
    }

    #[test]
    fn empty_query() {
        let (idx, _dir) = open_temp();
        idx.index_document(T, "docs", "d1", "some text here")
            .unwrap();

        let results = idx.search(T, "docs", "the a is", 10, false, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let (idx, _dir) = open_temp();
        idx.index_document(T, "col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document(T, "col_b", "d1", "delta echo foxtrot")
            .unwrap();

        let results = idx.search(T, "col_a", "alpha", 10, false, None).unwrap();
        assert_eq!(results.len(), 1);

        let results = idx.search(T, "col_b", "alpha", 10, false, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn purge_tenant_structurally_drops_data() {
        let (idx, _dir) = open_temp();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        idx.index_document(t1, "docs", "d1", "alpha bravo").unwrap();
        idx.index_document(t2, "docs", "d1", "alpha bravo").unwrap();

        idx.purge_tenant(t1).unwrap();

        assert!(
            idx.search(t1, "docs", "alpha", 10, false, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            !idx.search(t2, "docs", "alpha", 10, false, None)
                .unwrap()
                .is_empty()
        );
    }
}
