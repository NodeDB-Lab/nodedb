// SPDX-License-Identifier: Apache-2.0

use nodedb_types::Surrogate;

use crate::posting::Posting;
use crate::scope::IndexScope;

/// Storage backend abstraction for the full-text search engine.
///
/// Origin implements this with redb (persistent). Lite implements with
/// in-memory HashMap. All scoring, BMW, compression, and analysis logic
/// works identically over any backend.
///
/// Every tenant-partitioned method takes `database_id: u64` and `tid: u64`
/// as first-class parameters. Backends are required to isolate databases
/// and tenants structurally — no boundary may depend on lexical-prefix
/// ordering of a composed string key.
///
/// Per-index methods take an [`IndexScope`]: a collection's whole-document
/// index or one of its field indexes. Backends key every per-index entry by
/// both the collection and the scope's field key, so no two scopes share
/// postings, lengths, stats, metadata, or segments. Collection-level
/// configuration (analyzer, language, fuzzy) lives in the metadata of
/// `IndexScope::document(collection)`.
///
/// Write methods take `&self` (not `&mut self`) because:
/// - Redb provides transactional isolation internally — concurrent writes
///   are safe through redb's MVCC.
/// - MemoryBackend uses interior mutability (`RefCell`) to match the same
///   trait signature, keeping the trait uniform.
pub trait FtsBackend {
    /// Error type for backend operations.
    type Error: std::fmt::Display;

    /// Read the posting list for a term in an index.
    fn read_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
    ) -> Result<Vec<Posting>, Self::Error>;

    /// Write/replace the posting list for a term in an index.
    fn write_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error>;

    /// Remove a term's posting list entirely.
    fn remove_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
    ) -> Result<(), Self::Error>;

    /// Read the document length (token count) of a document in an index.
    fn read_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
    ) -> Result<Option<u32>, Self::Error>;

    /// Read the document lengths of `doc_ids` in an index, parallel to
    /// `doc_ids`. A backend with transactions reads them in one.
    fn read_doc_lengths(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_ids: &[Surrogate],
    ) -> Result<Vec<Option<u32>>, Self::Error> {
        doc_ids
            .iter()
            .map(|doc_id| self.read_doc_length(database_id, tid, index, *doc_id))
            .collect()
    }

    /// Write/replace the document length of a document in an index.
    fn write_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
        length: u32,
    ) -> Result<(), Self::Error>;

    /// Remove a document's length entry from an index.
    fn remove_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
    ) -> Result<(), Self::Error>;

    /// Get all term names in an index (for fuzzy matching).
    fn collection_terms(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<Vec<String>, Self::Error>;

    /// Get total document count and sum of all document lengths of an index.
    /// Returns `(doc_count, total_token_sum)`.
    ///
    /// Implementations should maintain these incrementally for O(1) lookup.
    fn collection_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<(u32, u64), Self::Error>;

    /// Increment index stats after indexing a document.
    /// `doc_len` is the number of tokens in the newly indexed document.
    fn increment_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_len: u32,
    ) -> Result<(), Self::Error>;

    /// Decrement index stats after removing a document.
    /// `doc_len` is the token count of the removed document.
    fn decrement_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_len: u32,
    ) -> Result<(), Self::Error>;

    /// Read a metadata blob by sub-key (e.g., "docmap", "fieldnorms",
    /// "analyzer", "language").
    fn read_meta(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        subkey: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Write a metadata blob by sub-key.
    fn write_meta(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        subkey: &str,
        value: &[u8],
    ) -> Result<(), Self::Error>;

    /// Write a segment blob. `segment_id` is a stable per-index
    /// identifier (e.g., `"L{level}:{id:016x}"`).
    fn write_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Read a segment blob. Returns None if not found.
    fn read_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// List all segment ids of an index.
    fn list_segments(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<Vec<String>, Self::Error>;

    /// Remove a segment blob.
    fn remove_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
    ) -> Result<(), Self::Error>;

    /// Remove all entries of every index of a collection. Returns count of
    /// removed entries.
    fn purge_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<usize, Self::Error>;

    /// Remove all entries for a `(database_id, tid)` across every collection.
    /// Returns count of removed entries. Implementations MUST use a structural
    /// drop (e.g., tuple range `(db, tid, ..)..(db, tid+1, ..)`) rather than a
    /// lexical-prefix scan.
    fn purge_tenant(&self, database_id: u64, tid: u64) -> Result<usize, Self::Error>;
}
