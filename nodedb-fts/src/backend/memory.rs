// SPDX-License-Identifier: Apache-2.0

//! In-memory FTS backend for Lite and WASM deployments.
//!
//! All data lives in HashMaps behind `RefCell` for interior mutability,
//! matching the `&self` trait signature. Rebuilt from documents on cold
//! start — acceptable for edge-scale datasets.
//!
//! Keys are fully structural tuples `(database_id, tid, collection, field, …)`
//! — database, tenant, and index isolation never depends on lexical-prefix
//! ordering.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use nodedb_types::Surrogate;

use crate::backend::FtsBackend;
use crate::posting::Posting;
use crate::scope::IndexScope;

/// In-memory backend error (infallible in practice, but trait requires it).
#[derive(Debug)]
pub struct MemoryError(String);

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "memory backend: {}", self.0)
    }
}

/// `(database_id, tid, collection, field)`: one index.
type IndexKey = (u64, u64, String, String);
/// An index key plus a term, meta subkey, or segment id.
type SubKey = (IndexKey, String);
/// An index key plus a document.
type DocLenKey = (IndexKey, Surrogate);
/// One term's postings keyed by document, so one document's posting is
/// replaced or removed without touching the rest of the list.
type PostingList = BTreeMap<Surrogate, Posting>;

/// In-memory FTS backend backed by HashMaps keyed by
/// `(database_id, tid, collection, field, …)` tuples.
///
/// Uses `RefCell` for interior mutability so the `FtsBackend` trait
/// can use `&self` uniformly (redb has its own transactional isolation).
#[derive(Debug, Default)]
pub struct MemoryBackend {
    /// `(index, term) → posting list`, ordered by document.
    postings: RefCell<HashMap<SubKey, PostingList>>,
    /// `(index, doc_id) → token count`.
    doc_lengths: RefCell<HashMap<DocLenKey, u32>>,
    /// `index → (doc_count, total_token_sum)`.
    stats: RefCell<HashMap<IndexKey, (u32, u64)>>,
    /// `(index, subkey) → blob` for fieldnorms, analyzer, language.
    meta: RefCell<HashMap<SubKey, Vec<u8>>>,
    /// `(index, segment_id) → compressed segment bytes`.
    segments: RefCell<HashMap<SubKey, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

fn index_key(database_id: u64, tid: u64, index: IndexScope<'_>) -> IndexKey {
    (
        database_id,
        tid,
        index.collection().to_string(),
        index.field_key().to_string(),
    )
}

fn sub_key(database_id: u64, tid: u64, index: IndexScope<'_>, sub: &str) -> SubKey {
    (index_key(database_id, tid, index), sub.to_string())
}

fn doc_len_key(database_id: u64, tid: u64, index: IndexScope<'_>, doc_id: Surrogate) -> DocLenKey {
    (index_key(database_id, tid, index), doc_id)
}

/// Whether `key` belongs to `(database_id, tid)`, and to `collection` when given.
fn owned_by(key: &IndexKey, database_id: u64, tid: u64, collection: Option<&str>) -> bool {
    key.0 == database_id && key.1 == tid && collection.is_none_or(|c| key.2 == c)
}

impl MemoryBackend {
    /// Drop every entry owned by `(database_id, tid[, collection])`.
    fn purge_owned(&self, database_id: u64, tid: u64, collection: Option<&str>) -> usize {
        let mut postings = self.postings.borrow_mut();
        let mut doc_lengths = self.doc_lengths.borrow_mut();
        let before = postings.len() + doc_lengths.len();
        postings.retain(|k, _| !owned_by(&k.0, database_id, tid, collection));
        doc_lengths.retain(|k, _| !owned_by(&k.0, database_id, tid, collection));
        self.stats
            .borrow_mut()
            .retain(|k, _| !owned_by(k, database_id, tid, collection));
        self.meta
            .borrow_mut()
            .retain(|k, _| !owned_by(&k.0, database_id, tid, collection));
        self.segments
            .borrow_mut()
            .retain(|k, _| !owned_by(&k.0, database_id, tid, collection));
        let after = postings.len() + doc_lengths.len();
        before - after
    }
}

impl FtsBackend for MemoryBackend {
    type Error = MemoryError;

    fn read_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
    ) -> Result<Vec<Posting>, Self::Error> {
        Ok(self
            .postings
            .borrow()
            .get(&sub_key(database_id, tid, index, term))
            .map(|list| list.values().cloned().collect())
            .unwrap_or_default())
    }

    fn write_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error> {
        let key = sub_key(database_id, tid, index, term);
        let mut map = self.postings.borrow_mut();
        if postings.is_empty() {
            map.remove(&key);
        } else {
            let list: PostingList = postings.iter().map(|p| (p.doc_id, p.clone())).collect();
            map.insert(key, list);
        }
        Ok(())
    }

    fn upsert_posting(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
        posting: &Posting,
    ) -> Result<(), Self::Error> {
        self.postings
            .borrow_mut()
            .entry(sub_key(database_id, tid, index, term))
            .or_default()
            .insert(posting.doc_id, posting.clone());
        Ok(())
    }

    fn remove_posting(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
        doc_id: Surrogate,
    ) -> Result<(), Self::Error> {
        let key = sub_key(database_id, tid, index, term);
        let mut map = self.postings.borrow_mut();
        let emptied = match map.get_mut(&key) {
            Some(list) => {
                list.remove(&doc_id);
                list.is_empty()
            }
            None => false,
        };
        if emptied {
            map.remove(&key);
        }
        Ok(())
    }

    fn remove_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
    ) -> Result<(), Self::Error> {
        self.postings
            .borrow_mut()
            .remove(&sub_key(database_id, tid, index, term));
        Ok(())
    }

    fn read_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
    ) -> Result<Option<u32>, Self::Error> {
        Ok(self
            .doc_lengths
            .borrow()
            .get(&doc_len_key(database_id, tid, index, doc_id))
            .copied())
    }

    fn write_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
        length: u32,
    ) -> Result<(), Self::Error> {
        self.doc_lengths
            .borrow_mut()
            .insert(doc_len_key(database_id, tid, index, doc_id), length);
        Ok(())
    }

    fn remove_doc_length(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
    ) -> Result<(), Self::Error> {
        self.doc_lengths
            .borrow_mut()
            .remove(&doc_len_key(database_id, tid, index, doc_id));
        Ok(())
    }

    fn collection_terms(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<Vec<String>, Self::Error> {
        let key = index_key(database_id, tid, index);
        Ok(self
            .postings
            .borrow()
            .keys()
            .filter(|(k, _)| *k == key)
            .map(|(_, term)| term.clone())
            .collect())
    }

    fn collection_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<(u32, u64), Self::Error> {
        Ok(self
            .stats
            .borrow()
            .get(&index_key(database_id, tid, index))
            .copied()
            .unwrap_or((0, 0)))
    }

    fn increment_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_len: u32,
    ) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats
            .entry(index_key(database_id, tid, index))
            .or_insert((0, 0));
        entry.0 += 1;
        entry.1 += doc_len as u64;
        Ok(())
    }

    fn decrement_stats(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_len: u32,
    ) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats
            .entry(index_key(database_id, tid, index))
            .or_insert((0, 0));
        entry.0 = entry.0.saturating_sub(1);
        entry.1 = entry.1.saturating_sub(doc_len as u64);
        Ok(())
    }

    fn read_meta(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        subkey: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .meta
            .borrow()
            .get(&sub_key(database_id, tid, index, subkey))
            .cloned())
    }

    fn write_meta(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        subkey: &str,
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.meta
            .borrow_mut()
            .insert(sub_key(database_id, tid, index, subkey), value.to_vec());
        Ok(())
    }

    fn write_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
        data: &[u8],
    ) -> Result<(), Self::Error> {
        self.segments
            .borrow_mut()
            .insert(sub_key(database_id, tid, index, segment_id), data.to_vec());
        Ok(())
    }

    fn read_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .segments
            .borrow()
            .get(&sub_key(database_id, tid, index, segment_id))
            .cloned())
    }

    fn list_segments(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<Vec<String>, Self::Error> {
        let key = index_key(database_id, tid, index);
        Ok(self
            .segments
            .borrow()
            .keys()
            .filter(|(k, _)| *k == key)
            .map(|(_, seg)| seg.clone())
            .collect())
    }

    fn remove_segment(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        segment_id: &str,
    ) -> Result<(), Self::Error> {
        self.segments
            .borrow_mut()
            .remove(&sub_key(database_id, tid, index, segment_id));
        Ok(())
    }

    fn purge_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<usize, Self::Error> {
        Ok(self.purge_owned(database_id, tid, Some(collection)))
    }

    fn purge_tenant(&self, database_id: u64, tid: u64) -> Result<usize, Self::Error> {
        Ok(self.purge_owned(database_id, tid, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u64 = 0;
    const T: u64 = 1;
    const COL: IndexScope<'static> = IndexScope::document("col");
    const OTHER: IndexScope<'static> = IndexScope::document("other");

    fn posting(position: u32) -> Vec<Posting> {
        vec![Posting {
            doc_id: Surrogate(1),
            term_freq: 1,
            positions: vec![position],
        }]
    }

    #[test]
    fn roundtrip_postings() {
        let backend = MemoryBackend::new();
        let postings = vec![Posting {
            doc_id: Surrogate(1),
            term_freq: 2,
            positions: vec![0, 5],
        }];
        backend
            .write_postings(DB, T, COL, "hello", &postings)
            .unwrap();

        let read = backend.read_postings(DB, T, COL, "hello").unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].doc_id, Surrogate(1));
    }

    #[test]
    fn upsert_replaces_one_document_and_remove_empties_the_list() {
        let backend = MemoryBackend::new();
        let doc = |id: u32, freq: u32| Posting {
            doc_id: Surrogate(id),
            term_freq: freq,
            positions: vec![0],
        };
        for (id, freq) in [(3, 1), (1, 1), (2, 1), (1, 4)] {
            backend
                .upsert_posting(DB, T, COL, "ab", &doc(id, freq))
                .unwrap();
        }
        backend
            .upsert_posting(DB, T, COL, "abc", &doc(9, 1))
            .unwrap();

        let ab = backend.read_postings(DB, T, COL, "ab").unwrap();
        let ids: Vec<(u32, u32)> = ab.iter().map(|p| (p.doc_id.0, p.term_freq)).collect();
        assert_eq!(ids, vec![(1, 4), (2, 1), (3, 1)]);
        assert_eq!(backend.read_postings(DB, T, COL, "abc").unwrap().len(), 1);

        for id in [1, 2, 3] {
            backend
                .remove_posting(DB, T, COL, "ab", Surrogate(id))
                .unwrap();
        }
        assert!(backend.read_postings(DB, T, COL, "ab").unwrap().is_empty());
        assert_eq!(backend.collection_terms(DB, T, COL).unwrap(), vec!["abc"]);
        backend
            .remove_posting(DB, T, COL, "missing", Surrogate(1))
            .unwrap();
    }

    #[test]
    fn roundtrip_doc_lengths() {
        let backend = MemoryBackend::new();
        backend
            .write_doc_length(DB, T, COL, Surrogate(1), 42)
            .unwrap();
        assert_eq!(
            backend.read_doc_length(DB, T, COL, Surrogate(1)).unwrap(),
            Some(42)
        );

        backend.remove_doc_length(DB, T, COL, Surrogate(1)).unwrap();
        assert_eq!(
            backend.read_doc_length(DB, T, COL, Surrogate(1)).unwrap(),
            None
        );
    }

    #[test]
    fn incremental_stats() {
        let backend = MemoryBackend::new();
        backend.increment_stats(DB, T, COL, 10).unwrap();
        backend.increment_stats(DB, T, COL, 20).unwrap();
        assert_eq!(backend.collection_stats(DB, T, COL).unwrap(), (2, 30));

        backend.decrement_stats(DB, T, COL, 10).unwrap();
        assert_eq!(backend.collection_stats(DB, T, COL).unwrap(), (1, 20));
    }

    #[test]
    fn stats_saturating_sub() {
        let backend = MemoryBackend::new();
        backend.decrement_stats(DB, T, COL, 100).unwrap();
        assert_eq!(backend.collection_stats(DB, T, COL).unwrap(), (0, 0));
    }

    #[test]
    fn field_scopes_are_isolated_from_the_document_scope() {
        let backend = MemoryBackend::new();
        let title = IndexScope::field("col", "title").unwrap();
        backend
            .write_postings(DB, T, title, "rust", &posting(0))
            .unwrap();
        backend.increment_stats(DB, T, title, 3).unwrap();
        backend
            .write_doc_length(DB, T, title, Surrogate(1), 3)
            .unwrap();

        assert!(
            backend
                .read_postings(DB, T, COL, "rust")
                .unwrap()
                .is_empty()
        );
        assert_eq!(backend.collection_stats(DB, T, COL).unwrap(), (0, 0));
        assert_eq!(
            backend.read_doc_length(DB, T, COL, Surrogate(1)).unwrap(),
            None
        );
        assert_eq!(
            backend.collection_terms(DB, T, title).unwrap(),
            vec!["rust"]
        );
        assert!(backend.collection_terms(DB, T, COL).unwrap().is_empty());
    }

    #[test]
    fn purge_clears_stats_and_isolates_collections() {
        let backend = MemoryBackend::new();
        let title = IndexScope::field("col", "title").unwrap();
        backend.increment_stats(DB, T, COL, 10).unwrap();
        backend.increment_stats(DB, T, title, 2).unwrap();
        backend
            .write_doc_length(DB, T, COL, Surrogate(1), 10)
            .unwrap();
        backend
            .write_postings(DB, T, COL, "hello", &posting(0))
            .unwrap();
        backend
            .write_postings(DB, T, title, "hello", &posting(0))
            .unwrap();

        backend.increment_stats(DB, T, OTHER, 7).unwrap();
        backend
            .write_doc_length(DB, T, OTHER, Surrogate(1), 7)
            .unwrap();
        backend
            .write_postings(DB, T, OTHER, "world", &posting(0))
            .unwrap();

        backend.purge_collection(DB, T, "col").unwrap();
        assert_eq!(backend.collection_stats(DB, T, COL).unwrap(), (0, 0));
        assert_eq!(backend.collection_stats(DB, T, title).unwrap(), (0, 0));
        assert!(
            backend
                .read_postings(DB, T, COL, "hello")
                .unwrap()
                .is_empty()
        );
        assert!(
            backend
                .read_postings(DB, T, title, "hello")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            backend.read_doc_length(DB, T, COL, Surrogate(1)).unwrap(),
            None
        );

        assert_eq!(backend.collection_stats(DB, T, OTHER).unwrap(), (1, 7));
        assert_eq!(
            backend.read_postings(DB, T, OTHER, "world").unwrap().len(),
            1
        );
        assert_eq!(
            backend.read_doc_length(DB, T, OTHER, Surrogate(1)).unwrap(),
            Some(7)
        );
    }

    #[test]
    fn collection_terms() {
        let backend = MemoryBackend::new();
        backend
            .write_postings(DB, T, COL, "hello", &posting(0))
            .unwrap();
        backend
            .write_postings(DB, T, COL, "world", &posting(1))
            .unwrap();

        let mut terms = backend.collection_terms(DB, T, COL).unwrap();
        terms.sort();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn segment_roundtrip() {
        let backend = MemoryBackend::new();
        let data = b"compressed segment bytes";
        backend.write_segment(DB, T, COL, "id1", data).unwrap();
        assert_eq!(
            backend.read_segment(DB, T, COL, "id1").unwrap(),
            Some(data.to_vec())
        );
        assert_eq!(backend.read_segment(DB, T, COL, "missing").unwrap(), None);
    }

    #[test]
    fn segment_list_filters_by_index() {
        let backend = MemoryBackend::new();
        let title = IndexScope::field("col", "title").unwrap();
        backend.write_segment(DB, T, COL, "a", b"a").unwrap();
        backend.write_segment(DB, T, COL, "b", b"b").unwrap();
        backend.write_segment(DB, T, OTHER, "c", b"c").unwrap();
        backend.write_segment(DB, T, title, "d", b"d").unwrap();

        let mut segs = backend.list_segments(DB, T, COL).unwrap();
        segs.sort();
        assert_eq!(segs, vec!["a", "b"]);

        assert_eq!(backend.list_segments(DB, T, OTHER).unwrap(), vec!["c"]);
        assert_eq!(backend.list_segments(DB, T, title).unwrap(), vec!["d"]);
    }

    #[test]
    fn segment_remove() {
        let backend = MemoryBackend::new();
        backend.write_segment(DB, T, COL, "id1", b"data").unwrap();
        backend.remove_segment(DB, T, COL, "id1").unwrap();
        assert_eq!(backend.read_segment(DB, T, COL, "id1").unwrap(), None);
    }

    #[test]
    fn purge_clears_segments() {
        let backend = MemoryBackend::new();
        backend.write_segment(DB, T, COL, "a", b"a").unwrap();
        backend.write_segment(DB, T, OTHER, "b", b"b").unwrap();

        backend.purge_collection(DB, T, "col").unwrap();
        assert!(backend.list_segments(DB, T, COL).unwrap().is_empty());
        assert_eq!(backend.list_segments(DB, T, OTHER).unwrap().len(), 1);
    }

    #[test]
    fn purge_tenant_isolates_tenants() {
        let backend = MemoryBackend::new();
        backend.increment_stats(DB, 1, COL, 5).unwrap();
        backend.increment_stats(DB, 2, COL, 7).unwrap();
        backend
            .write_postings(DB, 1, COL, "t", &posting(0))
            .unwrap();
        backend
            .write_postings(DB, 2, COL, "t", &posting(0))
            .unwrap();

        backend.purge_tenant(DB, 1).unwrap();
        assert_eq!(backend.collection_stats(DB, 1, COL).unwrap(), (0, 0));
        assert!(backend.read_postings(DB, 1, COL, "t").unwrap().is_empty());
        assert_eq!(backend.collection_stats(DB, 2, COL).unwrap(), (1, 7));
        assert_eq!(backend.read_postings(DB, 2, COL, "t").unwrap().len(), 1);
    }

    #[test]
    fn databases_isolated() {
        let backend = MemoryBackend::new();
        backend.increment_stats(0, T, COL, 5).unwrap();
        backend.increment_stats(9, T, COL, 7).unwrap();
        backend.write_postings(0, T, COL, "t", &posting(0)).unwrap();
        backend.write_postings(9, T, COL, "t", &posting(0)).unwrap();

        backend.purge_tenant(0, T).unwrap();
        assert_eq!(backend.collection_stats(0, T, COL).unwrap(), (0, 0));
        assert!(backend.read_postings(0, T, COL, "t").unwrap().is_empty());
        // Same tenant in a different database must be unaffected.
        assert_eq!(backend.collection_stats(9, T, COL).unwrap(), (1, 7));
        assert_eq!(backend.read_postings(9, T, COL, "t").unwrap().len(), 1);
    }
}
