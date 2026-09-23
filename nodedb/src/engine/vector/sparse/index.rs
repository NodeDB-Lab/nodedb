// SPDX-License-Identifier: BUSL-1.1

//! In-memory inverted index for sparse vectors.
//!
//! Maps each dimension to a posting list of `(doc_id, weight)` pairs.
//! Maintained synchronously on insert/delete. Checkpoint-serializable
//! for crash recovery.

use std::collections::HashMap;

use nodedb_types::SparseVector;
use serde::{Deserialize, Serialize};

/// One document of a [`SparseInvertedIndex`]: its internal id and its
/// `(dimension, weight)` entries.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseDocImage {
    internal_id: u32,
    entries: Vec<(u32, f32)>,
}

/// Inverted index for sparse vectors.
///
/// For each dimension that appears in any document's sparse vector, stores
/// a posting list of `(doc_id, weight)` pairs. Posting lists are sorted by
/// doc_id for efficient intersection during dot-product scoring.
///
/// This type is `!Send` — owned by a single Data Plane core.
pub struct SparseInvertedIndex {
    /// Dimension → posting list. Each posting: (doc_id as u32, weight).
    postings: HashMap<u32, Vec<(u32, f32)>>,
    /// Doc ID → list of dimensions that doc has entries for (for deletion).
    doc_dims: HashMap<u32, Vec<u32>>,
    /// Next doc ID counter (monotonic).
    next_id: u32,
    /// Mapping from string document ID to internal u32 ID.
    doc_id_forward: HashMap<String, u32>,
    /// Mapping from internal u32 ID to string document ID.
    doc_id_reverse: HashMap<u32, String>,
    /// Total number of documents indexed.
    doc_count: usize,
}

impl Default for SparseInvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseInvertedIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_dims: HashMap::new(),
            next_id: 0,
            doc_id_forward: HashMap::new(),
            doc_id_reverse: HashMap::new(),
            doc_count: 0,
        }
    }

    /// Insert a sparse vector for a document. Returns the internal doc ID.
    ///
    /// If the doc_id already exists, the old entries are removed first (upsert).
    pub fn insert(&mut self, doc_id: &str, vector: &SparseVector) -> u32 {
        // Upsert: remove old entries if doc already exists.
        if let Some(&existing_id) = self.doc_id_forward.get(doc_id) {
            self.remove_internal(existing_id);
            self.doc_id_forward.remove(doc_id);
            self.doc_id_reverse.remove(&existing_id);
            self.doc_count -= 1;
        }

        let internal_id = self.next_id;
        self.next_id += 1;

        self.doc_id_forward.insert(doc_id.to_string(), internal_id);
        self.doc_id_reverse.insert(internal_id, doc_id.to_string());

        let mut dims = Vec::with_capacity(vector.nnz());
        for &(dim, weight) in vector.entries() {
            self.postings
                .entry(dim)
                .or_default()
                .push((internal_id, weight));
            dims.push(dim);
        }
        self.doc_dims.insert(internal_id, dims);
        self.doc_count += 1;

        internal_id
    }

    /// Delete a document by string ID. Returns true if found and removed.
    pub fn delete(&mut self, doc_id: &str) -> bool {
        let Some(&internal_id) = self.doc_id_forward.get(doc_id) else {
            return false;
        };
        self.remove_internal(internal_id);
        self.doc_id_forward.remove(doc_id);
        self.doc_id_reverse.remove(&internal_id);
        self.doc_count -= 1;
        true
    }

    /// Remove all posting entries for an internal doc ID.
    fn remove_internal(&mut self, internal_id: u32) {
        if let Some(dims) = self.doc_dims.remove(&internal_id) {
            for dim in dims {
                if let Some(list) = self.postings.get_mut(&dim) {
                    list.retain(|&(id, _)| id != internal_id);
                    if list.is_empty() {
                        self.postings.remove(&dim);
                    }
                }
            }
        }
    }

    /// The internal id and entries of `doc_id`, or `None` when absent.
    pub fn doc_image(&self, doc_id: &str) -> Option<SparseDocImage> {
        let internal_id = *self.doc_id_forward.get(doc_id)?;
        let entries = self
            .doc_dims
            .get(&internal_id)
            .map(|dims| {
                dims.iter()
                    .filter_map(|dim| {
                        let list = self.postings.get(dim)?;
                        let weight = list.iter().find(|(id, _)| *id == internal_id)?.1;
                        Some((*dim, weight))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(SparseDocImage {
            internal_id,
            entries,
        })
    }

    /// The internal id the next insert takes.
    pub fn next_internal_id(&self) -> u32 {
        self.next_id
    }

    /// Put `doc_id` back to `prior` (absent when `None`) and the internal id
    /// counter back to `next_id`. A rollback uses it to withdraw a write, so
    /// the next insert takes the id it would have taken without it.
    pub fn roll_back_doc(&mut self, doc_id: &str, prior: Option<&SparseDocImage>, next_id: u32) {
        if let Some(current) = self.doc_id_forward.remove(doc_id) {
            self.remove_internal(current);
            self.doc_id_reverse.remove(&current);
            self.doc_count -= 1;
        }
        if let Some(prior) = prior {
            let id = prior.internal_id;
            let mut dims = Vec::with_capacity(prior.entries.len());
            for &(dim, weight) in &prior.entries {
                let list = self.postings.entry(dim).or_default();
                let at = list.partition_point(|(existing, _)| *existing < id);
                list.insert(at, (id, weight));
                dims.push(dim);
            }
            self.doc_dims.insert(id, dims);
            self.doc_id_forward.insert(doc_id.to_string(), id);
            self.doc_id_reverse.insert(id, doc_id.to_string());
            self.doc_count += 1;
        }
        self.next_id = self.next_id.min(next_id);
    }

    /// Get the posting list for a dimension.
    pub fn get_postings(&self, dim: u32) -> Option<&[(u32, f32)]> {
        self.postings.get(&dim).map(|v| v.as_slice())
    }

    /// Resolve an internal doc ID to the string document ID.
    pub fn resolve_doc_id(&self, internal_id: u32) -> Option<&str> {
        self.doc_id_reverse.get(&internal_id).map(|s| s.as_str())
    }

    /// Total number of documents in the index.
    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    /// Total number of distinct dimensions with at least one posting.
    pub fn dim_count(&self) -> usize {
        self.postings.len()
    }

    /// Total number of posting entries across all dimensions.
    pub fn total_postings(&self) -> usize {
        self.postings.values().map(|v| v.len()).sum()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.doc_count == 0
    }

    /// Serialize to bytes for checkpoint persistence.
    ///
    /// Fallible on purpose. The checkpoint that writes these bytes reports an
    /// LSN authorising the WAL segments below it to be unlinked, so an encode
    /// failure that yielded empty bytes here would publish an EMPTY index under
    /// a manifest claiming every document was durable — destroying the only
    /// other copy. The error propagates instead, and the caller clamps.
    pub fn checkpoint_to_bytes(&self) -> crate::Result<Vec<u8>> {
        let snapshot = SparseIndexSnapshot {
            next_id: self.next_id,
            documents: self
                .doc_id_reverse
                .iter()
                .map(|(&id, name)| {
                    let dims = self.doc_dims.get(&id).cloned().unwrap_or_default();
                    let entries: Vec<(u32, f32)> = dims
                        .iter()
                        .filter_map(|&dim| {
                            self.postings.get(&dim).and_then(|list| {
                                list.iter()
                                    .find(|&&(doc, _)| doc == id)
                                    .map(|&(_, w)| (dim, w))
                            })
                        })
                        .collect();
                    SparseDocSnapshot {
                        internal_id: id,
                        doc_id: name.clone(),
                        entries,
                    }
                })
                .collect(),
        };
        zerompk::to_msgpack_vec(&snapshot).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!(
                "sparse vector index checkpoint encode failed for {} documents: {e}",
                self.doc_count
            ),
        })
    }

    /// Restore from checkpoint bytes.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snapshot: SparseIndexSnapshot = zerompk::from_msgpack(bytes).ok()?;
        let mut index = Self::new();
        index.next_id = snapshot.next_id;

        for doc in snapshot.documents {
            index
                .doc_id_forward
                .insert(doc.doc_id.clone(), doc.internal_id);
            index.doc_id_reverse.insert(doc.internal_id, doc.doc_id);

            let mut dims = Vec::with_capacity(doc.entries.len());
            for (dim, weight) in doc.entries {
                index
                    .postings
                    .entry(dim)
                    .or_default()
                    .push((doc.internal_id, weight));
                dims.push(dim);
            }
            index.doc_dims.insert(doc.internal_id, dims);
            index.doc_count += 1;
        }

        Some(index)
    }
}

/// Checkpoint serialization format.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct SparseIndexSnapshot {
    next_id: u32,
    documents: Vec<SparseDocSnapshot>,
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct SparseDocSnapshot {
    internal_id: u32,
    doc_id: String,
    entries: Vec<(u32, f32)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).unwrap()
    }

    #[test]
    fn insert_and_lookup() {
        let mut idx = SparseInvertedIndex::new();
        let sv = make_sv(&[(10, 0.5), (20, 0.8)]);
        idx.insert("doc1", &sv);

        assert_eq!(idx.doc_count(), 1);
        assert_eq!(idx.dim_count(), 2);

        let postings = idx.get_postings(10).unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].1, 0.5);
    }

    #[test]
    fn upsert_replaces() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5)]));
        idx.insert("doc1", &make_sv(&[(20, 0.9)]));

        assert_eq!(idx.doc_count(), 1);
        // Dim 10 should be gone, dim 20 should be present.
        assert!(idx.get_postings(10).is_none());
        assert_eq!(idx.get_postings(20).unwrap().len(), 1);
    }

    #[test]
    fn delete() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5), (20, 0.8)]));
        assert!(idx.delete("doc1"));
        assert_eq!(idx.doc_count(), 0);
        assert!(idx.get_postings(10).is_none());
        assert!(idx.get_postings(20).is_none());
        assert!(!idx.delete("doc1")); // already gone
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5), (20, 0.8)]));
        idx.insert("doc2", &make_sv(&[(20, 0.3), (30, 1.0)]));

        let bytes = idx.checkpoint_to_bytes().expect("encode");
        let restored = SparseInvertedIndex::from_checkpoint(&bytes).unwrap();

        assert_eq!(restored.doc_count(), 2);
        assert_eq!(restored.dim_count(), 3);
        assert!(restored.resolve_doc_id(0).is_some());
    }

    #[test]
    fn multiple_docs_same_dim() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(5, 0.3)]));
        idx.insert("doc2", &make_sv(&[(5, 0.7)]));
        idx.insert("doc3", &make_sv(&[(5, 0.1)]));

        let postings = idx.get_postings(5).unwrap();
        assert_eq!(postings.len(), 3);
    }

    #[test]
    fn a_rolled_back_upsert_restores_the_prior_entries_and_id_counter() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5), (20, 0.8)]));
        idx.insert("doc2", &make_sv(&[(10, 0.1)]));
        let prior = idx.doc_image("doc1");
        let next_id = idx.next_internal_id();

        idx.insert("doc1", &make_sv(&[(30, 0.9)]));
        idx.roll_back_doc("doc1", prior.as_ref(), next_id);

        assert_eq!(idx.doc_image("doc1"), prior);
        assert_eq!(idx.next_internal_id(), next_id);
        assert!(idx.get_postings(30).is_none());
        assert_eq!(
            idx.get_postings(10).map(<[(u32, f32)]>::to_vec),
            Some(vec![(0, 0.5), (1, 0.1)]),
            "the restored posting keeps its id order"
        );
    }

    #[test]
    fn a_rolled_back_insert_of_a_new_document_removes_it() {
        let mut idx = SparseInvertedIndex::new();
        let next_id = idx.next_internal_id();
        idx.insert("doc1", &make_sv(&[(10, 0.5)]));
        idx.roll_back_doc("doc1", None, next_id);
        assert!(idx.doc_image("doc1").is_none());
        assert_eq!(idx.doc_count(), 0);
        assert!(idx.get_postings(10).is_none());
    }
}
