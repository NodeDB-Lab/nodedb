// SPDX-License-Identifier: BUSL-1.1

//! In-memory inverted index for sparse vectors.
//!
//! Maps each dimension to a posting list of `(doc_id, weight)` pairs.
//! Maintained synchronously on insert/delete. Checkpoint-serializable
//! for crash recovery.
//!
//! Every write carries its system-time ordinal. An overwrite or a delete
//! ends the document's live version there and keeps it in the history
//! (see `history`), so a read at an earlier cut still scores it.

use std::collections::HashMap;

use nodedb_types::SparseVector;

use super::history::SparseHistory;

/// One document of a [`SparseInvertedIndex`]: its internal id, its
/// `(dimension, weight)` entries, and the ordinal its live version started
/// at (`i64::MIN` when no retained cut reads below it).
#[derive(Debug, Clone, PartialEq)]
pub struct SparseDocImage {
    pub(super) internal_id: u32,
    pub(super) entries: Vec<(u32, f32)>,
    pub(super) sys_from: i64,
}

/// Inverted index for sparse vectors.
///
/// For each dimension that appears in any live document's sparse vector,
/// stores a posting list of `(doc_id, weight)` pairs. Ended versions keep
/// their entries in [`SparseHistory`].
///
/// This type is `!Send` — owned by a single Data Plane core.
pub struct SparseInvertedIndex {
    /// Dimension → live posting list. Each posting: (internal id, weight).
    pub(super) postings: HashMap<u32, Vec<(u32, f32)>>,
    /// Live internal id → the dimensions it has entries for.
    pub(super) doc_dims: HashMap<u32, Vec<u32>>,
    /// Next internal id (monotonic).
    pub(super) next_id: u32,
    /// String document id → live internal id.
    pub(super) doc_id_forward: HashMap<String, u32>,
    /// Live internal id → string document id.
    pub(super) doc_id_reverse: HashMap<u32, String>,
    /// Number of live documents.
    pub(super) doc_count: usize,
    /// Live starts and ended versions.
    pub(super) history: SparseHistory,
    /// Bytes the live versions hold ([`live_doc_bytes`]), kept in step with
    /// every install and removal.
    pub(super) live_bytes: usize,
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
            history: SparseHistory::default(),
            live_bytes: 0,
        }
    }

    /// Insert a sparse vector for a document at system-time ordinal
    /// `ordinal`. The document's live version ends at `ordinal`. Returns the
    /// new internal id.
    pub fn insert_at(&mut self, doc_id: &str, vector: &SparseVector, ordinal: i64) -> u32 {
        if let Some(&existing) = self.doc_id_forward.get(doc_id) {
            self.end_live(doc_id, existing, ordinal);
        }
        let internal_id = self.next_id;
        self.next_id += 1;
        self.install_live(doc_id, internal_id, vector.entries(), ordinal);
        internal_id
    }

    /// End the live version of `doc_id` at system-time ordinal `ordinal`.
    /// Returns `false` when the document has no live version.
    pub fn delete_at(&mut self, doc_id: &str, ordinal: i64) -> bool {
        let Some(&internal_id) = self.doc_id_forward.get(doc_id) else {
            return false;
        };
        self.end_live(doc_id, internal_id, ordinal);
        true
    }

    /// [`Self::insert_at`] for a write that keeps no history: the replaced
    /// version is dropped and the new one is visible at every cut.
    pub fn insert(&mut self, doc_id: &str, vector: &SparseVector) -> u32 {
        self.insert_at(doc_id, vector, i64::MIN)
    }

    /// [`Self::delete_at`] for a write that keeps no history.
    pub fn delete(&mut self, doc_id: &str) -> bool {
        self.delete_at(doc_id, i64::MIN)
    }

    /// Make `entries` the live version `internal_id` of `doc_id`, started at
    /// `sys_from`.
    pub(super) fn install_live(
        &mut self,
        doc_id: &str,
        internal_id: u32,
        entries: &[(u32, f32)],
        sys_from: i64,
    ) {
        let mut dims = Vec::with_capacity(entries.len());
        for &(dim, weight) in entries {
            let list = self.postings.entry(dim).or_default();
            let at = list.partition_point(|(existing, _)| *existing < internal_id);
            list.insert(at, (internal_id, weight));
            dims.push(dim);
        }
        self.live_bytes += live_doc_bytes(doc_id.len(), dims.len());
        self.doc_dims.insert(internal_id, dims);
        self.doc_id_forward.insert(doc_id.to_string(), internal_id);
        self.doc_id_reverse.insert(internal_id, doc_id.to_string());
        self.doc_count += 1;
        self.history.record_live(internal_id, sys_from);
    }

    /// Move live version `internal_id` of `doc_id` into the history, ended
    /// at `ordinal`.
    fn end_live(&mut self, doc_id: &str, internal_id: u32, ordinal: i64) {
        let entries = self.live_entries(internal_id);
        self.forget_live(internal_id);
        let sys_from = self.history.take_live_from(internal_id);
        self.history
            .end(internal_id, doc_id.to_string(), &entries, sys_from, ordinal);
    }

    /// Drop every trace of live version `internal_id` except its start.
    fn forget_live(&mut self, internal_id: u32) {
        let entries = self.doc_dims.get(&internal_id).map_or(0, Vec::len);
        if let Some(doc_id) = self.doc_id_reverse.get(&internal_id) {
            self.live_bytes -= live_doc_bytes(doc_id.len(), entries);
        }
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
        if let Some(doc_id) = self.doc_id_reverse.remove(&internal_id) {
            self.doc_id_forward.remove(&doc_id);
            self.doc_count -= 1;
        }
    }

    /// The `(dimension, weight)` entries of live version `internal_id`.
    pub(super) fn live_entries(&self, internal_id: u32) -> Vec<(u32, f32)> {
        self.doc_dims
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
            .unwrap_or_default()
    }

    /// The live version of `doc_id`, or `None` when absent.
    pub fn doc_image(&self, doc_id: &str) -> Option<SparseDocImage> {
        let internal_id = *self.doc_id_forward.get(doc_id)?;
        Some(SparseDocImage {
            internal_id,
            entries: self.live_entries(internal_id),
            sys_from: self.history.live_from(internal_id),
        })
    }

    /// The internal id the next insert takes.
    pub fn next_internal_id(&self) -> u32 {
        self.next_id
    }

    /// Put `doc_id` back to `prior` (absent when `None`) and the internal id
    /// counter back to `next_id`. A rollback uses it to withdraw a write: the
    /// version the write created is dropped with no history, the version it
    /// ended is live again from its own start, and the next insert takes the
    /// id it would have taken without the write.
    pub fn roll_back_doc(&mut self, doc_id: &str, prior: Option<&SparseDocImage>, next_id: u32) {
        if let Some(&current) = self.doc_id_forward.get(doc_id) {
            self.forget_live(current);
            self.history.take_live_from(current);
        }
        if let Some(prior) = prior {
            self.history.remove_ended(prior.internal_id);
            self.install_live(doc_id, prior.internal_id, &prior.entries, prior.sys_from);
        }
        self.next_id = self.next_id.min(next_id);
    }

    /// Drop every version no cut at or above `horizon` reads: a version
    /// whose successor started at or below `horizon`, and a deleted
    /// document's last version ended at or below it. The newest version at
    /// or below the horizon stays. Returns the number of versions dropped.
    pub fn purge_sparse_versions_below(&mut self, horizon: i64) -> usize {
        self.history.purge_below(horizon)
    }

    /// Bytes the index holds resident, live and history. Constant time:
    /// every write keeps the counts. A governor charge follows this value.
    pub fn resident_bytes(&self) -> usize {
        self.live_bytes + self.history.resident_bytes()
    }

    /// The live posting list for a dimension.
    pub fn get_postings(&self, dim: u32) -> Option<&[(u32, f32)]> {
        self.postings.get(&dim).map(|v| v.as_slice())
    }

    /// The history of ended versions and live starts.
    pub fn history(&self) -> &SparseHistory {
        &self.history
    }

    /// Resolve an internal id, live or ended, to its string document id.
    pub fn resolve_doc_id(&self, internal_id: u32) -> Option<&str> {
        self.doc_id_reverse
            .get(&internal_id)
            .map(|s| s.as_str())
            .or_else(|| self.history.ended_doc_id(internal_id))
    }

    /// Number of live documents.
    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    /// Number of distinct dimensions with at least one live posting.
    pub fn dim_count(&self) -> usize {
        self.postings.len()
    }

    /// Number of live posting entries across all dimensions.
    pub fn total_postings(&self) -> usize {
        self.postings.values().map(|v| v.len()).sum()
    }

    /// Whether the index holds no live document.
    pub fn is_empty(&self) -> bool {
        self.doc_count == 0
    }
}

/// Bytes one live document holds: its posting entries and dimension list,
/// and its id in both directions.
fn live_doc_bytes(doc_id_len: usize, entries: usize) -> usize {
    use std::mem::size_of;
    entries * (size_of::<(u32, f32)>() + size_of::<u32>())
        + size_of::<Vec<u32>>()
        + 2 * (doc_id_len + size_of::<String>() + size_of::<u32>())
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
    fn an_overwrite_keeps_the_prior_version_ended_at_its_ordinal() {
        let mut idx = SparseInvertedIndex::new();
        let first = idx.insert_at("doc1", &make_sv(&[(10, 0.5)]), 10);
        let second = idx.insert_at("doc1", &make_sv(&[(20, 0.9)]), 20);
        assert_eq!(idx.doc_count(), 1);
        assert!(
            idx.get_postings(10).is_none(),
            "live postings hold the new version"
        );
        let h = idx.history();
        assert!(h.ended_visible_at(first, 15));
        assert!(!h.ended_visible_at(first, 20));
        assert!(!h.live_visible_at(second, 19));
        assert!(h.live_visible_at(second, 20));
        assert_eq!(
            idx.resolve_doc_id(first),
            Some("doc1"),
            "an ended id resolves"
        );
    }

    #[test]
    fn a_delete_ends_the_live_version() {
        let mut idx = SparseInvertedIndex::new();
        let id = idx.insert_at("doc1", &make_sv(&[(10, 0.5)]), 10);
        assert!(idx.delete_at("doc1", 30));
        assert!(idx.is_empty());
        assert!(idx.history().ended_visible_at(id, 29));
        assert!(!idx.history().ended_visible_at(id, 30));
        assert!(!idx.delete_at("doc1", 40), "no live version left");
    }

    #[test]
    fn a_rolled_back_overwrite_revives_the_ended_version() {
        let mut idx = SparseInvertedIndex::new();
        let first = idx.insert_at("doc1", &make_sv(&[(10, 0.5)]), 10);
        let prior = idx.doc_image("doc1");
        let next_id = idx.next_internal_id();
        idx.insert_at("doc1", &make_sv(&[(20, 0.9)]), 20);
        idx.roll_back_doc("doc1", prior.as_ref(), next_id);
        assert_eq!(idx.doc_image("doc1"), prior);
        assert_eq!(
            idx.history().ended_count(),
            0,
            "the revived version left the history"
        );
        assert!(
            !idx.history().live_visible_at(first, 9),
            "it keeps its own start"
        );
        assert!(idx.get_postings(20).is_none());
    }

    #[test]
    fn purge_drops_versions_superseded_at_or_below_the_horizon() {
        let mut idx = SparseInvertedIndex::new();
        let v1 = idx.insert_at("doc1", &make_sv(&[(10, 0.5)]), 10);
        let v2 = idx.insert_at("doc1", &make_sv(&[(10, 0.6)]), 20);
        idx.insert_at("doc1", &make_sv(&[(10, 0.7)]), 30);
        assert_eq!(idx.purge_sparse_versions_below(25), 1);
        assert!(
            idx.resolve_doc_id(v1).is_none(),
            "successor started at 20 <= 25"
        );
        assert!(
            idx.history().ended_visible_at(v2, 25),
            "the version visible at the horizon stays"
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

    /// The byte count follows every insert, end, revive, and purge exactly.
    #[test]
    fn resident_bytes_follow_every_write() {
        let mut idx = SparseInvertedIndex::new();
        assert_eq!(idx.resident_bytes(), 0);
        let one = live_doc_bytes("d1".len(), 2);
        idx.insert_at("d1", &make_sv(&[(1, 0.5), (2, 0.25)]), 10);
        assert_eq!(idx.resident_bytes(), one + 12, "the live doc and its start");

        let prior = idx.doc_image("d1");
        let next_id = idx.next_internal_id();
        idx.insert_at("d1", &make_sv(&[(3, 1.0)]), 20);
        let ended = idx.history().resident_bytes() - 12;
        assert!(ended > 0, "the ended version is history");
        assert_eq!(
            idx.resident_bytes(),
            live_doc_bytes("d1".len(), 1) + 12 + ended
        );

        idx.roll_back_doc("d1", prior.as_ref(), next_id);
        assert_eq!(
            idx.resident_bytes(),
            one + 12,
            "a revive restores the bytes"
        );

        idx.insert_at("d1", &make_sv(&[(3, 1.0)]), 20);
        assert_eq!(idx.purge_sparse_versions_below(30), 1);
        assert_eq!(
            idx.resident_bytes(),
            live_doc_bytes("d1".len(), 1),
            "the purge releases the ended version and the settled start"
        );
        assert!(idx.delete("d1"));
        assert_eq!(idx.resident_bytes(), 0);
    }
}
