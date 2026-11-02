// SPDX-License-Identifier: Apache-2.0

//! Writes that keep history.
//!
//! A write stamps its system-time ordinal on every node it inserts. An
//! update or a delete of a row ends the row's node at the write's ordinal
//! (see `versions`): the node stays soft-deleted, so a current search skips
//! it, and a search at an earlier cut still reads it.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;
use crate::error::{VectorError, check_dim};

impl VectorCollection {
    /// Insert `vector` for `surrogate` at system-time ordinal `ordinal`. The
    /// node already bound to `surrogate` ends at `ordinal`. Returns the new
    /// node's global id.
    ///
    /// [`Surrogate::ZERO`] binds nothing: the node is headless. A vector
    /// without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] before anything changes.
    pub fn insert_at(
        &mut self,
        vector: Vec<f32>,
        surrogate: Surrogate,
        ordinal: i64,
    ) -> Result<u32, VectorError> {
        check_dim(self.dim, vector.len())?;
        if surrogate != Surrogate::ZERO
            && let Some(old) = self.surrogate_to_local.get(&surrogate).copied()
        {
            self.end_at(old, ordinal);
        }
        let id = self.insert(vector)?;
        if surrogate != Surrogate::ZERO {
            self.surrogate_map.insert(id, surrogate);
            self.surrogate_to_local.insert(surrogate, id);
        }
        self.versions.record_live(id, surrogate, ordinal);
        Ok(id)
    }

    /// [`Self::insert_at`] for a batch: the `i`-th vector for
    /// `surrogates[i]` ([`Surrogate::ZERO`] when the slice is shorter), all
    /// at `ordinal`. Every vector is checked against the collection
    /// dimension before any is inserted.
    pub fn insert_batch_at(
        &mut self,
        vectors: &[Vec<f32>],
        surrogates: &[Surrogate],
        ordinal: i64,
    ) -> Result<Vec<u32>, VectorError> {
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }
        let mut ids = Vec::with_capacity(vectors.len());
        for (i, v) in vectors.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            ids.push(self.insert_at(v.clone(), surrogate, ordinal)?);
        }
        Ok(ids)
    }

    /// Insert the vectors of one multi-vector document at `ordinal`. The
    /// document's prior vectors end at `ordinal`. Every vector is checked
    /// against the collection dimension before anything changes.
    pub fn insert_multi_vector_at(
        &mut self,
        vectors: &[&[f32]],
        document_surrogate: Surrogate,
        ordinal: i64,
    ) -> Result<Vec<u32>, VectorError> {
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }
        if document_surrogate != Surrogate::ZERO {
            self.end_multi_vector_at(document_surrogate, ordinal);
        }
        let mut ids = Vec::with_capacity(vectors.len());
        for &v in vectors {
            let id = self.insert(v.to_vec())?;
            if document_surrogate != Surrogate::ZERO {
                self.surrogate_map.insert(id, document_surrogate);
            }
            self.versions.record_live(id, document_surrogate, ordinal);
            ids.push(id);
        }
        if document_surrogate != Surrogate::ZERO {
            self.multi_doc_map.insert(document_surrogate, ids.clone());
        }
        Ok(ids)
    }

    /// End live node `id` at `ordinal`: a current search skips it, a search
    /// at a cut below `ordinal` still reads it. Returns `false` when `id` is
    /// not a live node.
    pub fn end_at(&mut self, id: u32, ordinal: i64) -> bool {
        let surrogate = self
            .surrogate_map
            .get(&id)
            .copied()
            .unwrap_or(Surrogate::ZERO);
        if !self.delete_inner(id) {
            return false;
        }
        self.surrogate_map.remove(&id);
        if surrogate != Surrogate::ZERO && self.surrogate_to_local.get(&surrogate) == Some(&id) {
            self.surrogate_to_local.remove(&surrogate);
        }
        self.versions.end(id, surrogate, ordinal);
        true
    }

    /// End the node bound to `surrogate` at `ordinal`. Returns `false` when
    /// no live node is bound to it.
    pub fn end_surrogate_at(&mut self, surrogate: Surrogate, ordinal: i64) -> bool {
        match self.surrogate_to_local.get(&surrogate).copied() {
            Some(id) => self.end_at(id, ordinal),
            None => false,
        }
    }

    /// End every vector of a multi-vector document at `ordinal`. Returns
    /// the number of nodes ended.
    pub fn end_multi_vector_at(&mut self, document_surrogate: Surrogate, ordinal: i64) -> usize {
        self.end_multi_vector_ids_at(document_surrogate, ordinal)
            .len()
    }

    /// [`Self::end_multi_vector_at`], returning the ids of the nodes it
    /// ended. The document's membership leaves `multi_doc_map`.
    pub fn end_multi_vector_ids_at(
        &mut self,
        document_surrogate: Surrogate,
        ordinal: i64,
    ) -> Vec<u32> {
        let Some(ids) = self.multi_doc_map.remove(&document_surrogate) else {
            return Vec::new();
        };
        let ended: Vec<u32> = ids
            .into_iter()
            .filter(|id| self.end_at(*id, ordinal))
            .collect();
        self.surrogate_to_local.remove(&document_surrogate);
        ended
    }

    /// The undo of [`Self::end_multi_vector_ids_at`]: make the nodes `ended`
    /// live again, and restore `members` as the document's membership with
    /// every member bound to it. Returns `false` when a node of `ended` holds
    /// no tombstone; the nodes before it stay revived.
    pub fn revive_multi_vector(
        &mut self,
        document_surrogate: Surrogate,
        members: Vec<u32>,
        ended: &[u32],
    ) -> bool {
        for &id in ended {
            if !self.undelete(id) {
                return false;
            }
        }
        if document_surrogate != Surrogate::ZERO {
            for &id in &members {
                self.surrogate_map.insert(id, document_surrogate);
            }
        }
        self.multi_doc_map.insert(document_surrogate, members);
        true
    }

    /// Drop every node version no cut at or above `horizon` reads: nodes
    /// ended at or below `horizon`. The version of each row visible at the
    /// horizon stays. A dropped node stays soft-deleted until compaction
    /// removes it. Returns the number of node versions dropped.
    pub fn purge_vector_versions_below(&mut self, horizon: i64) -> usize {
        self.versions.purge_below(horizon).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::HnswParams;

    fn collection() -> VectorCollection {
        VectorCollection::new(2, HnswParams::default())
    }

    #[test]
    fn an_update_ends_the_old_node_and_binds_the_new_one() {
        let mut coll = collection();
        let s = Surrogate::new(7);
        let first = coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        let second = coll.insert_at(vec![0.0, 1.0], s, 20).unwrap();
        assert_eq!(coll.live_count(), 1);
        assert_eq!(coll.local_for_surrogate(s), Some(second));
        assert_eq!(coll.get_surrogate(first), Some(s), "an ended node resolves");
        let span = coll.versions.span(first).copied().expect("ended span");
        assert_eq!((span.sys_from, span.sys_to), (10, Some(20)));
    }

    #[test]
    fn undelete_bound_undoes_an_end() {
        let mut coll = collection();
        let s = Surrogate::new(3);
        let id = coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        assert!(coll.end_surrogate_at(s, 20));
        assert!(coll.versions.is_retained(id));
        assert!(coll.undelete_bound(id, s));
        assert!(!coll.versions.is_retained(id));
        assert_eq!(coll.local_for_surrogate(s), Some(id));
    }

    #[test]
    fn delete_forgets_the_node() {
        let mut coll = collection();
        let s = Surrogate::new(4);
        let id = coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        assert!(coll.delete(id));
        assert!(coll.versions.span(id).is_none());
        assert_eq!(coll.get_surrogate(id), None);
    }

    #[test]
    fn a_multi_vector_document_ends_whole() {
        let mut coll = collection();
        let doc = Surrogate::new(9);
        let first = coll
            .insert_multi_vector_at(&[&[1.0, 0.0], &[0.0, 1.0]], doc, 10)
            .unwrap();
        let second = coll
            .insert_multi_vector_at(&[&[0.5, 0.5]], doc, 20)
            .unwrap();
        assert_eq!(coll.live_count(), 1);
        for id in &first {
            assert!(coll.versions.is_retained(*id));
        }
        assert_eq!(coll.multi_doc_map.get(&doc), Some(&second));
        assert_eq!(coll.end_multi_vector_at(doc, 30), 1);
        assert_eq!(coll.live_count(), 0);
    }

    #[test]
    fn a_revived_multi_vector_document_is_live_again() {
        let mut coll = collection();
        let doc = Surrogate::new(9);
        let ids = coll
            .insert_multi_vector_at(&[&[1.0, 0.0], &[0.0, 1.0]], doc, 10)
            .unwrap();
        let ended = coll.end_multi_vector_ids_at(doc, 20);
        assert_eq!(ended, ids);
        assert_eq!(coll.live_count(), 0);
        assert!(coll.revive_multi_vector(doc, ids.clone(), &ended));
        assert_eq!(coll.live_count(), 2);
        assert_eq!(coll.multi_doc_map.get(&doc), Some(&ids));
        for id in &ids {
            assert!(!coll.versions.is_retained(*id), "the end is withdrawn");
            assert_eq!(coll.get_surrogate(*id), Some(doc));
        }
    }

    #[test]
    fn purge_drops_versions_ended_at_or_below_the_horizon() {
        let mut coll = collection();
        let s = Surrogate::new(5);
        let first = coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        let second = coll.insert_at(vec![0.0, 1.0], s, 20).unwrap();
        coll.end_at(second, 30);
        assert_eq!(coll.purge_vector_versions_below(25), 1);
        assert!(coll.versions.span(first).is_none());
        assert!(
            coll.versions.span(second).is_some(),
            "the version visible at the horizon stays"
        );
    }
}
