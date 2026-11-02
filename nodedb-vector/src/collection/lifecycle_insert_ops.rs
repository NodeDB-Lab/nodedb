// SPDX-License-Identifier: Apache-2.0

//! Insert, delete, and surrogate-map operations for `VectorCollection`.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;
use crate::error::{VectorError, check_dim};

impl VectorCollection {
    /// Insert a vector. Returns the global vector ID. A trained IVF-PQ
    /// collection inserts into its IVF index, every other collection into
    /// the growing segment.
    ///
    /// A vector without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] and changes nothing.
    pub fn insert(&mut self, vector: Vec<f32>) -> Result<u32, VectorError> {
        check_dim(self.dim, vector.len())?;
        let id = self.next_id;
        match &mut self.ivf {
            Some(ivf) => ivf.insert_with_id(id, vector)?,
            None => {
                self.growing.insert(vector)?;
            }
        }
        self.next_id += 1;
        Ok(id)
    }

    /// Insert a vector with an associated surrogate. The surrogate is
    /// allocated by the Control Plane before the call; the engine only
    /// stores the binding.
    ///
    /// One surrogate names one live node. A node already bound to
    /// `surrogate` is soft-deleted before the new one is bound, so a
    /// re-insert never leaves an unreachable node scoring in searches.
    /// The caller owns the payload bitmap entries of the old node and
    /// removes them with [`Self::local_for_surrogate`] before this call.
    ///
    /// A vector without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] before the old binding is touched.
    ///
    /// [`Surrogate::ZERO`] binds nothing: a headless vector has no surrogate,
    /// so it never enters `surrogate_map` or `surrogate_to_local`, and no
    /// lookup or delete by `ZERO` reaches it. Compaction, checkpoint restore
    /// and rollback rebuild both maps from `surrogate_map` alone, so they
    /// never map `ZERO` either.
    pub fn insert_with_surrogate(
        &mut self,
        vector: Vec<f32>,
        surrogate: Surrogate,
    ) -> Result<u32, VectorError> {
        check_dim(self.dim, vector.len())?;
        if surrogate != Surrogate::ZERO
            && let Some(old) = self.surrogate_to_local.get(&surrogate).copied()
        {
            self.delete_inner(old);
            self.surrogate_map.remove(&old);
            self.versions.forget(old);
        }
        let id = self.insert(vector)?;
        if surrogate != Surrogate::ZERO {
            self.surrogate_map.insert(id, surrogate);
            self.surrogate_to_local.insert(surrogate, id);
        }
        Ok(id)
    }

    /// Insert a batch of vectors, the `i`-th bound to `surrogates[i]`
    /// ([`Surrogate::ZERO`] when the slice is shorter). Returns the global ids
    /// in batch order.
    ///
    /// Every vector is checked against the collection dimension before any is
    /// inserted, so a mismatch fails with [`VectorError::DimensionMismatch`]
    /// and inserts none.
    pub fn insert_batch_with_surrogates(
        &mut self,
        vectors: &[Vec<f32>],
        surrogates: &[Surrogate],
    ) -> Result<Vec<u32>, VectorError> {
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }
        let mut ids = Vec::with_capacity(vectors.len());
        for (i, v) in vectors.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            ids.push(self.insert_with_surrogate(v.clone(), surrogate)?);
        }
        Ok(ids)
    }

    /// Insert multiple vectors for a single document (ColBERT-style).
    /// All N vectors are bound to the same `document_surrogate`.
    ///
    /// Every vector is checked against the collection dimension before any
    /// is inserted, so a mismatch fails with
    /// [`VectorError::DimensionMismatch`] and inserts none.
    pub fn insert_multi_vector(
        &mut self,
        vectors: &[&[f32]],
        document_surrogate: Surrogate,
    ) -> Result<Vec<u32>, VectorError> {
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }
        let mut ids = Vec::with_capacity(vectors.len());
        for &v in vectors {
            let id = self.insert(v.to_vec())?;
            if document_surrogate != Surrogate::ZERO {
                self.surrogate_map.insert(id, document_surrogate);
            }
            ids.push(id);
        }
        if document_surrogate != Surrogate::ZERO {
            self.multi_doc_map.insert(document_surrogate, ids.clone());
        }
        Ok(ids)
    }

    /// Delete all vectors belonging to a multi-vector document.
    pub fn delete_multi_vector(&mut self, document_surrogate: Surrogate) -> usize {
        let Some(ids) = self.multi_doc_map.remove(&document_surrogate) else {
            return 0;
        };
        let mut deleted = 0;
        for id in &ids {
            if self.delete(*id) {
                deleted += 1;
            }
            self.surrogate_map.remove(id);
        }
        self.surrogate_to_local.remove(&document_surrogate);
        deleted
    }

    /// Look up the surrogate for a global vector ID: the row a live node
    /// holds, or the row an ended node held, so a hit of a search at a past
    /// cut resolves.
    pub fn get_surrogate(&self, vector_id: u32) -> Option<Surrogate> {
        self.surrogate_map
            .get(&vector_id)
            .copied()
            .or_else(|| self.versions.ended_surrogate(vector_id))
    }

    /// Resolve a surrogate back to its global vector ID, if bound.
    pub fn local_for_surrogate(&self, surrogate: Surrogate) -> Option<u32> {
        self.surrogate_to_local.get(&surrogate).copied()
    }

    /// Soft-delete a vector by global ID and drop its history: no cut reads
    /// it again. A row update or delete ends the node instead
    /// ([`Self::end_at`]), so earlier cuts keep reading it. The undo of an
    /// insert deletes this way.
    pub fn delete(&mut self, id: u32) -> bool {
        let ok = self.delete_inner(id);
        if ok && let Some(s) = self.surrogate_map.remove(&id) {
            self.surrogate_to_local.remove(&s);
        }
        self.versions.forget(id);
        ok
    }

    pub(super) fn delete_inner(&mut self, id: u32) -> bool {
        if let Some(ivf) = &mut self.ivf
            && ivf.contains(id)
        {
            return ivf.delete(id);
        }
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.delete(local);
            }
        }
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.delete(local);
                }
            }
        }
        for seg in &mut self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.delete(local);
                }
            }
        }
        false
    }

    /// The live FP32 vector stored under global `id`, whichever segment
    /// holds it. `None` for an unknown or soft-deleted id.
    pub fn vector_for_id(&self, id: u32) -> Option<Vec<f32>> {
        if let Some(ivf) = &self.ivf
            && ivf.contains(id)
        {
            return ivf.get_vector(id).map(<[f32]>::to_vec);
        }
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.get_vector(local).map(<[f32]>::to_vec);
            }
        }
        for seg in &self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    if seg.index.is_deleted(local) {
                        return None;
                    }
                    return sealed_vector(seg, local);
                }
            }
        }
        for seg in &self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.get_vector(local).map(<[f32]>::to_vec);
                }
            }
        }
        None
    }

    /// The live FP32 vector bound to `surrogate`, if any.
    pub fn vector_for_surrogate(&self, surrogate: Surrogate) -> Option<Vec<f32>> {
        self.local_for_surrogate(surrogate)
            .and_then(|id| self.vector_for_id(id))
    }

    /// Soft-delete a vector by surrogate.
    pub fn delete_by_surrogate(&mut self, surrogate: Surrogate) -> bool {
        let Some(global_id) = self.surrogate_to_local.get(&surrogate).copied() else {
            return false;
        };
        self.delete(global_id)
    }

    /// Un-delete a previously soft-deleted vector (for transaction rollback).
    ///
    /// Symmetric to [`Self::delete_inner`]: the vector may live in the IVF
    /// index, the growing segment (the common case for a just-inserted
    /// vector), a sealed HNSW segment, or an in-flight building segment. The
    /// tombstone is reversed wherever it landed. An ended node becomes live
    /// again: the undo of the write that ended it.
    pub fn undelete(&mut self, id: u32) -> bool {
        let revived = self.undelete_node(id);
        if revived {
            self.versions.revive(id);
        }
        revived
    }

    /// Clear the tombstone of node `id`, whichever segment holds it.
    fn undelete_node(&mut self, id: u32) -> bool {
        if let Some(ivf) = &mut self.ivf
            && ivf.contains(id)
        {
            return ivf.undelete(id);
        }
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.undelete(local);
            }
        }
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.undelete(local);
                }
            }
        }
        for seg in &mut self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.undelete(local);
                }
            }
        }
        false
    }

    /// Un-delete `id` and bind it to `surrogate` again: the reverse of
    /// [`Self::delete`] on a bound node, which drops the binding.
    ///
    /// Returns `false` and changes nothing when `id` carries no tombstone.
    /// [`Surrogate::ZERO`] binds nothing, as in [`Self::insert_with_surrogate`].
    pub fn undelete_bound(&mut self, id: u32, surrogate: Surrogate) -> bool {
        if !self.undelete(id) {
            return false;
        }
        if surrogate != Surrogate::ZERO {
            self.surrogate_map.insert(id, surrogate);
            self.surrogate_to_local.insert(surrogate, id);
        }
        true
    }
}

/// The FP32 vector at `local` in a sealed segment: the mmap tier when the
/// segment lives there, else the HNSW node (decoded from a narrow dtype or
/// fetched from the segment backing when the node holds no local copy).
pub(super) fn sealed_vector(seg: &super::segment::SealedSegment, local: u32) -> Option<Vec<f32>> {
    if let Some(mmap) = &seg.mmap_vectors {
        return mmap.get_vector(local).map(<[f32]>::to_vec);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        seg.index
            .get_vector_or_backing(local)
            .map(std::borrow::Cow::into_owned)
    }
    #[cfg(target_arch = "wasm32")]
    {
        seg.index.get_vector(local).map(<[f32]>::to_vec)
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
    fn re_insert_under_the_same_surrogate_leaves_one_live_node() {
        let mut coll = collection();
        let s = Surrogate::new(7);
        let first = coll.insert_with_surrogate(vec![1.0, 0.0], s).unwrap();
        let second = coll.insert_with_surrogate(vec![0.0, 1.0], s).unwrap();
        assert_ne!(first, second);
        assert_eq!(coll.live_count(), 1, "the old node must be tombstoned");
        assert_eq!(coll.local_for_surrogate(s), Some(second));
        assert_eq!(coll.get_surrogate(first), None);
        assert_eq!(coll.get_surrogate(second), Some(s));
    }

    #[test]
    fn delete_then_insert_under_the_same_surrogate_leaves_one_live_node() {
        let mut coll = collection();
        let s = Surrogate::new(9);
        let first = coll.insert_with_surrogate(vec![1.0, 0.0], s).unwrap();
        assert!(coll.delete_by_surrogate(s));
        assert_eq!(coll.local_for_surrogate(s), None);
        let second = coll.insert_with_surrogate(vec![0.0, 1.0], s).unwrap();
        assert_ne!(first, second);
        assert_eq!(coll.live_count(), 1);
        assert_eq!(coll.local_for_surrogate(s), Some(second));
        assert!(!coll.delete(first), "the first node is already gone");
    }

    #[test]
    fn undelete_bound_restores_the_node_and_its_binding() {
        let mut coll = collection();
        let s = Surrogate::new(5);
        let id = coll.insert_with_surrogate(vec![1.0, 0.0], s).unwrap();
        assert!(coll.delete(id));
        assert_eq!(coll.local_for_surrogate(s), None);

        assert!(coll.undelete_bound(id, s));
        assert_eq!(coll.live_count(), 1);
        assert_eq!(coll.local_for_surrogate(s), Some(id));
        assert_eq!(coll.get_surrogate(id), Some(s));
        assert!(
            !coll.undelete_bound(id, s),
            "a live node has no tombstone to clear"
        );
    }

    #[test]
    fn delete_by_surrogate_is_idempotent() {
        let mut coll = collection();
        let s = Surrogate::new(3);
        coll.insert_with_surrogate(vec![1.0, 0.0], s).unwrap();
        assert!(coll.delete_by_surrogate(s));
        assert!(!coll.delete_by_surrogate(s));
        assert_eq!(coll.live_count(), 0);
    }

    #[test]
    fn vector_for_surrogate_reads_the_growing_segment_and_hides_deletes() {
        let mut coll = collection();
        let s = Surrogate::new(11);
        coll.insert_with_surrogate(vec![0.5, 0.25], s).unwrap();
        assert_eq!(coll.vector_for_surrogate(s), Some(vec![0.5, 0.25]));
        assert!(coll.delete_by_surrogate(s));
        assert_eq!(coll.vector_for_surrogate(s), None);
        assert_eq!(coll.vector_for_id(999), None);
    }

    #[test]
    fn headless_vectors_bind_no_surrogate() {
        let mut coll = collection();
        let first = coll
            .insert_with_surrogate(vec![1.0, 0.0], Surrogate::ZERO)
            .unwrap();
        let second = coll
            .insert_with_surrogate(vec![0.0, 1.0], Surrogate::ZERO)
            .unwrap();
        coll.insert_multi_vector(&[&[0.5, 0.5]], Surrogate::ZERO)
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(coll.live_count(), 3, "every headless insert stays live");
        assert_eq!(coll.local_for_surrogate(Surrogate::ZERO), None);
        assert_eq!(coll.get_surrogate(first), None);
        assert_eq!(coll.get_surrogate(second), None);
        assert!(coll.surrogate_to_local.is_empty());
        assert!(coll.multi_doc_map.is_empty());

        assert!(
            !coll.delete_by_surrogate(Surrogate::ZERO),
            "a delete by ZERO reaches no vector"
        );
        assert_eq!(coll.live_count(), 3);
    }
}
