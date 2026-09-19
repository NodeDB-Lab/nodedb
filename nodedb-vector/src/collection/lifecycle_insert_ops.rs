// SPDX-License-Identifier: Apache-2.0

//! Insert, delete, and surrogate-map operations for `VectorCollection`.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Insert a vector. Returns the global vector ID.
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        let id = self.next_id;
        self.growing.insert(vector);
        self.next_id += 1;
        id
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
    pub fn insert_with_surrogate(&mut self, vector: Vec<f32>, surrogate: Surrogate) -> u32 {
        if surrogate != Surrogate::ZERO
            && let Some(old) = self.surrogate_to_local.get(&surrogate).copied()
        {
            self.delete_inner(old);
            self.surrogate_map.remove(&old);
        }
        let id = self.insert(vector);
        if surrogate != Surrogate::ZERO {
            self.surrogate_map.insert(id, surrogate);
            self.surrogate_to_local.insert(surrogate, id);
        }
        id
    }

    /// Insert multiple vectors for a single document (ColBERT-style).
    /// All N vectors are bound to the same `document_surrogate`.
    pub fn insert_multi_vector(
        &mut self,
        vectors: &[&[f32]],
        document_surrogate: Surrogate,
    ) -> Vec<u32> {
        let mut ids = Vec::with_capacity(vectors.len());
        for &v in vectors {
            let id = self.insert(v.to_vec());
            if document_surrogate != Surrogate::ZERO {
                self.surrogate_map.insert(id, document_surrogate);
            }
            ids.push(id);
        }
        if document_surrogate != Surrogate::ZERO {
            self.multi_doc_map.insert(document_surrogate, ids.clone());
        }
        ids
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

    /// Look up the surrogate for a global vector ID.
    pub fn get_surrogate(&self, vector_id: u32) -> Option<Surrogate> {
        self.surrogate_map.get(&vector_id).copied()
    }

    /// Resolve a surrogate back to its global vector ID, if bound.
    pub fn local_for_surrogate(&self, surrogate: Surrogate) -> Option<u32> {
        self.surrogate_to_local.get(&surrogate).copied()
    }

    /// Soft-delete a vector by global ID.
    pub fn delete(&mut self, id: u32) -> bool {
        let ok = self.delete_inner(id);
        if ok && let Some(s) = self.surrogate_map.remove(&id) {
            self.surrogate_to_local.remove(&s);
        }
        ok
    }

    pub(super) fn delete_inner(&mut self, id: u32) -> bool {
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

    /// Soft-delete a vector by surrogate.
    pub fn delete_by_surrogate(&mut self, surrogate: Surrogate) -> bool {
        let Some(global_id) = self.surrogate_to_local.get(&surrogate).copied() else {
            return false;
        };
        self.delete(global_id)
    }

    /// Un-delete a previously soft-deleted vector (for transaction rollback).
    ///
    /// Symmetric to [`Self::delete_inner`]: the vector may live in the growing
    /// segment (the common case for a just-inserted vector), a sealed HNSW
    /// segment, or an in-flight building segment — reverse the tombstone
    /// wherever it landed. Only clearing sealed tombstones (the prior behavior)
    /// silently failed to restore growing/building vectors, leaving a
    /// rolled-back delete permanently unsearchable.
    pub fn undelete(&mut self, id: u32) -> bool {
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
        let first = coll.insert_with_surrogate(vec![1.0, 0.0], s);
        let second = coll.insert_with_surrogate(vec![0.0, 1.0], s);
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
        let first = coll.insert_with_surrogate(vec![1.0, 0.0], s);
        assert!(coll.delete_by_surrogate(s));
        assert_eq!(coll.local_for_surrogate(s), None);
        let second = coll.insert_with_surrogate(vec![0.0, 1.0], s);
        assert_ne!(first, second);
        assert_eq!(coll.live_count(), 1);
        assert_eq!(coll.local_for_surrogate(s), Some(second));
        assert!(!coll.delete(first), "the first node is already gone");
    }

    #[test]
    fn delete_by_surrogate_is_idempotent() {
        let mut coll = collection();
        let s = Surrogate::new(3);
        coll.insert_with_surrogate(vec![1.0, 0.0], s);
        assert!(coll.delete_by_surrogate(s));
        assert!(!coll.delete_by_surrogate(s));
        assert_eq!(coll.live_count(), 0);
    }
}
