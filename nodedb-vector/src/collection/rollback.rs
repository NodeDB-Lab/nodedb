// SPDX-License-Identifier: Apache-2.0

//! Roll a collection back to the state it held before a write.
//!
//! A [`VectorWriteMark`] records what a write can change: the id counter,
//! the binding of every surrogate the write names, which of the named nodes
//! were live, and the multi-vector documents it names. Rolling back drops
//! every node inserted since the mark from the growing segment, so the id
//! counter returns to where it was and the next insert takes the same id it
//! would have taken without the write. Then it puts every binding and every
//! tombstone back.
//!
//! The inserted nodes must still sit in the growing segment: a seal between
//! the mark and the rollback moves them out, and the rollback then reports
//! that it cannot restore the mark.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;
use crate::flat::FlatIndex;

/// A collection's state before a write. See the module docs.
#[derive(Debug, Clone)]
pub struct VectorWriteMark {
    next_id: u32,
    growing_base_id: u32,
    /// Each named surrogate with the node bound to it, if any.
    bindings: Vec<(Surrogate, Option<u32>)>,
    /// Named nodes that were live.
    live: Vec<u32>,
    /// Each named multi-vector document with its node list, if any.
    multi_docs: Vec<(Surrogate, Option<Vec<u32>>)>,
}

impl VectorCollection {
    /// Whether node `id` exists and is not soft-deleted, whichever segment
    /// holds it.
    pub fn is_live(&self, id: u32) -> bool {
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return !self.growing.is_deleted(local);
            }
        }
        for seg in &self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return !seg.index.is_deleted(local);
                }
            }
        }
        for seg in &self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return !seg.flat.is_deleted(local);
                }
            }
        }
        false
    }

    /// Mark the state a write naming `surrogates` and node `ids` can change.
    pub fn write_mark(&self, surrogates: &[Surrogate], ids: &[u32]) -> VectorWriteMark {
        let mut bindings = Vec::with_capacity(surrogates.len() + ids.len());
        let mut live = Vec::new();
        let mut multi_docs = Vec::with_capacity(surrogates.len());
        for &surrogate in surrogates {
            let bound = self.surrogate_to_local.get(&surrogate).copied();
            bindings.push((surrogate, bound));
            if let Some(id) = bound
                && self.is_live(id)
            {
                live.push(id);
            }
            let doc = self.multi_doc_map.get(&surrogate).cloned();
            if let Some(doc_ids) = &doc {
                live.extend(doc_ids.iter().copied().filter(|id| self.is_live(*id)));
            }
            multi_docs.push((surrogate, doc));
        }
        for &id in ids {
            if !self.is_live(id) {
                continue;
            }
            live.push(id);
            if let Some(&surrogate) = self.surrogate_map.get(&id) {
                bindings.push((surrogate, Some(id)));
            }
        }
        VectorWriteMark {
            next_id: self.next_id,
            growing_base_id: self.growing_base_id,
            bindings,
            live,
            multi_docs,
        }
    }

    /// Put the collection back to `mark`. Returns `false`, changing nothing,
    /// when a seal moved the nodes inserted since the mark out of the growing
    /// segment.
    pub fn roll_back_to(&mut self, mark: VectorWriteMark) -> bool {
        if self.growing_base_id != mark.growing_base_id || mark.next_id < self.growing_base_id {
            return false;
        }
        for id in mark.next_id..self.next_id {
            if let Some(surrogate) = self.surrogate_map.remove(&id)
                && self.surrogate_to_local.get(&surrogate) == Some(&id)
            {
                self.surrogate_to_local.remove(&surrogate);
            }
        }
        self.growing
            .truncate((mark.next_id - self.growing_base_id) as usize);
        self.next_id = mark.next_id;

        for (surrogate, prior) in mark.bindings {
            if let Some(current) = self.surrogate_to_local.remove(&surrogate)
                && Some(current) != prior
            {
                self.surrogate_map.remove(&current);
            }
            if let Some(id) = prior {
                self.surrogate_to_local.insert(surrogate, id);
                self.surrogate_map.insert(id, surrogate);
            }
        }
        for (surrogate, prior) in mark.multi_docs {
            match prior {
                Some(ids) => {
                    for id in &ids {
                        self.surrogate_map.insert(*id, surrogate);
                    }
                    self.multi_doc_map.insert(surrogate, ids);
                }
                None => {
                    self.multi_doc_map.remove(&surrogate);
                }
            }
        }
        for id in mark.live {
            if !self.is_live(id) {
                self.undelete(id);
            }
        }
        true
    }

    /// An empty collection with this one's configuration and id counters:
    /// same dimension, parameters, index config, quantization, seal
    /// threshold, storage settings, registered payload fields and
    /// watermarks. The next insert takes the id this collection's next insert
    /// would have taken.
    pub fn detached_empty(&self) -> Self {
        let mut fresh = Self::with_seal_threshold_and_config(
            self.dim,
            self.index_config.clone(),
            self.seal_threshold,
        );
        fresh.params = self.params.clone();
        fresh.growing = FlatIndex::new(self.dim, self.params.metric);
        fresh.next_id = self.next_id;
        fresh.growing_base_id = self.next_id;
        fresh.next_segment_id = self.next_segment_id;
        fresh.data_dir = self.data_dir.clone();
        fresh.ram_budget_bytes = self.ram_budget_bytes;
        fresh.mmap_fallback_count = self.mmap_fallback_count;
        fresh.quantization = self.quantization;
        fresh.payload = self.payload.definitions_only();
        fresh.arena_index = self.arena_index;
        fresh.checkpoint_wal_lsn = self.checkpoint_wal_lsn;
        fresh.applied_wal_lsn = self.applied_wal_lsn;
        fresh
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
    fn a_rolled_back_insert_returns_the_id_counter_and_the_binding() {
        let mut coll = collection();
        coll.insert_with_surrogate(vec![0.1, 0.2], Surrogate::new(1));
        let mark = coll.write_mark(&[Surrogate::new(2)], &[]);
        coll.insert_with_surrogate(vec![0.3, 0.4], Surrogate::new(2));
        assert!(coll.roll_back_to(mark));
        assert_eq!(coll.local_for_surrogate(Surrogate::new(2)), None);
        assert_eq!(
            coll.insert_with_surrogate(vec![0.5, 0.6], Surrogate::new(3)),
            1,
            "the next insert takes the id the rolled-back insert took"
        );
    }

    #[test]
    fn a_rolled_back_rebind_restores_the_replaced_node() {
        let mut coll = collection();
        let first = coll.insert_with_surrogate(vec![0.1, 0.2], Surrogate::new(7));
        let mark = coll.write_mark(&[Surrogate::new(7)], &[]);
        coll.insert_with_surrogate(vec![0.3, 0.4], Surrogate::new(7));
        assert!(!coll.is_live(first));
        assert!(coll.roll_back_to(mark));
        assert!(coll.is_live(first));
        assert_eq!(coll.local_for_surrogate(Surrogate::new(7)), Some(first));
    }

    #[test]
    fn a_rolled_back_delete_restores_the_node_and_its_binding() {
        let mut coll = collection();
        let id = coll.insert_with_surrogate(vec![0.1, 0.2], Surrogate::new(4));
        let mark = coll.write_mark(&[], &[id]);
        coll.delete(id);
        assert!(coll.roll_back_to(mark));
        assert!(coll.is_live(id));
        assert_eq!(coll.local_for_surrogate(Surrogate::new(4)), Some(id));
    }

    #[test]
    fn a_seal_after_the_mark_refuses_the_rollback() {
        let mut coll = VectorCollection::with_seal_threshold(2, HnswParams::default(), 1);
        let mark = coll.write_mark(&[Surrogate::new(1)], &[]);
        coll.insert_with_surrogate(vec![0.1, 0.2], Surrogate::new(1));
        assert!(coll.seal("k").is_some());
        assert!(!coll.roll_back_to(mark));
    }

    #[test]
    fn a_detached_empty_collection_continues_the_id_counter() {
        let mut coll = collection();
        coll.insert(vec![0.1, 0.2]);
        coll.insert(vec![0.3, 0.4]);
        let mut fresh = coll.detached_empty();
        assert_eq!(fresh.live_count(), 0);
        assert_eq!(fresh.insert(vec![0.5, 0.6]), 2);
    }
}
