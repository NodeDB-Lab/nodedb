// SPDX-License-Identifier: Apache-2.0

//! Soft deletion and the counts derived from it.
//!
//! Deleting a node marks it rather than removing it, because removal would
//! renumber every id the graph's neighbor lists refer to. The tombstone ratio
//! is what compaction watches to decide when the graph is worth rebuilding.

use super::state::HnswIndex;

impl HnswIndex {
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn live_count(&self) -> usize {
        self.nodes.len() - self.tombstone_count()
    }

    pub fn tombstone_count(&self) -> usize {
        self.tombstones
    }

    /// Tombstone ratio: fraction of nodes that are deleted.
    pub fn tombstone_ratio(&self) -> f64 {
        if self.nodes.is_empty() {
            0.0
        } else {
            self.tombstone_count() as f64 / self.nodes.len() as f64
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    /// Soft-delete a vector by internal node ID.
    pub fn delete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize) {
            if node.deleted {
                return false;
            }
            node.deleted = true;
            self.tombstones += 1;
            true
        } else {
            false
        }
    }

    pub fn is_deleted(&self, id: u32) -> bool {
        self.nodes.get(id as usize).is_none_or(|n| n.deleted)
    }

    pub fn undelete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize)
            && node.deleted
        {
            node.deleted = false;
            self.tombstones -= 1;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn index_of(n: usize) -> HnswIndex {
        let mut idx = HnswIndex::with_seed(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
            7,
        );
        for i in 0..n {
            idx.insert(vec![i as f32, 0.0]).unwrap();
        }
        idx
    }

    /// The live count follows every delete and undelete, and a repeated
    /// delete or undelete changes nothing.
    #[test]
    fn live_count_follows_delete_and_undelete() {
        let mut idx = index_of(10);
        assert_eq!(idx.live_count(), 10);
        assert!(idx.delete(3));
        assert!(!idx.delete(3));
        assert!(idx.delete(4));
        assert_eq!(idx.tombstone_count(), 2);
        assert_eq!(idx.live_count(), 8);
        assert!(idx.undelete(3));
        assert!(!idx.undelete(3));
        assert_eq!(idx.tombstone_count(), 1);
        assert_eq!(idx.live_count(), 9);
    }

    /// An index with every node deleted is empty, and compaction leaves no
    /// tombstone behind.
    #[test]
    fn compaction_clears_the_tombstone_count() {
        let mut idx = index_of(10);
        for id in 0..5 {
            idx.delete(id);
        }
        let dropped = idx.compact();
        assert_eq!(dropped, 5);
        assert_eq!(idx.tombstone_count(), 0);
        assert_eq!(idx.live_count(), 5);
        assert!(!idx.is_empty());
        for id in 0..5 {
            idx.delete(id);
        }
        assert!(idx.is_empty());
    }
}
