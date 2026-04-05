//! Flat neighbor storage for zero-copy HNSW checkpoint restore.
//!
//! After cold start from an rkyv checkpoint, neighbor lists are stored in a
//! flat contiguous layout that can be zero-copy on little-endian platforms.
//! This avoids allocating `Vec<Vec<u32>>` per node (millions of small Vecs).
//!
//! When the first insert/delete happens post-restore, the flat store is
//! materialized back into per-node `Vec<Vec<u32>>` and dropped.
//!
//! Layout (CSR-like, two levels):
//! ```text
//! data:    [n0_l0_neighbors..., n0_l1_neighbors..., n1_l0_neighbors..., ...]
//! offsets: [0, |n0_l0|, |n0_l0|+|n0_l1|, ..., |n0|+|n1_l0|, ...]
//! starts:  [0, n0_num_layers, n0_num_layers+n1_num_layers, ...]
//! ```

use std::sync::Arc;

/// Flat neighbor store for zero-copy HNSW access.
pub(crate) struct FlatNeighborStore {
    /// All neighbor IDs concatenated across all nodes and layers.
    data: FlatArray,
    /// Cumulative offsets into `data` for each layer of each node.
    /// For node `i` at layer `l`: neighbors = `data[offsets[starts[i]+l]..offsets[starts[i]+l+1]]`
    offsets: FlatArray,
    /// Per-node start index into `offsets`. Length: `num_nodes + 1`.
    /// `starts[i]..starts[i+1]` = layer range for node `i` in `offsets`.
    starts: Vec<u32>,
}

/// A flat u32 array backed by either owned memory or a shared buffer.
#[allow(dead_code)]
enum FlatArray {
    Owned(Vec<u32>),
    #[cfg(target_endian = "little")]
    ZeroCopy {
        _backing: Arc<rkyv::util::AlignedVec>,
        ptr: *const u32,
        len: usize,
    },
}

unsafe impl Send for FlatArray {}
unsafe impl Sync for FlatArray {}

impl std::ops::Deref for FlatArray {
    type Target = [u32];
    fn deref(&self) -> &[u32] {
        match self {
            Self::Owned(v) => v,
            #[cfg(target_endian = "little")]
            Self::ZeroCopy { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }
}

impl FlatNeighborStore {
    /// Build from per-node neighbor lists (owned data).
    pub fn from_nested(neighbors: &[Vec<Vec<u32>>]) -> Self {
        let num_nodes = neighbors.len();
        let mut starts = Vec::with_capacity(num_nodes + 1);
        let mut offsets = Vec::new();
        let mut data = Vec::new();

        for node_neighbors in neighbors {
            starts.push(offsets.len() as u32);
            let mut pos = data.len() as u32;
            for layer_neighbors in node_neighbors {
                offsets.push(pos);
                data.extend_from_slice(layer_neighbors);
                pos = data.len() as u32;
            }
            // Sentinel: end of last layer for this node.
            offsets.push(pos);
        }
        starts.push(offsets.len() as u32);

        Self {
            data: FlatArray::Owned(data),
            offsets: FlatArray::Owned(offsets),
            starts,
        }
    }

    /// Build from rkyv-archived flat arrays (zero-copy on little-endian).
    #[cfg(target_endian = "little")]
    #[allow(dead_code)]
    pub unsafe fn from_rkyv(
        backing: Arc<rkyv::util::AlignedVec>,
        data_ptr: *const u32,
        data_len: usize,
        offsets_ptr: *const u32,
        offsets_len: usize,
        starts: Vec<u32>,
    ) -> Self {
        Self {
            data: FlatArray::ZeroCopy {
                _backing: backing.clone(),
                ptr: data_ptr,
                len: data_len,
            },
            offsets: FlatArray::ZeroCopy {
                _backing: backing,
                ptr: offsets_ptr,
                len: offsets_len,
            },
            starts,
        }
    }

    /// Number of layers for a given node.
    pub fn num_layers(&self, node_id: u32) -> usize {
        let s = self.starts[node_id as usize] as usize;
        let e = self.starts[node_id as usize + 1] as usize;
        // Number of layers = number of offset entries - 1 (sentinel).
        if e > s { e - s - 1 } else { 0 }
    }

    /// Get neighbors of a node at a specific layer.
    pub fn neighbors_at(&self, node_id: u32, layer: usize) -> &[u32] {
        let layer_base = self.starts[node_id as usize] as usize;
        let num_layers = self.num_layers(node_id);
        if layer >= num_layers {
            return &[];
        }
        let start = self.offsets[layer_base + layer] as usize;
        let end = self.offsets[layer_base + layer + 1] as usize;
        &self.data[start..end]
    }

    /// Materialize back into per-node Vec<Vec<u32>>.
    pub fn to_nested(&self, num_nodes: usize) -> Vec<Vec<Vec<u32>>> {
        let mut result = Vec::with_capacity(num_nodes);
        for i in 0..num_nodes {
            let nl = self.num_layers(i as u32);
            let mut layers = Vec::with_capacity(nl);
            for l in 0..nl {
                layers.push(self.neighbors_at(i as u32, l).to_vec());
            }
            result.push(layers);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let neighbors: Vec<Vec<Vec<u32>>> = vec![vec![], vec![]];
        let flat = FlatNeighborStore::from_nested(&neighbors);
        assert_eq!(flat.num_layers(0), 0);
        assert_eq!(flat.num_layers(1), 0);
        let nested = flat.to_nested(2);
        assert_eq!(nested, neighbors);
    }

    #[test]
    fn roundtrip_basic() {
        let neighbors = vec![
            vec![vec![1, 2, 3], vec![1]], // node 0: 2 layers
            vec![vec![0, 2]],             // node 1: 1 layer
            vec![vec![0, 1], vec![0]],    // node 2: 2 layers
        ];
        let flat = FlatNeighborStore::from_nested(&neighbors);

        assert_eq!(flat.num_layers(0), 2);
        assert_eq!(flat.num_layers(1), 1);
        assert_eq!(flat.num_layers(2), 2);

        assert_eq!(flat.neighbors_at(0, 0), &[1, 2, 3]);
        assert_eq!(flat.neighbors_at(0, 1), &[1]);
        assert_eq!(flat.neighbors_at(1, 0), &[0, 2]);
        assert_eq!(flat.neighbors_at(2, 0), &[0, 1]);
        assert_eq!(flat.neighbors_at(2, 1), &[0]);

        // Out of bounds layer returns empty.
        assert_eq!(flat.neighbors_at(1, 5), &[] as &[u32]);

        let nested = flat.to_nested(3);
        assert_eq!(nested, neighbors);
    }
}
