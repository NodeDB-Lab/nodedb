//! HNSW graph structure — nodes, parameters, core index operations.
//!
//! Production implementation per Malkov & Yashunin (2018).
//! FP32 construction for structural integrity; heuristic neighbor selection.

use crate::distance::distance;

// Re-export shared params from nodedb-types.
pub use nodedb_types::hnsw::HnswParams;

/// Hard cap on the layer assigned to any node during insertion.
/// Standard HNSW practice — prevents pathological RNG draws from inflating
/// `max_layer` and slowing every subsequent search.
pub const MAX_LAYER_CAP: usize = 16;

/// Result of a k-NN search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order).
    pub id: u32,
    /// Distance from the query vector.
    pub distance: f32,
}

/// A node in the HNSW graph.
pub struct Node {
    /// Full-precision vector data.
    pub vector: Vec<f32>,
    /// Neighbors at each layer this node participates in.
    pub neighbors: Vec<Vec<u32>>,
    /// Tombstone flag for soft-deletion.
    pub deleted: bool,
}

/// Hierarchical Navigable Small World graph index.
///
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4)
/// - Beam search with configurable ef parameter
pub struct HnswIndex {
    pub(crate) params: HnswParams,
    pub(crate) dim: usize,
    pub(crate) nodes: Vec<Node>,
    pub(crate) entry_point: Option<u32>,
    pub(crate) max_layer: usize,
    pub(crate) rng: Xorshift64,
    /// Flat neighbor storage for zero-copy access after checkpoint restore.
    /// When present, `neighbors_at()` reads from here instead of per-node Vecs.
    /// Cleared on first mutation (insert/delete).
    pub(crate) flat_neighbors: Option<crate::hnsw::flat_neighbors::FlatNeighborStore>,
}

impl HnswIndex {
    /// Get neighbors of a node at a specific layer.
    /// Uses flat zero-copy storage if available, otherwise per-node Vec.
    #[inline]
    pub(crate) fn neighbors_at(&self, node_id: u32, layer: usize) -> &[u32] {
        if let Some(ref flat) = self.flat_neighbors {
            return flat.neighbors_at(node_id, layer);
        }
        let node = &self.nodes[node_id as usize];
        if layer < node.neighbors.len() {
            &node.neighbors[layer]
        } else {
            &[]
        }
    }

    /// Number of layers a node participates in.
    #[inline]
    pub(crate) fn node_num_layers(&self, node_id: u32) -> usize {
        if let Some(ref flat) = self.flat_neighbors {
            return flat.num_layers(node_id);
        }
        self.nodes[node_id as usize].neighbors.len()
    }

    /// Ensure mutable per-node neighbor Vecs are available.
    /// Materializes flat storage back to per-node Vecs if needed.
    pub(crate) fn ensure_mutable_neighbors(&mut self) {
        if let Some(flat) = self.flat_neighbors.take() {
            let nested = flat.to_nested(self.nodes.len());
            for (i, layers) in nested.into_iter().enumerate() {
                self.nodes[i].neighbors = layers;
            }
        }
    }
}

/// Lightweight xorshift64 PRNG for layer assignment.
pub struct Xorshift64(pub u64);

impl Xorshift64 {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 as f64) / (u64::MAX as f64)
    }
}

/// Ordered candidate for priority queues during search and construction.
#[derive(Clone, Copy, PartialEq)]
pub struct Candidate {
    pub dist: f32,
    pub id: u32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.id.cmp(&other.id))
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(42),
            flat_neighbors: None,
            params,
        }
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(seed),
            flat_neighbors: None,
            params,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn live_count(&self) -> usize {
        self.nodes.len() - self.tombstone_count()
    }

    pub fn tombstone_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.deleted).count()
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
            return true;
        }
        false
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.nodes.get(id as usize).map(|n| n.vector.as_slice())
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    pub fn max_layer(&self) -> usize {
        self.max_layer
    }

    /// Current RNG state (for snapshot reproducibility).
    pub fn rng_state(&self) -> u64 {
        self.rng.0
    }

    /// Approximate memory usage in bytes (vector data + neighbor lists).
    pub fn memory_usage_bytes(&self) -> usize {
        let vector_bytes = self.nodes.len() * self.dim * std::mem::size_of::<f32>();
        let neighbor_bytes: usize = self
            .nodes
            .iter()
            .map(|n| {
                n.neighbors
                    .iter()
                    .map(|layer| layer.len() * 4)
                    .sum::<usize>()
            })
            .sum();
        let node_overhead = self.nodes.len() * std::mem::size_of::<Node>();
        vector_bytes + neighbor_bytes + node_overhead
    }

    /// Export all vectors for snapshot transfer.
    pub fn export_vectors(&self) -> Vec<Vec<f32>> {
        self.nodes.iter().map(|n| n.vector.clone()).collect()
    }

    /// Export all neighbor lists for snapshot transfer.
    pub fn export_neighbors(&self) -> Vec<Vec<Vec<u32>>> {
        self.nodes.iter().map(|n| n.neighbors.clone()).collect()
    }

    /// Assign a random layer using the exponential distribution.
    ///
    /// Capped at `MAX_LAYER_CAP` to prevent pathological RNG draws from
    /// promoting the index's `max_layer` to hundreds or thousands, which
    /// would make every search's Phase-1 greedy descent O(max_layer).
    pub(crate) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        let layer = (-r.ln() * ml).floor() as usize;
        layer.min(MAX_LAYER_CAP)
    }

    /// Compute distance between a query vector and a stored node.
    pub(crate) fn dist_to_node(&self, query: &[f32], node_id: u32) -> f32 {
        distance(
            query,
            &self.nodes[node_id as usize].vector,
            self.params.metric,
        )
    }

    /// Max neighbors allowed at a given layer.
    pub(crate) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }

    /// Compact the index by removing all tombstoned nodes.
    ///
    /// Returns the number of removed nodes. See `compact_with_map` for the
    /// variant that also returns the old→new id remapping.
    pub fn compact(&mut self) -> usize {
        self.compact_with_map().0
    }

    /// Compact and return both the removed count and the old→new id map.
    ///
    /// `id_map[old_local]` = new_local, or `u32::MAX` if the node was
    /// tombstoned (removed).
    pub fn compact_with_map(&mut self) -> (usize, Vec<u32>) {
        let tombstone_count = self.tombstone_count();
        if tombstone_count == 0 {
            let identity: Vec<u32> = (0..self.nodes.len() as u32).collect();
            return (0, identity);
        }
        self.ensure_mutable_neighbors();

        let mut id_map: Vec<u32> = Vec::with_capacity(self.nodes.len());
        let mut new_id = 0u32;
        for node in &self.nodes {
            if node.deleted {
                id_map.push(u32::MAX);
            } else {
                id_map.push(new_id);
                new_id += 1;
            }
        }

        let mut new_nodes: Vec<Node> = Vec::with_capacity(new_id as usize);
        for node in self.nodes.drain(..) {
            if node.deleted {
                continue;
            }
            let remapped_neighbors: Vec<Vec<u32>> = node
                .neighbors
                .into_iter()
                .map(|layer_neighbors| {
                    layer_neighbors
                        .into_iter()
                        .filter_map(|old_nid| {
                            let new_nid = id_map[old_nid as usize];
                            if new_nid == u32::MAX {
                                None
                            } else {
                                Some(new_nid)
                            }
                        })
                        .collect()
                })
                .collect();
            new_nodes.push(Node {
                vector: node.vector,
                neighbors: remapped_neighbors,
                deleted: false,
            });
        }

        self.entry_point = if let Some(old_ep) = self.entry_point {
            let new_ep = id_map[old_ep as usize];
            if new_ep == u32::MAX {
                new_nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, n)| n.neighbors.len())
                    .map(|(i, _)| i as u32)
            } else {
                Some(new_ep)
            }
        } else {
            None
        };

        self.max_layer = new_nodes
            .iter()
            .map(|n| n.neighbors.len().saturating_sub(1))
            .max()
            .unwrap_or(0);

        self.nodes = new_nodes;
        (tombstone_count, id_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;

    #[test]
    fn create_empty_index() {
        let idx = HnswIndex::new(3, HnswParams::default());
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.entry_point().is_none());
    }

    #[test]
    fn params_default() {
        let p = HnswParams::default();
        assert_eq!(p.m, 16);
        assert_eq!(p.m0, 32);
        assert_eq!(p.ef_construction, 200);
        assert_eq!(p.metric, DistanceMetric::Cosine);
    }

    #[test]
    fn candidate_ordering() {
        let a = Candidate { dist: 0.1, id: 1 };
        let b = Candidate { dist: 0.5, id: 2 };
        assert!(a < b);
    }
}
