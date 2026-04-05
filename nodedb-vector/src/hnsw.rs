//! HNSW graph structure — nodes, parameters, checkpoint serialization.
//!
//! Production implementation per Malkov & Yashunin (2018).
//! Adapted for edge devices: no SIMD runtime dispatch, no Roaring bitmap
//! (those are in the Origin). Pure scalar distance functions.

use crate::distance::{DistanceMetric, distance};

// Re-export shared params from nodedb-types.
pub use nodedb_types::hnsw::HnswParams;

/// Result of a k-NN search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order).
    pub id: u32,
    /// Distance from the query vector.
    pub distance: f32,
}

/// A node in the HNSW graph.
pub(crate) struct Node {
    /// Full-precision vector data.
    pub vector: Vec<f32>,
    /// Neighbors at each layer this node participates in.
    pub neighbors: Vec<Vec<u32>>,
    /// Tombstone flag for soft-deletion.
    pub deleted: bool,
}

/// Magic header for rkyv-serialized HNSW snapshots (6 bytes).
const HNSW_RKYV_MAGIC: &[u8; 6] = b"RKHNS\0";

/// rkyv-serialized HNSW snapshot. SoA layout for better rkyv compatibility
/// (flat Vecs instead of Vec<struct>).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct HnswSnapshotRkyv {
    dim: usize,
    m: usize,
    m0: usize,
    ef_construction: usize,
    metric: u8,
    entry_point: Option<u32>,
    max_layer: usize,
    rng_state: u64,
    /// Per-node vectors (SoA).
    node_vectors: Vec<Vec<f32>>,
    /// Per-node neighbor lists (SoA).
    node_neighbors: Vec<Vec<Vec<u32>>>,
    /// Per-node deleted flags (SoA).
    node_deleted: Vec<bool>,
}

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
    pub(crate) flat_neighbors: Option<crate::flat_neighbors::FlatNeighborStore>,
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
pub(crate) struct Xorshift64(pub u64);

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
pub(crate) struct Candidate {
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

    /// Serialize the index to MessagePack bytes for storage.
    ///
    /// NOTE: The checklist mandates rkyv for engine blobs, but HNSW's
    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// rkyv 0.8 supports `Vec<Vec<u32>>` with the `alloc` feature.
    /// Magic header `RKHNS\0` allows `from_checkpoint` to detect format.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            node_vectors: self.nodes.iter().map(|n| n.vector.clone()).collect(),
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("HNSW rkyv serialization should not fail");
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.extend_from_slice(&rkyv_bytes);
        buf
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Auto-detects format: rkyv (magic `RKHNS\0`) or legacy MessagePack.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > HNSW_RKYV_MAGIC.len() && &bytes[..HNSW_RKYV_MAGIC.len()] == HNSW_RKYV_MAGIC
        {
            return Self::from_rkyv_checkpoint(&bytes[HNSW_RKYV_MAGIC.len()..]);
        }
        Self::from_msgpack_checkpoint(bytes)
    }

    /// Restore from rkyv-serialized bytes.
    fn from_rkyv_checkpoint(bytes: &[u8]) -> Option<Self> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let snap: HnswSnapshotRkyv =
            rkyv::from_bytes::<HnswSnapshotRkyv, rkyv::rancor::Error>(&aligned).ok()?;
        Self::from_hnsw_snapshot(snap)
    }

    /// Restore from legacy MessagePack bytes.
    fn from_msgpack_checkpoint(bytes: &[u8]) -> Option<Self> {
        use zerompk::{FromMessagePack, ToMessagePack};

        #[derive(ToMessagePack, FromMessagePack)]
        struct Snapshot {
            dim: usize,
            m: usize,
            m0: usize,
            ef_construction: usize,
            metric: u8,
            entry_point: Option<u32>,
            max_layer: usize,
            rng_state: u64,
            nodes: Vec<NodeSnap>,
        }

        #[derive(ToMessagePack, FromMessagePack)]
        struct NodeSnap {
            vector: Vec<f32>,
            neighbors: Vec<Vec<u32>>,
            deleted: bool,
        }

        let snap: Snapshot = zerompk::from_msgpack(bytes).ok()?;
        Self::from_hnsw_snapshot(HnswSnapshotRkyv {
            dim: snap.dim,
            m: snap.m,
            m0: snap.m0,
            ef_construction: snap.ef_construction,
            metric: snap.metric,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng_state: snap.rng_state,
            node_vectors: snap.nodes.iter().map(|n| n.vector.clone()).collect(),
            node_neighbors: snap.nodes.iter().map(|n| n.neighbors.clone()).collect(),
            node_deleted: snap.nodes.iter().map(|n| n.deleted).collect(),
        })
    }

    /// Reconstruct HnswIndex from deserialized snapshot fields.
    fn from_hnsw_snapshot(snap: HnswSnapshotRkyv) -> Option<Self> {
        let metric = match snap.metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };

        // Build flat neighbor store from SoA data (avoids per-node Vec<Vec<u32>>
        // allocations — millions of small Vecs on large graphs).
        let flat = crate::flat_neighbors::FlatNeighborStore::from_nested(&snap.node_neighbors);

        // Nodes only need vector + deleted; neighbors are in flat store.
        let nodes: Vec<Node> = snap
            .node_vectors
            .into_iter()
            .zip(snap.node_deleted)
            .map(|(vector, deleted)| Node {
                vector,
                neighbors: Vec::new(), // Empty — flat store handles reads.
                deleted,
            })
            .collect();

        Some(Self {
            dim: snap.dim,
            params: HnswParams {
                m: snap.m,
                m0: snap.m0,
                ef_construction: snap.ef_construction,
                metric,
            },
            nodes,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng: Xorshift64::new(snap.rng_state),
            flat_neighbors: Some(flat),
        })
    }

    /// Assign a random layer using the exponential distribution.
    pub(crate) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        (-r.ln() * ml).floor() as usize
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
    pub fn compact(&mut self) -> usize {
        let tombstone_count = self.tombstone_count();
        if tombstone_count == 0 {
            return 0;
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
        tombstone_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
