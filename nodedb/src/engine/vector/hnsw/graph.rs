use serde::{Deserialize, Serialize};

use super::super::distance::{DistanceMetric, distance};

/// Serializable HNSW snapshot for checkpointing.
#[derive(Serialize, Deserialize)]
struct HnswSnapshot {
    dim: usize,
    params_m: usize,
    params_m0: usize,
    params_ef_construction: usize,
    params_metric: u8,
    entry_point: Option<u32>,
    max_layer: usize,
    rng_state: u64,
    nodes: Vec<NodeSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct NodeSnapshot {
    vector: Vec<f32>,
    neighbors: Vec<Vec<u32>>,
    deleted: bool,
}

/// HNSW index parameters.
#[derive(Debug, Clone)]
pub struct HnswParams {
    /// Max bidirectional connections per node at layers > 0.
    pub m: usize,
    /// Max connections at layer 0 (typically 2*M for denser base layer).
    pub m0: usize,
    /// Dynamic candidate list size during construction. Higher = better
    /// recall at the cost of slower inserts.
    pub ef_construction: usize,
    /// Distance metric for similarity computation.
    pub metric: DistanceMetric,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
        }
    }
}

/// Result of a k-NN search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order).
    pub id: u32,
    /// Distance from the query vector under the configured metric.
    pub distance: f32,
}

/// A node in the HNSW graph.
///
/// Stores the full-precision vector (FP32) for structural integrity during
/// construction, and per-layer neighbor lists.
pub(super) struct Node {
    /// Full-precision vector data.
    pub vector: Vec<f32>,
    /// Neighbors at each layer this node participates in.
    /// `neighbors[layer]` is the list of neighbor node IDs at that layer.
    pub neighbors: Vec<Vec<u32>>,
    /// Tombstone flag. Soft-deleted nodes are excluded from search results
    /// and neighbor selection, but remain in the graph for navigation.
    /// This preserves graph connectivity without expensive restructuring.
    pub deleted: bool,
}

/// Hierarchical Navigable Small World graph index.
///
/// Production implementation per Malkov & Yashunin (2018):
/// - Multi-layer graph with exponential layer assignment
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4) for diverse connectivity
/// - Beam search with configurable ef parameter
/// - Roaring bitmap pre-filtering for HNSW traversal
///
/// This type is intentionally `!Send` — owned by a single Data Plane core.
pub struct HnswIndex {
    pub(super) params: HnswParams,
    pub(super) dim: usize,
    pub(super) nodes: Vec<Node>,
    pub(super) entry_point: Option<u32>,
    pub(super) max_layer: usize,
    pub(super) rng: Xorshift64,
}

/// Lightweight xorshift64 PRNG for layer assignment. No external dependency.
pub(super) struct Xorshift64(u64);

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

/// Ordered candidate used in priority queues during search and construction.
///
/// Implements `Ord` by distance (ascending), then by id for stability.
/// Rust's `BinaryHeap` is a max-heap, so use `Reverse<Candidate>` for min-heap.
#[derive(Clone, Copy, PartialEq)]
pub(super) struct Candidate {
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
    /// Create a new empty HNSW index for vectors of the given dimensionality.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(42),
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
            params,
        }
    }

    /// Return the total number of nodes (including tombstoned).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return the number of live (non-deleted) vectors.
    pub fn live_count(&self) -> usize {
        self.nodes.len() - self.tombstone_count()
    }

    /// Return the number of tombstoned (soft-deleted) vectors.
    pub fn tombstone_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.deleted).count()
    }

    /// Tombstone ratio: fraction of nodes that are deleted.
    /// High ratio (>0.3) indicates the index needs compaction.
    pub fn tombstone_ratio(&self) -> f64 {
        if self.nodes.is_empty() {
            0.0
        } else {
            self.tombstone_count() as f64 / self.nodes.len() as f64
        }
    }

    /// Check whether the index contains no live vectors.
    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    /// Soft-delete a vector by internal node ID.
    ///
    /// The node is marked as tombstoned: it is excluded from search results
    /// and future neighbor selection, but its position in the graph is
    /// preserved for navigation. This is O(1) — no graph restructuring.
    ///
    /// Returns `true` if the node was found and deleted, `false` if the ID
    /// is out of range or already deleted.
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

    /// Check whether a node is tombstoned.
    pub fn is_deleted(&self, id: u32) -> bool {
        self.nodes.get(id as usize).is_none_or(|n| n.deleted)
    }

    /// Un-delete a previously soft-deleted node, clearing the tombstone flag.
    ///
    /// Used for transaction rollback when a VectorDelete needs to be undone.
    /// Returns `true` if the node was found and un-deleted, `false` if the
    /// ID is out of range or wasn't deleted.
    pub fn undelete(&mut self, id: u32) -> bool {
        if let Some(node) = self.nodes.get_mut(id as usize)
            && node.deleted
        {
            node.deleted = false;
            return true;
        }
        false
    }

    /// Return the vector dimensionality this index was created for.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Retrieve a stored vector by node ID.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.nodes.get(id as usize).map(|n| n.vector.as_slice())
    }

    /// Access the index parameters.
    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Current entry point node ID.
    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    /// Highest layer in the graph.
    pub fn max_layer(&self) -> usize {
        self.max_layer
    }

    /// Current RNG state (for snapshot reproducibility).
    pub fn rng_state(&self) -> u64 {
        self.rng.0
    }

    /// Export all vectors for snapshot transfer.
    pub fn export_vectors(&self) -> Vec<Vec<f32>> {
        self.nodes.iter().map(|n| n.vector.clone()).collect()
    }

    /// Export all neighbor lists for snapshot transfer.
    pub fn export_neighbors(&self) -> Vec<Vec<Vec<u32>>> {
        self.nodes.iter().map(|n| n.neighbors.clone()).collect()
    }

    /// Serialize the entire index to bytes for checkpointing.
    ///
    /// The snapshot includes all vectors, neighbor lists, deleted flags,
    /// params, entry point, max layer, and RNG state. On reload, the
    /// index is fully reconstructed without WAL replay.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = HnswSnapshot {
            dim: self.dim,
            params_m: self.params.m,
            params_m0: self.params.m0,
            params_ef_construction: self.params.ef_construction,
            params_metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            nodes: self
                .nodes
                .iter()
                .map(|n| NodeSnapshot {
                    vector: n.vector.clone(),
                    neighbors: n.neighbors.clone(),
                    deleted: n.deleted,
                })
                .collect(),
        };
        rmp_serde::to_vec_named(&snapshot).unwrap_or_default()
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Returns `None` if the snapshot is invalid or corrupt.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snapshot: HnswSnapshot = rmp_serde::from_slice(bytes).ok()?;
        let metric = match snapshot.params_metric {
            0 => super::super::distance::DistanceMetric::L2,
            1 => super::super::distance::DistanceMetric::Cosine,
            2 => super::super::distance::DistanceMetric::InnerProduct,
            _ => super::super::distance::DistanceMetric::Cosine,
        };
        let params = HnswParams {
            m: snapshot.params_m,
            m0: snapshot.params_m0,
            ef_construction: snapshot.params_ef_construction,
            metric,
        };
        let nodes: Vec<Node> = snapshot
            .nodes
            .into_iter()
            .map(|n| Node {
                vector: n.vector,
                neighbors: n.neighbors,
                deleted: n.deleted,
            })
            .collect();

        Some(Self {
            dim: snapshot.dim,
            params,
            nodes,
            entry_point: snapshot.entry_point,
            max_layer: snapshot.max_layer,
            rng: Xorshift64::new(snapshot.rng_state),
        })
    }

    /// Assign a random layer for a new node using the exponential distribution.
    /// layer = floor(-ln(uniform()) * m_L) where m_L = 1 / ln(M).
    pub(super) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        (-r.ln() * ml).floor() as usize
    }

    /// Compute distance between a query vector and a stored node.
    pub(super) fn dist_to_node(&self, query: &[f32], node_id: u32) -> f32 {
        distance(
            query,
            &self.nodes[node_id as usize].vector,
            self.params.metric,
        )
    }

    /// Compact the index by removing all tombstoned nodes and reclaiming memory.
    ///
    /// Rebuilds the node array with only live nodes, remapping all neighbor
    /// IDs to new dense indices. The old `Vec<Node>` — including all deleted
    /// nodes' `Vec<f32>` vector data — is dropped, freeing the jemalloc
    /// arena memory.
    ///
    /// At 768-dim FP32 (~3 KiB per vector), 90M deleted vectors waste ~270 GB.
    /// This method reclaims that memory in a single atomic swap.
    ///
    /// Returns the number of nodes removed.
    pub fn compact(&mut self) -> usize {
        let tombstone_count = self.tombstone_count();
        if tombstone_count == 0 {
            return 0;
        }

        // Build old_id → new_id mapping. Deleted nodes map to u32::MAX.
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

        // Build new nodes vector with remapped neighbor IDs.
        let mut new_nodes: Vec<Node> = Vec::with_capacity(new_id as usize);
        for node in self.nodes.drain(..) {
            if node.deleted {
                // Drop the node — its Vec<f32> vector data is freed here.
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
                                None // Neighbor was deleted — remove edge.
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

        // Update entry point.
        self.entry_point = if let Some(old_ep) = self.entry_point {
            let new_ep = id_map[old_ep as usize];
            if new_ep == u32::MAX {
                // Entry point was deleted. Promote the node at the highest layer.
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

        // Recalculate max_layer from the remaining nodes.
        self.max_layer = new_nodes
            .iter()
            .map(|n| n.neighbors.len().saturating_sub(1))
            .max()
            .unwrap_or(0);

        self.nodes = new_nodes;
        tombstone_count
    }

    /// Max neighbors allowed at a given layer.
    pub(super) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }
}
