// SPDX-License-Identifier: Apache-2.0

//! `HnswCodecIndex<C>` — generic HNSW graph parameterised on any `VectorCodec`.
//!
//! Stores one `C::Quantized` payload per node instead of raw FP32 vectors.
//! All distance computations are delegated to the codec's
//! `fast_symmetric_distance` (routing) and `exact_asymmetric_distance`
//! (base-layer rerank).

use std::collections::HashMap;

use nodedb_codec::vector_quant::codec::VectorCodec;

/// Hard cap on the layer assigned to any node during insertion.
pub const MAX_LAYER_CAP: usize = 16;

/// A node in the codec-generic HNSW graph.
pub struct NodeC<C: VectorCodec> {
    /// External caller-supplied identifier.
    pub id: u32,
    /// Tombstone flag for soft-deletion.
    pub deleted: bool,
    /// Layer this node was inserted at (determines how many neighbor vecs exist).
    pub layer: usize,
    /// One quantized payload in the codec's packed form.
    pub quantized: C::Quantized,
    /// `neighbors[layer]` = adjacency at that layer.
    /// `neighbors[0]` is layer 0 (base); `neighbors[layer]` is the top.
    pub neighbors: Vec<Vec<u32>>,
}

/// Minimal xorshift64 for layer assignment — same generator as `HnswIndex`.
pub(crate) struct Xorshift64(pub u64);

impl Xorshift64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    #[inline]
    pub(crate) fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 as f64) / (u64::MAX as f64)
    }
}

/// Hierarchical Navigable Small World graph index parameterised on a
/// [`VectorCodec`].
///
/// - **Upper-layer routing**: `codec.fast_symmetric_distance`
/// - **Base-layer rerank**: `codec.exact_asymmetric_distance`
pub struct HnswCodecIndex<C: VectorCodec> {
    pub dim: usize,
    /// Max neighbors per node in upper layers.
    pub m: usize,
    /// Max neighbors per node at layer 0 (= `m * 2`).
    pub m0: usize,
    pub ef_construction: usize,
    /// Current highest layer in the index.
    pub max_layer: usize,
    /// Dense index (insertion order) of the current entry-point node.
    pub entry_point: Option<u32>,
    /// `1.0 / ln(m)` — used by `random_layer`.
    pub level_mult: f32,
    pub codec: C,
    /// Dense storage; index position == internal node index.
    pub(crate) nodes: Vec<NodeC<C>>,
    /// Caller id → dense node index.
    pub(crate) by_id: HashMap<u32, u32>,
    pub(crate) rng: Xorshift64,
}

impl<C: VectorCodec> HnswCodecIndex<C> {
    /// Create a new empty codec index.
    pub fn new(dim: usize, m: usize, ef_construction: usize, codec: C, seed: u64) -> Self {
        let m0 = m * 2;
        let level_mult = 1.0 / (m as f32).ln();
        Self {
            dim,
            m,
            m0,
            ef_construction,
            max_layer: 0,
            entry_point: None,
            level_mult,
            codec,
            nodes: Vec::new(),
            by_id: HashMap::new(),
            rng: Xorshift64::new(seed),
        }
    }

    /// Number of nodes (including deleted).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.iter().all(|n| n.deleted)
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Assign a random layer per the HNSW exponential distribution.
    ///
    /// Capped at `MAX_LAYER_CAP` to prevent pathological draws from inflating
    /// `max_layer`.
    pub fn random_layer(&mut self) -> usize {
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        let layer = (-r.ln() * self.level_mult as f64).floor() as usize;
        layer.min(MAX_LAYER_CAP)
    }

    /// Return a reference to the quantized payload at `idx`, if present and
    /// not deleted.
    pub fn quantized_at(&self, idx: u32) -> Option<&C::Quantized> {
        self.nodes
            .get(idx as usize)
            .filter(|n| !n.deleted)
            .map(|n| &n.quantized)
    }

    /// Return the neighbor slice for `idx` at `layer`.
    pub fn neighbors_at(&self, idx: u32, layer: usize) -> &[u32] {
        let node = &self.nodes[idx as usize];
        if layer < node.neighbors.len() {
            &node.neighbors[layer]
        } else {
            &[]
        }
    }

    /// Max neighbors allowed at `layer`.
    pub(crate) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 { self.m0 } else { self.m }
    }

    /// Mark the node inserted under `id` deleted or live. `false` when no
    /// node carries `id`.
    pub fn set_deleted(&mut self, id: u32, deleted: bool) -> bool {
        match self.by_id.get(&id).copied() {
            Some(idx) => match self.nodes.get_mut(idx as usize) {
                Some(node) => {
                    node.deleted = deleted;
                    true
                }
                None => false,
            },
            None => false,
        }
    }

    /// Rewrite node ids through `id_map` (old id → new id, `u32::MAX` for a
    /// removed node). A removed node stays in the graph as a stale routing
    /// node: deleted, carrying no id, and never remapped again. Only nodes
    /// that carry an id follow the map, so a stale node never takes a live
    /// node's new id.
    pub fn remap_ids(&mut self, id_map: &[u32]) {
        let mut by_id = HashMap::with_capacity(self.by_id.len());
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            let carries_id = self.by_id.get(&node.id) == Some(&(idx as u32));
            let new_id = id_map.get(node.id as usize).copied();
            match new_id {
                Some(new_id) if carries_id && new_id != u32::MAX => {
                    node.id = new_id;
                    by_id.insert(new_id, idx as u32);
                }
                Some(_) | None => {
                    node.id = u32::MAX;
                    node.deleted = true;
                }
            }
        }
        self.by_id = by_id;
    }

    /// Stale routing nodes: nodes a compaction removed that carry no id.
    pub fn stale_count(&self) -> usize {
        self.nodes.len().saturating_sub(self.by_id.len())
    }

    /// Bytes the graph holds: nodes, quantized payloads, adjacency and the
    /// id map. Walks every node.
    pub fn memory_usage_bytes(&self) -> usize {
        let nodes: usize = self
            .nodes
            .iter()
            .map(|n| {
                let adjacency: usize = n
                    .neighbors
                    .iter()
                    .map(|layer| {
                        std::mem::size_of::<Vec<u32>>()
                            + layer.capacity() * std::mem::size_of::<u32>()
                    })
                    .sum();
                std::mem::size_of::<NodeC<C>>() + n.quantized.as_ref().as_bytes().len() + adjacency
            })
            .sum();
        let id_map = self.by_id.capacity() * 2 * std::mem::size_of::<u32>();
        nodes + id_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantize::Sq8Codec;

    fn make_sq8(dim: usize) -> Sq8Codec {
        let vecs: Vec<Vec<f32>> = (0..20)
            .map(|i| (0..dim).map(|d| (i * dim + d) as f32 * 0.1).collect())
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        Sq8Codec::calibrate(&refs, dim).unwrap()
    }

    #[test]
    fn new_index_is_empty() {
        let codec = make_sq8(4);
        let idx: HnswCodecIndex<Sq8Codec> = HnswCodecIndex::new(4, 16, 100, codec, 42);
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.entry_point.is_none());
        assert_eq!(idx.m0, idx.m * 2);
    }

    #[test]
    fn random_layer_is_bounded() {
        let codec = make_sq8(4);
        let mut idx: HnswCodecIndex<Sq8Codec> = HnswCodecIndex::new(4, 16, 100, codec, 123);
        for _ in 0..1000 {
            assert!(idx.random_layer() <= MAX_LAYER_CAP);
        }
    }

    fn filled(n: u32) -> HnswCodecIndex<Sq8Codec> {
        let mut idx: HnswCodecIndex<Sq8Codec> = HnswCodecIndex::new(4, 8, 64, make_sq8(4), 9);
        for i in 0..n {
            idx.insert(i, &[i as f32 * 0.1, 0.0, 0.0, 0.0]).unwrap();
        }
        idx
    }

    #[test]
    fn set_deleted_hides_and_restores_a_node() {
        let mut idx = filled(20);
        let query = [0.5f32, 0.0, 0.0, 0.0];
        assert_eq!(idx.search(&query, 1, 32).unwrap()[0].id, 5);
        assert!(idx.set_deleted(5, true));
        assert!(
            idx.search(&query, 20, 32)
                .unwrap()
                .iter()
                .all(|r| r.id != 5)
        );
        assert!(idx.set_deleted(5, false));
        assert_eq!(idx.search(&query, 1, 32).unwrap()[0].id, 5);
        assert!(!idx.set_deleted(99, true), "no node carries id 99");
    }

    #[test]
    fn remap_ids_renumbers_results() {
        let mut idx = filled(10);
        // Node 3 goes; every later id moves down by one.
        let id_map: Vec<u32> = (0..10u32)
            .map(|i| match i {
                3 => u32::MAX,
                i if i > 3 => i - 1,
                i => i,
            })
            .collect();
        idx.remap_ids(&id_map);
        let hits = idx.search(&[0.7, 0.0, 0.0, 0.0], 10, 32).unwrap();
        assert_eq!(hits.len(), 9, "the removed node stays deleted");
        assert_eq!(hits[0].id, 6, "old id 7 is new id 6");
        assert!(idx.set_deleted(8, true), "old id 9 answers to 8");
        assert!(!idx.set_deleted(9, true), "no node keeps id 9");
        assert_eq!(idx.stale_count(), 1);
    }

    #[test]
    fn a_stale_node_never_takes_a_live_id_in_a_later_remap() {
        let mut idx = filled(10);
        // Node 0 goes; every later id moves down by one.
        let first: Vec<u32> = (0..10u32)
            .map(|i| if i == 0 { u32::MAX } else { i - 1 })
            .collect();
        idx.remap_ids(&first);
        // Nothing goes: the identity map over the nine live ids.
        let second: Vec<u32> = (0..9u32).collect();
        idx.remap_ids(&second);
        assert_eq!(idx.stale_count(), 1);
        // Deleting id 0 (old id 1) hides that node, not the stale one.
        assert!(idx.set_deleted(0, true));
        let hits = idx.search(&[0.1, 0.0, 0.0, 0.0], 10, 32).unwrap();
        assert!(hits.iter().all(|h| h.id != 0), "{hits:?}");
        assert_eq!(hits.len(), 8);
    }
}
