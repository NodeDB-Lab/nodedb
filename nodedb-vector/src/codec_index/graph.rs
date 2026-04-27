//! `HnswCodecIndex<C>` — generic HNSW graph parameterised on any `VectorCodec`.
//!
//! Stores one `C::Quantized` payload per node instead of raw FP32 vectors.
//! All distance computations are delegated to the codec's
//! `fast_symmetric_distance` (routing) and `exact_asymmetric_distance`
//! (base-layer rerank).

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
        Sq8Codec::calibrate(&refs, dim)
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
}
