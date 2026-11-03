// SPDX-License-Identifier: Apache-2.0

//! A quantized HNSW graph over one sealed segment, one variant per codec.

use nodedb_codec::vector_quant::bbq::BbqCodec;
use nodedb_codec::vector_quant::opq::OpqCodec;
use nodedb_codec::vector_quant::rabitq::RaBitQCodec;
use nodedb_codec::vector_quant::ternary::TernaryCodec;

use crate::codec_index::{CodecSearchResult, HnswCodecIndex};
use crate::error::VectorError;
use crate::quantize::BinaryCodec;

/// A codec graph whose node ids are the segment's local node ids.
pub enum CodecGraph {
    RaBitQ(HnswCodecIndex<RaBitQCodec>),
    Bbq(HnswCodecIndex<BbqCodec>),
    Binary(HnswCodecIndex<BinaryCodec>),
    Ternary(HnswCodecIndex<TernaryCodec>),
    Opq(HnswCodecIndex<OpqCodec>),
}

impl CodecGraph {
    /// The `k` live nodes nearest by the codec estimate, searched at beam
    /// `ef`. Distances are codec estimates: callers rerank them exactly.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<Vec<CodecSearchResult>, VectorError> {
        match self {
            Self::RaBitQ(g) => g.search(query, k, ef),
            Self::Bbq(g) => g.search(query, k, ef),
            Self::Binary(g) => g.search(query, k, ef),
            Self::Ternary(g) => g.search(query, k, ef),
            Self::Opq(g) => g.search(query, k, ef),
        }
    }

    /// Mark node `id` deleted or live. `false` when no node carries `id`.
    pub fn set_deleted(&mut self, id: u32, deleted: bool) -> bool {
        match self {
            Self::RaBitQ(g) => g.set_deleted(id, deleted),
            Self::Bbq(g) => g.set_deleted(id, deleted),
            Self::Binary(g) => g.set_deleted(id, deleted),
            Self::Ternary(g) => g.set_deleted(id, deleted),
            Self::Opq(g) => g.set_deleted(id, deleted),
        }
    }

    /// Rewrite node ids through a compaction's `id_map`.
    pub fn remap_ids(&mut self, id_map: &[u32]) {
        match self {
            Self::RaBitQ(g) => g.remap_ids(id_map),
            Self::Bbq(g) => g.remap_ids(id_map),
            Self::Binary(g) => g.remap_ids(id_map),
            Self::Ternary(g) => g.remap_ids(id_map),
            Self::Opq(g) => g.remap_ids(id_map),
        }
    }

    /// Nodes the graph holds, stale routing nodes included.
    pub fn len(&self) -> usize {
        match self {
            Self::RaBitQ(g) => g.len(),
            Self::Bbq(g) => g.len(),
            Self::Binary(g) => g.len(),
            Self::Ternary(g) => g.len(),
            Self::Opq(g) => g.len(),
        }
    }

    /// Whether the graph holds no node.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stale routing nodes: nodes a compaction removed.
    pub fn stale_count(&self) -> usize {
        match self {
            Self::RaBitQ(g) => g.stale_count(),
            Self::Bbq(g) => g.stale_count(),
            Self::Binary(g) => g.stale_count(),
            Self::Ternary(g) => g.stale_count(),
            Self::Opq(g) => g.stale_count(),
        }
    }

    /// Bytes the graph and its codec parameters hold in RAM.
    pub fn memory_usage_bytes(&self) -> usize {
        let f32_bytes = std::mem::size_of::<f32>();
        match self {
            // The centroid holds one f32 per dimension.
            Self::RaBitQ(g) => g.memory_usage_bytes() + g.codec.dim * f32_bytes,
            Self::Bbq(g) => g.memory_usage_bytes() + g.codec.dim * f32_bytes,
            Self::Binary(g) => g.memory_usage_bytes(),
            Self::Ternary(g) => g.memory_usage_bytes(),
            // The rotation is `dim × dim` and the codebooks `m × k × sub_dim`.
            Self::Opq(g) => {
                let c = &g.codec;
                g.memory_usage_bytes() + (c.dim * c.dim + c.m * c.k * c.sub_dim) * f32_bytes
            }
        }
    }
}
