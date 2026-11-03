// SPDX-License-Identifier: Apache-2.0

//! The codec one sealed segment carries.

use nodedb_types::{VectorIndexQuantization, VectorQuantization};

use crate::quantize::pq::PqCodec;
use crate::quantize::sq8::Sq8Codec;

use super::graph::CodecGraph;

/// The codec of one sealed segment. Codes and graph nodes are indexed by the
/// segment's local node id.
pub enum SegmentCodec {
    /// No codec.
    None,
    /// SQ8 codes, `codec.dim()` bytes per node.
    Sq8 { codec: Sq8Codec, codes: Vec<u8> },
    /// PQ codes, `codec.m` bytes per node.
    Pq { codec: PqCodec, codes: Vec<u8> },
    /// A quantized HNSW graph.
    Graph(CodecGraph),
}

impl SegmentCodec {
    /// The quantization this codec reports in index statistics.
    pub fn quantization(&self) -> VectorIndexQuantization {
        match self {
            Self::None => VectorIndexQuantization::None,
            Self::Sq8 { .. } => VectorIndexQuantization::Sq8,
            Self::Pq { .. } => VectorIndexQuantization::Pq,
            Self::Graph(CodecGraph::RaBitQ(_)) => VectorIndexQuantization::RaBitQ,
            Self::Graph(CodecGraph::Bbq(_)) => VectorIndexQuantization::Bbq,
            Self::Graph(CodecGraph::Binary(_)) => VectorIndexQuantization::Binary,
            Self::Graph(CodecGraph::Ternary(_)) => VectorIndexQuantization::Ternary,
            Self::Graph(CodecGraph::Opq(_)) => VectorIndexQuantization::Opq,
        }
    }

    /// The collection quantization this codec serves.
    pub fn serves(&self) -> VectorQuantization {
        match self {
            Self::None => VectorQuantization::None,
            Self::Sq8 { .. } => VectorQuantization::Sq8,
            Self::Pq { .. } => VectorQuantization::Pq,
            Self::Graph(CodecGraph::RaBitQ(_)) => VectorQuantization::RaBitQ,
            Self::Graph(CodecGraph::Bbq(_)) => VectorQuantization::Bbq,
            Self::Graph(CodecGraph::Binary(_)) => VectorQuantization::Binary,
            Self::Graph(CodecGraph::Ternary(_)) => VectorQuantization::Ternary,
            Self::Graph(CodecGraph::Opq(_)) => VectorQuantization::Opq,
        }
    }

    /// Bytes the codec holds in RAM: parameters, codes and graph.
    pub fn resident_bytes(&self) -> usize {
        let f32_bytes = std::mem::size_of::<f32>();
        match self {
            Self::None => 0,
            // Per dimension: min, max, scale and inverse scale.
            Self::Sq8 { codec, codes } => codec.dim() * 4 * f32_bytes + codes.len(),
            Self::Pq { codec, codes } => {
                codec.m * codec.k * codec.sub_dim * f32_bytes + codes.len()
            }
            Self::Graph(graph) => graph.memory_usage_bytes(),
        }
    }

    /// Mark local node `local` deleted or live in a graph codec. Code rows
    /// carry no tombstone: the HNSW graph filters deleted nodes first.
    pub fn set_deleted(&mut self, local: u32, deleted: bool) {
        match self {
            Self::None | Self::Sq8 { .. } | Self::Pq { .. } => {}
            Self::Graph(graph) => {
                graph.set_deleted(local, deleted);
            }
        }
    }
}
