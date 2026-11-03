// SPDX-License-Identifier: Apache-2.0

//! The codec a collection builds its sealed segments with.

use nodedb_types::VectorQuantization;

use crate::error::VectorError;

/// Largest dimension a graph codec encodes: quantized vector headers store
/// the dimension as `u16`.
pub const MAX_GRAPH_CODEC_DIM: usize = u16::MAX as usize;

/// Check that a graph codec can encode vectors of dimension `dim`. The DDL
/// and the segment build both call it, so a refused shape never reaches a
/// build.
pub fn check_graph_codec_shape(dim: usize) -> Result<(), VectorError> {
    if dim == 0 || dim > MAX_GRAPH_CODEC_DIM {
        return Err(VectorError::InvalidInput {
            detail: format!(
                "a graph vector codec needs a dimension in 1..={MAX_GRAPH_CODEC_DIM}, got {dim}"
            ),
        });
    }
    Ok(())
}

/// A codec that keeps its own quantized HNSW graph over a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphCodecKind {
    RaBitQ,
    Bbq,
    Binary,
    Ternary,
    /// OPQ with `m` subspaces.
    Opq {
        m: usize,
    },
}

/// The codec every sealed segment of a collection carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecPlan {
    /// No codec: the HNSW graph answers from the stored vectors.
    None,
    /// SQ8 codes order the HNSW candidates.
    Sq8,
    /// PQ codes of `m` subspaces order the HNSW candidates.
    Pq { m: usize },
    /// A quantized HNSW graph yields the candidates.
    Graph(GraphCodecKind),
}

impl CodecPlan {
    /// The quantization this plan serves.
    pub fn quantization(self) -> VectorQuantization {
        match self {
            Self::None => VectorQuantization::None,
            Self::Sq8 => VectorQuantization::Sq8,
            Self::Pq { .. } => VectorQuantization::Pq,
            Self::Graph(GraphCodecKind::RaBitQ) => VectorQuantization::RaBitQ,
            Self::Graph(GraphCodecKind::Bbq) => VectorQuantization::Bbq,
            Self::Graph(GraphCodecKind::Binary) => VectorQuantization::Binary,
            Self::Graph(GraphCodecKind::Ternary) => VectorQuantization::Ternary,
            Self::Graph(GraphCodecKind::Opq { .. }) => VectorQuantization::Opq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_codecs_refuse_a_dimension_past_u16() {
        assert!(check_graph_codec_shape(MAX_GRAPH_CODEC_DIM).is_ok());
        assert!(check_graph_codec_shape(MAX_GRAPH_CODEC_DIM + 1).is_err());
        assert!(check_graph_codec_shape(0).is_err());
    }
}
