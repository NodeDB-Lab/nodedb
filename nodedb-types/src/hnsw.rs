// SPDX-License-Identifier: Apache-2.0

//! Shared HNSW types used by both Origin and Lite vector engines.

use crate::vector_distance::DistanceMetric;
use crate::vector_dtype::VectorStorageDtype;
use serde::{Deserialize, Serialize};

/// HNSW index parameters shared between Origin and Lite.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HnswParams {
    /// Max bidirectional connections per node at layers > 0.
    pub m: usize,
    /// Max connections at layer 0 (typically 2*M for denser base layer).
    pub m0: usize,
    /// Dynamic candidate list size during construction.
    pub ef_construction: usize,
    /// Distance metric for similarity computation.
    pub metric: DistanceMetric,
    /// On-disk + in-memory vector storage dtype.
    pub dtype: VectorStorageDtype,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
            dtype: VectorStorageDtype::F32,
        }
    }
}

/// HNSW node snapshot for checkpoint serialization.
///
/// Shared format between Origin and Lite — both serialize nodes
/// identically via MessagePack, enabling cross-deployment checkpoint
/// compatibility.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HnswNodeSnapshot {
    pub vector: Vec<f32>,
    pub neighbors: Vec<Vec<u32>>,
    pub deleted: bool,
}

/// HNSW checkpoint snapshot — shared serialization format.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HnswCheckpoint {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub metric: u8,
    pub entry_point: Option<u32>,
    pub max_layer: usize,
    pub rng_state: u64,
    pub nodes: Vec<HnswNodeSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_params() {
        let p = HnswParams::default();
        assert_eq!(p.m, 16);
        assert_eq!(p.m0, 32);
        assert_eq!(p.ef_construction, 200);
    }

    #[test]
    fn checkpoint_serde_roundtrip() {
        let snap = HnswCheckpoint {
            dim: 128,
            m: 16,
            m0: 32,
            ef_construction: 200,
            metric: 1,
            entry_point: Some(0),
            max_layer: 3,
            rng_state: 42,
            nodes: vec![HnswNodeSnapshot {
                vector: vec![0.1, 0.2, 0.3],
                neighbors: vec![vec![1, 2], vec![3]],
                deleted: false,
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&snap).unwrap();
        let restored: HnswCheckpoint = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(restored.dim, 128);
        assert_eq!(restored.nodes.len(), 1);
        assert_eq!(restored.nodes[0].vector.len(), 3);
    }
}
