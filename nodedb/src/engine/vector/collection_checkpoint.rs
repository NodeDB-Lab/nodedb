//! Checkpoint serialization and deserialization for `VectorCollection`.
//!
//! Persists segment state (growing, sealed, building) to bytes for crash
//! recovery. Sealed segments serialize their HNSW indexes. Building segments
//! serialize raw vectors and are rebuilt on restore.

use serde::{Deserialize, Serialize};

use super::collection::{SealedSegment, VectorCollection};
use super::distance::DistanceMetric;
use super::flat::FlatIndex;
use super::hnsw::{HnswIndex, HnswParams};

#[derive(Serialize, Deserialize)]
pub(super) struct CollectionSnapshot {
    pub dim: usize,
    pub params_m: usize,
    pub params_m0: usize,
    pub params_ef_construction: usize,
    pub params_metric: u8,
    pub next_id: u32,
    pub growing_base_id: u32,
    pub growing_vectors: Vec<Vec<f32>>,
    pub growing_deleted: Vec<bool>,
    pub sealed_segments: Vec<SealedSnapshot>,
    pub building_segments: Vec<BuildingSnapshot>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct SealedSnapshot {
    pub base_id: u32,
    pub hnsw_bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct BuildingSnapshot {
    pub base_id: u32,
    pub vectors: Vec<Vec<f32>>,
}

impl VectorCollection {
    /// Serialize all segments for checkpointing.
    ///
    /// Lock-free: sealed segments are immutable (no concurrent writes).
    /// Growing segment is small (<=64K vectors). Building segments are
    /// serialized as raw vectors (will trigger rebuild on reload).
    ///
    /// Returns empty bytes if serialization fails (caller should log the error).
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = CollectionSnapshot {
            dim: self.dim,
            params_m: self.params.m,
            params_m0: self.params.m0,
            params_ef_construction: self.params.ef_construction,
            params_metric: self.params.metric as u8,
            next_id: self.next_id,
            growing_base_id: self.growing_base_id,
            growing_vectors: (0..self.growing.len() as u32)
                .filter_map(|i| self.growing.get_vector(i).map(|v| v.to_vec()))
                .collect(),
            growing_deleted: (0..self.growing.len() as u32)
                .map(|i| self.growing.get_vector(i).is_none())
                .collect(),
            sealed_segments: self
                .sealed
                .iter()
                .map(|s| SealedSnapshot {
                    base_id: s.base_id,
                    hnsw_bytes: s.index.checkpoint_to_bytes(),
                })
                .collect(),
            building_segments: self
                .building
                .iter()
                .map(|b| BuildingSnapshot {
                    base_id: b.base_id,
                    vectors: (0..b.flat.len() as u32)
                        .filter_map(|i| b.flat.get_vector(i).map(|v| v.to_vec()))
                        .collect(),
                })
                .collect(),
        };
        match rmp_serde::to_vec_named(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, "vector collection checkpoint serialization failed");
                Vec::new()
            }
        }
    }

    /// Restore a collection from checkpoint bytes.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CollectionSnapshot = rmp_serde::from_slice(bytes).ok()?;
        let metric = match snap.params_metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };
        let params = HnswParams {
            m: snap.params_m,
            m0: snap.params_m0,
            ef_construction: snap.params_ef_construction,
            metric,
        };

        // Restore growing segment.
        let mut growing = FlatIndex::new(snap.dim, metric);
        for v in &snap.growing_vectors {
            growing.insert(v.clone());
        }

        // Restore sealed segments.
        let mut sealed = Vec::with_capacity(snap.sealed_segments.len());
        for ss in &snap.sealed_segments {
            if let Some(index) = HnswIndex::from_checkpoint(&ss.hnsw_bytes) {
                let sq8 = Self::build_sq8_for_index(&index);
                sealed.push(SealedSegment {
                    index,
                    base_id: ss.base_id,
                    sq8,
                    tier: crate::storage::tier::StorageTier::L0Ram,
                    mmap_vectors: None,
                });
            }
        }

        // Building segments become sealed with fresh HNSW builds.
        for bs in &snap.building_segments {
            let mut index = HnswIndex::new(snap.dim, params.clone());
            for v in &bs.vectors {
                index.insert(v.clone());
            }
            let sq8 = Self::build_sq8_for_index(&index);
            sealed.push(SealedSegment {
                index,
                base_id: bs.base_id,
                sq8,
                tier: crate::storage::tier::StorageTier::L0Ram,
                mmap_vectors: None,
            });
        }

        let next_segment_id = (sealed.len() + 1) as u32;

        Some(Self {
            growing,
            growing_base_id: snap.growing_base_id,
            sealed,
            building: Vec::new(),
            params,
            next_id: snap.next_id,
            next_segment_id,
            dim: snap.dim,
            data_dir: None,
            ram_budget_bytes: 0,
            mmap_fallback_count: 0,
            mmap_segment_count: 0,
            doc_id_map: std::collections::HashMap::new(),
        })
    }
}
