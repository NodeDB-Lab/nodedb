//! Checkpoint serialization and deserialization for `VectorCollection`.

use serde::{Deserialize, Serialize};

use crate::collection::segment::{DEFAULT_SEAL_THRESHOLD, SealedSegment};
use crate::collection::tier::StorageTier;
use crate::distance::DistanceMetric;
use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::quantize::pq::PqCodec;

use super::lifecycle::VectorCollection;

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct CollectionSnapshot {
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
    #[serde(default)]
    pub doc_id_map: Vec<(u32, String)>,
    #[serde(default)]
    pub multi_doc_map: Vec<(String, Vec<u32>)>,
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct SealedSnapshot {
    pub base_id: u32,
    pub hnsw_bytes: Vec<u8>,
    #[serde(default)]
    pub pq_bytes: Option<Vec<u8>>,
    #[serde(default)]
    pub pq_codes: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct BuildingSnapshot {
    pub base_id: u32,
    pub vectors: Vec<Vec<f32>>,
    #[serde(default)]
    pub deleted: Vec<bool>,
}

impl VectorCollection {
    /// Serialize all segments for checkpointing.
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
                .filter_map(|i| self.growing.get_vector_raw(i).map(|v| v.to_vec()))
                .collect(),
            growing_deleted: (0..self.growing.len() as u32)
                .map(|i| self.growing.is_deleted(i))
                .collect(),
            sealed_segments: self
                .sealed
                .iter()
                .map(|s| {
                    let (pq_bytes, pq_codes) = match &s.pq {
                        Some((codec, codes)) => {
                            (zerompk::to_msgpack_vec(codec).ok(), Some(codes.clone()))
                        }
                        None => (None, None),
                    };
                    SealedSnapshot {
                        base_id: s.base_id,
                        hnsw_bytes: s.index.checkpoint_to_bytes(),
                        pq_bytes,
                        pq_codes,
                    }
                })
                .collect(),
            building_segments: self
                .building
                .iter()
                .map(|b| BuildingSnapshot {
                    base_id: b.base_id,
                    vectors: (0..b.flat.len() as u32)
                        .filter_map(|i| b.flat.get_vector_raw(i).map(|v| v.to_vec()))
                        .collect(),
                    deleted: (0..b.flat.len() as u32)
                        .map(|i| b.flat.is_deleted(i))
                        .collect(),
                })
                .collect(),
            doc_id_map: self
                .doc_id_map
                .iter()
                .map(|(&k, v)| (k, v.clone()))
                .collect(),
            multi_doc_map: self
                .multi_doc_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        match zerompk::to_msgpack_vec(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, "vector collection checkpoint serialization failed");
                Vec::new()
            }
        }
    }

    /// Restore a collection from checkpoint bytes.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CollectionSnapshot = zerompk::from_msgpack(bytes).ok()?;
        let metric = match snap.params_metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            3 => DistanceMetric::Manhattan,
            4 => DistanceMetric::Chebyshev,
            5 => DistanceMetric::Hamming,
            6 => DistanceMetric::Jaccard,
            7 => DistanceMetric::Pearson,
            _ => DistanceMetric::Cosine,
        };
        let params = HnswParams {
            m: snap.params_m,
            m0: snap.params_m0,
            ef_construction: snap.params_ef_construction,
            metric,
        };

        let mut growing = FlatIndex::new(snap.dim, metric);
        for (i, v) in snap.growing_vectors.iter().enumerate() {
            let deleted = snap.growing_deleted.get(i).copied().unwrap_or(false);
            if deleted {
                growing.insert_tombstoned(v.clone());
            } else {
                growing.insert(v.clone());
            }
        }

        let mut sealed = Vec::with_capacity(snap.sealed_segments.len());
        for ss in &snap.sealed_segments {
            if let Some(index) = HnswIndex::from_checkpoint(&ss.hnsw_bytes) {
                let pq = match (&ss.pq_bytes, &ss.pq_codes) {
                    (Some(bytes), Some(codes)) => zerompk::from_msgpack::<PqCodec>(bytes)
                        .ok()
                        .map(|codec| (codec, codes.clone())),
                    _ => None,
                };
                // Only train SQ8 when PQ isn't present — a segment never carries both.
                let sq8 = if pq.is_some() {
                    None
                } else {
                    VectorCollection::build_sq8_for_index(&index)
                };
                sealed.push(SealedSegment {
                    index,
                    base_id: ss.base_id,
                    sq8,
                    pq,
                    tier: StorageTier::L0Ram,
                    mmap_vectors: None,
                });
            }
        }

        for bs in &snap.building_segments {
            let mut index = HnswIndex::new(snap.dim, params.clone());
            for v in &bs.vectors {
                index
                    .insert(v.clone())
                    .expect("dimension guaranteed by checkpoint");
            }
            // Replay building-segment tombstones onto the HNSW index.
            for (i, &dead) in bs.deleted.iter().enumerate() {
                if dead {
                    index.delete(i as u32);
                }
            }
            let sq8 = VectorCollection::build_sq8_for_index(&index);
            sealed.push(SealedSegment {
                index,
                base_id: bs.base_id,
                sq8,
                pq: None,
                tier: StorageTier::L0Ram,
                mmap_vectors: None,
            });
        }

        let next_segment_id = (sealed.len() + 1) as u32;

        let index_config = crate::index_config::IndexConfig {
            hnsw: params.clone(),
            ..crate::index_config::IndexConfig::default()
        };
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
            doc_id_map: snap.doc_id_map.into_iter().collect(),
            multi_doc_map: snap.multi_doc_map.into_iter().collect(),
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            index_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::distance::DistanceMetric;
    use crate::hnsw::HnswParams;

    #[test]
    fn checkpoint_roundtrip() {
        let mut coll = VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        for i in 0..50u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        let bytes = coll.checkpoint_to_bytes();
        let restored = VectorCollection::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);

        let results = restored.search(&[25.0, 0.0, 0.0], 1, 64);
        assert_eq!(results[0].id, 25);
    }
}
