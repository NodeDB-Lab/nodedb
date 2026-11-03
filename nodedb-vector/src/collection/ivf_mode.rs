// SPDX-License-Identifier: Apache-2.0

//! The IVF-PQ mode of a `VectorCollection`.
//!
//! An `IvfPq` collection buffers its vectors in the growing segment until it
//! holds the training threshold, `max(ivf_cells, pq_k)`. The growing segment
//! is searched exactly, checkpointed, and rebuilt by WAL replay, so the
//! buffer is durable and searchable with no extra machinery. It never seals.
//!
//! [`VectorCollection::train_ivf`] trains the IVF centroids and PQ codebooks
//! on every live vector, moves each one into the trained index under its
//! global id, and empties the segments. The move happens in one call on the
//! owning core, so no search sees a vector twice or misses one. Later inserts
//! land in the trained index.

use nodedb_mem::ScopedMemory;

use crate::error::VectorError;
use crate::index_config::{IndexConfig, IndexType};
use crate::ivf::IvfPqIndex;

use super::lifecycle::VectorCollection;
use super::lifecycle_insert_ops::sealed_vector;

impl VectorCollection {
    /// Whether the collection is configured as an IVF-PQ index.
    pub fn is_ivf(&self) -> bool {
        self.index_config.index_type == IndexType::IvfPq
    }

    /// The full index configuration.
    pub fn index_config(&self) -> &IndexConfig {
        &self.index_config
    }

    /// Replace the index configuration. The HNSW parameters the collection
    /// holds stay: its segments were built with them.
    pub fn set_index_config(&mut self, config: IndexConfig) {
        self.index_config = IndexConfig {
            hnsw: self.params.clone(),
            ..config
        };
        // A new plan gets a fresh codec build on every segment.
        self.codec_refused.clear();
    }

    /// Live vectors an `IvfPq` collection needs before it trains.
    pub fn ivf_training_threshold(&self) -> usize {
        self.index_config.to_ivf_params().training_threshold()
    }

    /// Whether an `IvfPq` collection is untrained and holds the training
    /// threshold of live vectors.
    pub fn needs_ivf_training(&self) -> bool {
        self.is_ivf() && self.ivf.is_none() && self.live_count() >= self.ivf_training_threshold()
    }

    /// The trained IVF-PQ index, if any.
    pub fn ivf_index(&self) -> Option<&IvfPqIndex> {
        self.ivf.as_ref()
    }

    /// Train the IVF-PQ index on every live vector and move them all into it.
    /// `trained_at_ms` stamps the training, in Unix milliseconds.
    ///
    /// Fails with [`VectorError::InvalidInput`] when the collection is not
    /// `IvfPq`, is already trained, or holds fewer live vectors than the
    /// training threshold. A training error propagates. Any failure leaves
    /// the collection unchanged.
    pub fn train_ivf(
        &mut self,
        memory: ScopedMemory,
        trained_at_ms: u64,
    ) -> Result<(), VectorError> {
        if !self.is_ivf() || self.ivf.is_some() {
            return Err(VectorError::InvalidInput {
                detail: "IVF-PQ training needs an untrained ivf_pq collection".into(),
            });
        }
        let live = self.gather_live_vectors();
        let threshold = self.ivf_training_threshold();
        if live.len() < threshold {
            return Err(VectorError::InvalidInput {
                detail: format!(
                    "IVF-PQ training needs {threshold} live vectors; the collection holds {}",
                    live.len()
                ),
            });
        }
        let mut ivf = IvfPqIndex::new(self.dim, self.index_config.to_ivf_params());
        {
            let refs: Vec<&[f32]> = live.iter().map(|(_, v)| v.as_slice()).collect();
            ivf.train(&refs, memory)?;
        }
        for (id, vector) in live {
            ivf.insert_with_id(id, vector)?;
        }
        ivf.set_trained_at_ms(trained_at_ms);
        self.clear_segments();
        self.ivf = Some(ivf);
        Ok(())
    }

    /// Every live FP32 vector outside the IVF index, keyed by global id.
    fn gather_live_vectors(&self) -> Vec<(u32, Vec<f32>)> {
        let mut out = Vec::with_capacity(self.live_count());
        for seg in &self.sealed {
            for local in 0..seg.index.len() as u32 {
                if !seg.index.is_deleted(local)
                    && let Some(v) = sealed_vector(seg, local)
                {
                    out.push((seg.base_id + local, v));
                }
            }
        }
        for seg in &self.building {
            for local in 0..seg.flat.len() as u32 {
                if let Some(v) = seg.flat.get_vector(local) {
                    out.push((seg.base_id + local, v.to_vec()));
                }
            }
        }
        for local in 0..self.growing.len() as u32 {
            if let Some(v) = self.growing.get_vector(local) {
                out.push((self.growing_base_id + local, v.to_vec()));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::test_support::test_memory;

    const DIM: usize = 8;

    /// An L2 `IvfPq` collection with 4 cells, all probed, and 256 PQ
    /// centroids: the threshold is 256 vectors.
    fn ivf_collection() -> VectorCollection {
        let config = IndexConfig {
            hnsw: crate::hnsw::HnswParams {
                metric: crate::distance::DistanceMetric::L2,
                ..crate::hnsw::HnswParams::default()
            },
            index_type: IndexType::IvfPq,
            pq_m: 4,
            ivf_cells: 4,
            ivf_nprobe: 4,
            ..IndexConfig::default()
        };
        VectorCollection::with_index_config(DIM, config)
    }

    /// Distinct per `i`: the first component is `i + 1`.
    fn vector(i: usize) -> Vec<f32> {
        [1, 7, 11, 13, 17, 19, 23, 29]
            .iter()
            .map(|&m| (if m == 1 { i + 1 } else { i % m + 1 }) as f32)
            .collect()
    }

    fn all_ids(coll: &VectorCollection, query: &[f32]) -> Vec<u32> {
        let mut ids: Vec<u32> = coll
            .search(query, 10_000, 64)
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn below_the_threshold_vectors_wait_in_the_exact_buffer() {
        let mut coll = ivf_collection();
        assert_eq!(coll.ivf_training_threshold(), 256);
        for i in 0..10 {
            coll.insert_with_surrogate(vector(i), Surrogate::new(i as u32 + 1))
                .unwrap();
        }
        assert!(!coll.needs_ivf_training());
        assert!(!coll.needs_seal());
        let hit = &coll.search(&vector(3), 1, 64).unwrap()[0];
        assert_eq!(hit.id, 3);
        assert_eq!(hit.distance, 0.0, "the buffer is searched exactly");
    }

    #[test]
    fn training_moves_every_vector_once_and_later_inserts_follow() {
        let mut coll = ivf_collection();
        for i in 0..256 {
            coll.insert_with_surrogate(vector(i), Surrogate::new(i as u32 + 1))
                .unwrap();
        }
        coll.delete(5);
        coll.insert(vector(256)).unwrap();
        assert!(coll.needs_ivf_training());
        let before = all_ids(&coll, &vector(0));

        coll.train_ivf(test_memory(), 42).unwrap();

        assert!(!coll.needs_ivf_training());
        assert!(coll.growing_is_empty());
        let ivf = coll.ivf_index().unwrap();
        assert_eq!(ivf.trained_on(), 256);
        assert_eq!(ivf.trained_at_ms(), 42);
        assert_eq!(
            all_ids(&coll, &vector(0)),
            before,
            "no vector lost or doubled"
        );

        let id = coll
            .insert_with_surrogate(vector(300), Surrogate::new(9_999))
            .unwrap();
        assert_eq!(id, 257);
        assert!(
            coll.growing_is_empty(),
            "a trained collection inserts into IVF"
        );
        assert_eq!(coll.search(&vector(300), 1, 64).unwrap()[0].id, 257);
        assert_eq!(
            coll.vector_for_surrogate(Surrogate::new(9_999)),
            Some(vector(300))
        );
        assert!(coll.delete_by_surrogate(Surrogate::new(9_999)));
        assert!(coll.search(&vector(300), 1, 64).unwrap()[0].id != 257);
    }

    #[test]
    fn training_below_the_threshold_is_refused() {
        let mut coll = ivf_collection();
        coll.insert(vector(0)).unwrap();
        assert!(matches!(
            coll.train_ivf(test_memory(), 0),
            Err(VectorError::InvalidInput { .. })
        ));
        assert!(coll.ivf_index().is_none());
        assert_eq!(coll.live_count(), 1);
    }

    #[test]
    fn a_trained_collection_survives_a_checkpoint() {
        let mut coll = ivf_collection();
        for i in 0..260 {
            coll.insert(vector(i)).unwrap();
        }
        coll.train_ivf(test_memory(), 7).unwrap();
        coll.insert(vector(400)).unwrap();
        coll.delete(9);

        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();

        assert!(restored.is_ivf());
        assert_eq!(restored.ivf_index().unwrap().trained_at_ms(), 7);
        assert_eq!(all_ids(&restored, &vector(0)), all_ids(&coll, &vector(0)));
        assert_eq!(restored.search(&vector(400), 1, 64).unwrap()[0].id, 260);
    }

    #[test]
    fn an_untrained_buffer_survives_a_checkpoint_and_trains_after() {
        let mut coll = ivf_collection();
        for i in 0..100 {
            coll.insert(vector(i)).unwrap();
        }
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let mut restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert!(restored.is_ivf());
        assert!(restored.ivf_index().is_none());
        for i in 100..256 {
            restored.insert(vector(i)).unwrap();
        }
        assert!(restored.needs_ivf_training());
        restored.train_ivf(test_memory(), 1).unwrap();
        assert_eq!(
            all_ids(&restored, &vector(0)),
            (0..256).collect::<Vec<u32>>()
        );
    }
}
