// SPDX-License-Identifier: Apache-2.0

//! Shared test fixtures for this crate's inline `#[cfg(test)]` modules.

use std::sync::Arc;

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};

/// Per-engine limit. The ceiling multiplies it back up, because the governor
/// rejects a config whose engine limits outgrow the global ceiling.
const PER_ENGINE: usize = usize::MAX / EngineId::COUNT;

/// A scope backed by a governor whose ceiling covers every engine's limit.
pub(crate) fn test_memory() -> ScopedMemory {
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: PER_ENGINE * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(PER_ENGINE),
        })
        .expect("test governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Vector,
    )
}

/// Deterministic vectors in `[0, 1)` from an xorshift64 stream.
///
/// `seed` must be non-zero.
pub(crate) fn seeded_vectors(count: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..count)
        .map(|_| {
            (0..dim)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % 10_000) as f32 / 10_000.0
                })
                .collect()
        })
        .collect()
}

/// An HNSW index over `vectors`, built with a fixed seed.
pub(crate) fn seeded_index(
    vectors: &[Vec<f32>],
    metric: crate::distance::DistanceMetric,
    dtype: nodedb_types::vector_dtype::VectorStorageDtype,
) -> crate::hnsw::HnswIndex {
    let dim = vectors.first().map_or(0, Vec::len);
    let params = crate::hnsw::HnswParams {
        m: 4,
        m0: 8,
        ef_construction: 32,
        metric,
        dtype,
    };
    let mut index = crate::hnsw::HnswIndex::with_seed(dim, params, 12345);
    for v in vectors {
        index
            .insert(v.clone())
            .expect("seeded vector has the index dim");
    }
    index
}

/// `src` restored from its graph-only checkpoint with no backing attached.
///
/// Every node of the result has no vector.
pub(crate) fn graph_only_restore(src: &crate::hnsw::HnswIndex) -> crate::hnsw::HnswIndex {
    let bytes = src
        .graph_checkpoint_to_bytes()
        .expect("graph checkpoint encodes");
    crate::hnsw::HnswIndex::from_checkpoint(&bytes)
        .expect("graph checkpoint decodes")
        .expect("graph checkpoint is recognized")
}

/// Serves an in-memory copy of an index's vectors.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct VecBacking {
    pub(crate) dim: usize,
    pub(crate) vectors: Vec<Vec<f32>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::segment_backing::VectorSegmentBacking for VecBacking {
    fn len(&self) -> usize {
        self.vectors.len()
    }
    fn dim(&self) -> usize {
        self.dim
    }
    fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.vectors.get(id as usize).map(Vec::as_slice)
    }
    fn get_surrogate(&self, _id: u32) -> Option<u64> {
        None
    }
}

/// `src` restored from its graph-only checkpoint, its vectors served by a
/// backing over `vectors`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn graph_only_restore_with_backing(
    src: &crate::hnsw::HnswIndex,
    vectors: &[Vec<f32>],
) -> crate::hnsw::HnswIndex {
    let mut restored = graph_only_restore(src);
    restored
        .with_backing(Arc::new(VecBacking {
            dim: src.dim(),
            vectors: vectors.to_vec(),
        }))
        .expect("a complete backing attaches");
    restored
}

/// Build a copy of `req` on the calling thread.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn build_now(req: &crate::collection::BuildRequest) -> crate::collection::BuiltSegment {
    let copy = crate::collection::BuildRequest {
        key: req.key.clone(),
        segment_id: req.segment_id,
        kind: req.kind,
        vectors: req.vectors.clone(),
        dim: req.dim,
        params: req.params.clone(),
        codec: req.codec,
        memory: req.memory.clone(),
    };
    let built = crate::builder::build_segment(7, copy).expect("test segment build");
    if let Err(e) = &built.codec {
        panic!("test codec build: {e}");
    }
    built
}
