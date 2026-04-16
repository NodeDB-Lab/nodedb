//! Quantizer training helpers for `VectorCollection`.
//!
//! Split from `lifecycle.rs` to keep that file under the 500-line cap.
//! All methods here are `impl VectorCollection` blocks — Rust allows a
//! type's impl to be split across files.

use crate::hnsw::{HnswIndex, HnswParams};
use crate::index_config::{IndexConfig, IndexType};
use crate::quantize::pq::PqCodec;
use crate::quantize::sq8::Sq8Codec;

use super::lifecycle::VectorCollection;
use super::segment::DEFAULT_SEAL_THRESHOLD;

impl VectorCollection {
    /// Convenience constructor for PQ-configured collections.
    ///
    /// Equivalent to building a full `IndexConfig` with
    /// `index_type = HnswPq` and the given `pq_m`.
    pub fn with_pq_config(dim: usize, hnsw: HnswParams, pq_m: usize) -> Self {
        let config = IndexConfig {
            hnsw,
            index_type: IndexType::HnswPq,
            pq_m,
            ..IndexConfig::default()
        };
        Self::with_index_config(dim, config)
    }

    /// Convenience constructor for PQ-configured collections with a custom
    /// seal threshold.
    pub fn with_seal_threshold_and_pq_config(
        dim: usize,
        hnsw: HnswParams,
        pq_m: usize,
        seal_threshold: usize,
    ) -> Self {
        let config = IndexConfig {
            hnsw,
            index_type: IndexType::HnswPq,
            pq_m,
            ..IndexConfig::default()
        };
        Self::with_seal_threshold_and_config(dim, config, seal_threshold)
    }

    /// Build SQ8 quantized data for an HNSW index.
    ///
    /// Returns `None` when there are too few live vectors for stable
    /// min/max calibration.
    pub fn build_sq8_for_index(index: &HnswIndex) -> Option<(Sq8Codec, Vec<u8>)> {
        if index.live_count() < 1000 {
            return None;
        }
        let dim = index.dim();
        let n = index.len();

        let mut refs: Vec<&[f32]> = Vec::with_capacity(n);
        for i in 0..n {
            if !index.is_deleted(i as u32)
                && let Some(v) = index.get_vector(i as u32)
            {
                refs.push(v);
            }
        }
        if refs.is_empty() {
            return None;
        }

        let codec = Sq8Codec::calibrate(&refs, dim);

        let mut data = Vec::with_capacity(dim * n);
        for i in 0..n {
            if let Some(v) = index.get_vector(i as u32) {
                data.extend(codec.quantize(v));
            } else {
                data.extend(vec![0u8; dim]);
            }
        }

        Some((codec, data))
    }

    /// Train a PQ codec from a built HNSW index's live vectors.
    pub fn build_pq_for_index(index: &HnswIndex, pq_m: usize) -> Option<(PqCodec, Vec<u8>)> {
        let dim = index.dim();
        if pq_m == 0 || !dim.is_multiple_of(pq_m) {
            return None;
        }
        let n = index.len();
        let mut refs: Vec<Vec<f32>> = Vec::with_capacity(n);
        for i in 0..n {
            if !index.is_deleted(i as u32)
                && let Some(v) = index.get_vector(i as u32)
            {
                refs.push(v.to_vec());
            }
        }
        if refs.is_empty() {
            return None;
        }
        let refs_slices: Vec<&[f32]> = refs.iter().map(|v| v.as_slice()).collect();
        let k = 256usize.min(refs.len());
        let codec = PqCodec::train(&refs_slices, dim, pq_m, k, 20);
        let codes = codec.encode_batch(&refs_slices);
        Some((codec, codes))
    }
}

// Keep the DEFAULT_SEAL_THRESHOLD import live when future refactors move
// additional ctors into this file; explicitly referenced to suppress
// an otherwise-unused warning.
const _: usize = DEFAULT_SEAL_THRESHOLD;
