//! Dual-phase `VectorCodec` trait — the seam that makes future quantization
//! algorithms drop-in additions rather than engine rewrites.
//!
//! Upper-layer routing (HNSW navigation, beam expansion) calls
//! `fast_symmetric_distance` on bitwise/heuristic kernels.
//! Base-layer rerank calls `exact_asymmetric_distance` with full ADC +
//! scalar correction + sparse outlier resolution.

use crate::vector_quant::layout::UnifiedQuantizedVector;

/// Asymmetric Distance Computation lookup table — used by codecs that
/// pre-decompose the query into per-subspace distance tables (PQ, IVF-PQ,
/// TurboQuant). Consumed by base-layer rerank kernels via AVX2 `pshufb`
/// or AVX-512 `vpermb`.
///
/// Layout:
/// - `subspace_count`: number of independent subspaces (PQ M parameter)
/// - `centroids_per_subspace`: typically 256 (one byte per code)
/// - `table`: row-major `[subspace][centroid] -> f32 distance`
pub struct AdcLut {
    pub subspace_count: u16,
    pub centroids_per_subspace: u16,
    pub table: Vec<f32>,
}

impl AdcLut {
    #[inline]
    pub fn new(subspace_count: u16, centroids_per_subspace: u16) -> Self {
        let n = subspace_count as usize * centroids_per_subspace as usize;
        Self {
            subspace_count,
            centroids_per_subspace,
            table: vec![0.0; n],
        }
    }

    /// Return the precomputed distance for the given `subspace` and `centroid`.
    ///
    /// # Panics
    ///
    /// Panics if `subspace >= self.subspace_count` or
    /// `centroid as usize >= self.centroids_per_subspace as usize`.
    /// Bounds checking is the caller's responsibility on this hot-path accessor.
    #[inline]
    pub fn lookup(&self, subspace: u16, centroid: u8) -> f32 {
        let idx = subspace as usize * self.centroids_per_subspace as usize + centroid as usize;
        self.table[idx]
    }
}

/// The dual-phase quantization codec trait.
///
/// `Quantized` is the on-disk / in-memory packed form (one per vector).
/// `Query` is the prepared query — may be raw FP32, may be rotated, may
/// hold a precomputed ADC LUT, depending on the codec.
pub trait VectorCodec: Send + Sync {
    /// The packed quantized form. Must be convertible to a `UnifiedQuantizedVector`
    /// reference via `AsRef`.
    type Quantized: AsRef<UnifiedQuantizedVector>;

    /// The prepared query form (codec-specific).
    type Query;

    /// Encode a single FP32 vector into the codec's packed form.
    fn encode(&self, v: &[f32]) -> Self::Quantized;

    /// Prepare a query for distance computations against this codec.
    /// May rotate, normalize, build a LUT, etc.
    fn prepare_query(&self, q: &[f32]) -> Self::Query;

    /// Optional: precompute ADC lookup table for codecs that use one
    /// (PQ, IVF-PQ, TurboQuant). Returns `None` for codecs that don't
    /// (RaBitQ, BBQ, ternary, binary).
    fn adc_lut(&self, _q: &Self::Query) -> Option<AdcLut> {
        None
    }

    /// Fast symmetric distance — bitwise / heuristic. Used during HNSW
    /// upper-layer routing. Both arguments are quantized; no scalar
    /// correction; no outlier resolution. Hot path; must be inline-friendly.
    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32;

    /// Exact asymmetric distance — full ADC with scalar correction and
    /// sparse outlier resolution. Used at base-layer rerank only. Slower
    /// but high-fidelity.
    fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_quant::layout::{QuantHeader, QuantMode, UnifiedQuantizedVector};

    #[test]
    fn adc_lut_new_produces_zeroed_table() {
        let lut = AdcLut::new(4, 256);
        assert_eq!(lut.table.len(), 1024);
        assert!(lut.table.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn adc_lut_lookup_returns_written_value() {
        let mut lut = AdcLut::new(4, 256);
        // Write a sentinel into subspace=2, centroid=17
        let idx = 2usize * 256 + 17;
        lut.table[idx] = 2.5;
        assert_eq!(lut.lookup(2, 17), 2.5);
        // Other entries remain zero.
        assert_eq!(lut.lookup(0, 0), 0.0);
        assert_eq!(lut.lookup(3, 255), 0.0);
    }

    // --- Stub codec used only to verify that the trait compiles ---

    /// Minimal quantized wrapper for test purposes.
    struct StubQuantized(UnifiedQuantizedVector);

    impl AsRef<UnifiedQuantizedVector> for StubQuantized {
        fn as_ref(&self) -> &UnifiedQuantizedVector {
            &self.0
        }
    }

    struct StubCodec;

    impl VectorCodec for StubCodec {
        type Quantized = StubQuantized;
        type Query = Vec<f32>;

        fn encode(&self, v: &[f32]) -> Self::Quantized {
            let header = QuantHeader {
                quant_mode: QuantMode::Binary as u16,
                dim: v.len() as u16,
                global_scale: 1.0,
                residual_norm: 0.0,
                dot_quantized: 0.0,
                outlier_bitmask: 0,
                reserved: [0; 8],
            };
            let packed = vec![0u8; v.len().div_ceil(8)];
            let uqv = UnifiedQuantizedVector::new(header, &packed, &[])
                .expect("stub encode: layout construction must succeed");
            StubQuantized(uqv)
        }

        fn prepare_query(&self, q: &[f32]) -> Self::Query {
            q.to_vec()
        }

        fn fast_symmetric_distance(&self, _q: &Self::Quantized, _v: &Self::Quantized) -> f32 {
            0.0
        }

        fn exact_asymmetric_distance(&self, _q: &Self::Query, _v: &Self::Quantized) -> f32 {
            0.0
        }
    }

    /// Verify a generic function parameterised on `VectorCodec` compiles.
    fn use_codec<C: VectorCodec>(c: &C, q: &[f32], v: &[f32]) -> f32 {
        let qv = c.encode(v);
        let qq = c.prepare_query(q);
        let sym = c.fast_symmetric_distance(&qv, &qv);
        let asym = c.exact_asymmetric_distance(&qq, &qv);
        sym + asym
    }

    #[test]
    fn generic_use_codec_compiles_and_runs() {
        let codec = StubCodec;
        let result = use_codec(&codec, &[1.0, 0.0], &[0.0, 1.0]);
        assert_eq!(result, 0.0);
    }
}
