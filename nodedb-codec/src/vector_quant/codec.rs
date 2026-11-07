// SPDX-License-Identifier: Apache-2.0

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
///
/// # Extensibility audit — future algorithms
///
/// The following six algorithms can each be added as a new `impl VectorCodec`
/// without changing the trait surface:
///
/// **TurboQuant** — learned rotation matrix applied before scalar quantization.
/// The rotation is held in the codec struct (same pattern as `OpqCodec.rotation`)
/// and applied inside `encode` and `prepare_query`. No trait change needed.
/// `QuantMode::TurboQuant4b` already exists as a layout discriminant.
///
/// **PolarQuant** — encodes magnitude and direction as separate components.
/// Both components are packed into a single `packed_bits` region whose layout
/// the codec controls. The `encode` return type is `Vec<u8>` of arbitrary
/// length via `UnifiedQuantizedVector::packed_bits`, so multi-component payloads
/// fit without a trait change. `QuantMode::PolarQuant` is already reserved.
///
/// **ITQ3_S** — Iterative Quantization with 3-bit codes, learned via SVD.
/// Requires a calibration/fitting step before encoding begins. Existing codecs
/// expose this as a concrete static `calibrate` method (e.g. `Sq8Codec::calibrate`,
/// `RaBitQCodec::calibrate`). The `train` method added below makes this fitting
/// phase reachable through the trait for generic dispatchers that hold a
/// `C: VectorCodec` without knowing the concrete type. The default impl is a
/// no-op so all existing impls compile without change.
///
/// **OSAQ** — Optimized Symmetric/Asymmetric Quantization. Uses symmetric
/// encoding for indexed vectors and asymmetric encoding for queries.
/// The existing trait already separates these paths: `encode` is the index path,
/// `prepare_query` is the query path, and `type Query` is a distinct associated
/// type. No trait change needed.
///
/// **RaBiT** — Rotation + Binary quantization with full-precision query and
/// 1-bit index. Exactly the pattern implemented by `RaBitQCodec`: `type Query`
/// holds a full-precision `Vec<f32>`, `type Quantized` holds 1-bit sign codes,
/// and `exact_asymmetric_distance` uses the full query against binary candidates.
/// No trait change needed.
///
/// **LVQ** — Locally-adaptive Vector Quantization with per-vector scale/offset.
/// The `QuantHeader` carries `global_scale` and `residual_norm` — two f32
/// scalars sufficient for per-vector min/max parameters. If a codec needs
/// additional per-vector state it can prepend a small structured header inside
/// `packed_bits` (the codec controls that region's layout entirely). No trait
/// change needed.
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

    /// Optional: fit the codec's learned parameters on a set of training vectors.
    ///
    /// Called once before encoding begins, typically at index-build time. Codecs
    /// with no learnable parameters (binary sign-bit, ternary) can use the
    /// default no-op. Codecs with an SVD- or k-means-based fitting phase
    /// (ITQ3_S, OPQ, PQ, TurboQuant) override this to update their internal
    /// rotation matrices or codebooks.
    ///
    /// Returns `Ok(())` on success. The error type is `CodecError` so callers
    /// that drive training through a generic bound can propagate failures without
    /// knowing the concrete codec.
    ///
    /// The default implementation is a no-op and always returns `Ok(())`.
    #[allow(unused_variables)]
    fn train(&mut self, samples: &[&[f32]]) -> Result<(), crate::error::CodecError> {
        Ok(())
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
            let uqv = UnifiedQuantizedVector::without_outliers(header, &packed);
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
