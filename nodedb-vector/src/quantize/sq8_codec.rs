//! `VectorCodec` implementation for `Sq8Codec`.
//!
//! Wraps the existing concrete SQ8 quantizer as a dual-phase codec. The
//! `Quantized` newtype holds a `UnifiedQuantizedVector` with `QuantMode::Sq8`
//! and M = dim u8 codes in the packed-bits region. The `Query` type is a raw
//! FP32 slice (asymmetric: query stays full-precision, candidates are INT8).

use nodedb_codec::vector_quant::{
    codec::{AdcLut, VectorCodec},
    layout::{QuantHeader, QuantMode, UnifiedQuantizedVector},
};

use crate::quantize::sq8::Sq8Codec;

// ── Newtype ──────────────────────────────────────────────────────────────────

/// Thin newtype wrapping `UnifiedQuantizedVector` for SQ8-encoded vectors.
pub struct Sq8Quantized(pub UnifiedQuantizedVector);

impl AsRef<UnifiedQuantizedVector> for Sq8Quantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

// ── Helper ───────────────────────────────────────────────────────────────────

#[inline]
fn packed_bits_of(q: &Sq8Quantized) -> &[u8] {
    q.0.packed_bits()
}

// ── VectorCodec impl ─────────────────────────────────────────────────────────

impl VectorCodec for Sq8Codec {
    type Quantized = Sq8Quantized;
    /// Raw FP32 query — asymmetric distance; query is never quantized.
    type Query = Vec<f32>;

    /// Encode a single FP32 vector into an SQ8 `UnifiedQuantizedVector`.
    ///
    /// # Panics
    ///
    /// The `UnifiedQuantizedVector::new` call will only fail if the outlier
    /// count mismatches the bitmask, or an outlier dim_index ≥ 64. Neither
    /// condition can arise here: `outlier_bitmask` is 0 and `outliers` is
    /// empty. The `expect` is therefore unreachable in practice.
    fn encode(&self, v: &[f32]) -> Self::Quantized {
        let codes = self.quantize(v);
        let header = QuantHeader {
            quant_mode: QuantMode::Sq8 as u16,
            dim: self.dim as u16,
            global_scale: 0.0,
            residual_norm: 0.0,
            dot_quantized: 0.0,
            outlier_bitmask: 0,
            reserved: [0; 8],
        };
        let uqv = UnifiedQuantizedVector::new(header, &codes, &[])
            .expect("Sq8Codec::encode: layout construction is infallible (no outliers)");
        Sq8Quantized(uqv)
    }

    /// Prepare the FP32 query for asymmetric distance computations.
    ///
    /// For SQ8 the query is used directly without rotation or normalization.
    fn prepare_query(&self, q: &[f32]) -> Self::Query {
        q.to_vec()
    }

    /// SQ8 has no precomputed ADC table — returns `None`.
    fn adc_lut(&self, _q: &Self::Query) -> Option<AdcLut> {
        None
    }

    /// Symmetric L2 squared distance between two SQ8-quantized vectors.
    ///
    /// Both codes are dequantized to FP32 via `self.mins` + `self.scales`, then
    /// the squared difference is accumulated. This is slower than an exact
    /// INT8-INT8 Hamming estimate but SQ8 has no faster bitwise symmetric form.
    #[inline]
    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32 {
        let qa = packed_bits_of(q);
        let qb = packed_bits_of(v);
        let dq_a = self.dequantize(qa);
        let dq_b = self.dequantize(qb);
        dq_a.iter()
            .zip(dq_b.iter())
            .map(|(&a, &b)| {
                let d = a - b;
                d * d
            })
            .sum()
    }

    /// Asymmetric L2 squared distance: FP32 query vs INT8 candidate.
    ///
    /// Delegates directly to `Sq8Codec::asymmetric_l2`.
    /// Cosine / IP variants require a separate codec wrapper that normalizes
    /// or negates at encode time.
    #[inline]
    fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32 {
        self.asymmetric_l2(q, packed_bits_of(v))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_codec() -> Sq8Codec {
        let vecs: Vec<Vec<f32>> = (0..50)
            .map(|i| vec![i as f32 * 0.1, -(i as f32) * 0.05, 1.0 + i as f32 * 0.02])
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        Sq8Codec::calibrate(&refs, 3)
    }

    /// `encode` round-trip: packed_bits in the UQV must match the raw
    /// `quantize` output from the underlying codec.
    #[test]
    fn encode_packed_bits_matches_raw_quantize() {
        let codec = make_codec();
        let v = vec![1.5f32, -0.3, 1.1];
        let raw = codec.quantize(&v);
        let quantized = <Sq8Codec as VectorCodec>::encode(&codec, &v);
        assert_eq!(quantized.as_ref().packed_bits(), raw.as_slice());
    }

    /// `fast_symmetric_distance` returns a non-negative finite value.
    #[test]
    fn fast_symmetric_distance_is_non_negative_finite() {
        let codec = make_codec();
        let a = <Sq8Codec as VectorCodec>::encode(&codec, &[0.5, -0.1, 1.0]);
        let b = <Sq8Codec as VectorCodec>::encode(&codec, &[2.0, -0.5, 1.5]);
        let d = codec.fast_symmetric_distance(&a, &b);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// `exact_asymmetric_distance` returns a non-negative finite value.
    #[test]
    fn exact_asymmetric_distance_is_non_negative_finite() {
        let codec = make_codec();
        let q = codec.prepare_query(&[0.5, -0.1, 1.0]);
        let v = <Sq8Codec as VectorCodec>::encode(&codec, &[2.0, -0.5, 1.5]);
        let d = codec.exact_asymmetric_distance(&q, &v);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// Verify the trait impl compiles via a generic function.
    fn use_vector_codec<C: VectorCodec>(c: &C, q: &[f32], v: &[f32]) -> f32 {
        let qv = c.encode(v);
        let qq = c.prepare_query(q);
        c.fast_symmetric_distance(&qv, &qv) + c.exact_asymmetric_distance(&qq, &qv)
    }

    #[test]
    fn trait_bounds_compile() {
        let codec = make_codec();
        let result = use_vector_codec(&codec, &[0.5, -0.1, 1.0], &[1.0, 0.0, 1.2]);
        assert!(result.is_finite());
    }
}
