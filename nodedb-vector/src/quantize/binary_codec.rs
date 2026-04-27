//! `VectorCodec` implementation for binary quantization.
//!
//! Introduces the `BinaryCodec` wrapper struct (holding `dim`) and implements
//! the dual-phase trait using the sign-bit encoding and Hamming distance
//! functions from `binary`. The prepared query is pre-encoded to bits; both
//! symmetric and asymmetric distances delegate to `hamming_distance`.

use nodedb_codec::vector_quant::{
    codec::{AdcLut, VectorCodec},
    layout::{QuantHeader, QuantMode, UnifiedQuantizedVector},
};

use crate::quantize::binary;

// ── Codec struct ──────────────────────────────────────────────────────────────

/// Binary quantization codec (sign-bit encoding, Hamming distance).
///
/// Stores only `dim` — no learned parameters. Suitable as a coarse pre-filter
/// before exact reranking with a higher-fidelity codec.
pub struct BinaryCodec {
    pub dim: usize,
}

// ── Newtype ───────────────────────────────────────────────────────────────────

/// Thin newtype wrapping `UnifiedQuantizedVector` for binary-encoded vectors.
pub struct BinaryQuantized(pub UnifiedQuantizedVector);

impl AsRef<UnifiedQuantizedVector> for BinaryQuantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

// ── Helper ────────────────────────────────────────────────────────────────────

#[inline]
fn packed_bits_of(q: &BinaryQuantized) -> &[u8] {
    q.0.packed_bits()
}

// ── VectorCodec impl ──────────────────────────────────────────────────────────

impl VectorCodec for BinaryCodec {
    type Quantized = BinaryQuantized;
    /// Pre-encoded query bits (`ceil(dim/8)` bytes).
    type Query = Vec<u8>;

    /// Encode an FP32 vector into binary sign bits.
    ///
    /// # Panics
    ///
    /// `UnifiedQuantizedVector::new` fails only on outlier-count/bitmask
    /// mismatches. With `outlier_bitmask = 0` and an empty outliers slice this
    /// can never happen. The `expect` is therefore unreachable in practice.
    fn encode(&self, v: &[f32]) -> Self::Quantized {
        let bits = binary::encode(v);
        let header = QuantHeader {
            quant_mode: QuantMode::Binary as u16,
            dim: self.dim as u16,
            global_scale: 0.0,
            residual_norm: 0.0,
            dot_quantized: 0.0,
            outlier_bitmask: 0,
            reserved: [0; 8],
        };
        let uqv = UnifiedQuantizedVector::new(header, &bits, &[])
            .expect("BinaryCodec::encode: layout construction is infallible (no outliers)");
        BinaryQuantized(uqv)
    }

    /// Pre-encode the query to binary sign bits for fast Hamming comparison.
    fn prepare_query(&self, q: &[f32]) -> Self::Query {
        binary::encode(q)
    }

    /// Binary codec has no ADC table — returns `None`.
    fn adc_lut(&self, _q: &Self::Query) -> Option<AdcLut> {
        None
    }

    /// Symmetric Hamming distance between two binary-encoded vectors.
    #[inline]
    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32 {
        binary::hamming_distance(packed_bits_of(q), packed_bits_of(v)) as f32
    }

    /// Asymmetric Hamming distance: pre-encoded query bits vs stored candidate.
    #[inline]
    fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32 {
        binary::hamming_distance(q, packed_bits_of(v)) as f32
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_codec(dim: usize) -> BinaryCodec {
        BinaryCodec { dim }
    }

    /// `encode` round-trip: packed_bits in the UQV must match `binary::encode`.
    #[test]
    fn encode_packed_bits_matches_raw_encode() {
        let dim = 8;
        let codec = make_codec(dim);
        let v = vec![1.0f32, -1.0, 1.0, -1.0, 0.5, -0.5, 1.0, -1.0];
        let raw = binary::encode(&v);
        let quantized = codec.encode(&v);
        assert_eq!(quantized.as_ref().packed_bits(), raw.as_slice());
    }

    /// `fast_symmetric_distance` returns a non-negative finite value.
    #[test]
    fn fast_symmetric_distance_is_non_negative_finite() {
        let codec = make_codec(8);
        let a = codec.encode(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]);
        let b = codec.encode(&[-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0]);
        let d = codec.fast_symmetric_distance(&a, &b);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// `exact_asymmetric_distance` returns a non-negative finite value.
    #[test]
    fn exact_asymmetric_distance_is_non_negative_finite() {
        let codec = make_codec(8);
        let q = codec.prepare_query(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]);
        let v = codec.encode(&[-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0]);
        let d = codec.exact_asymmetric_distance(&q, &v);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// Opposite-sign vectors should have maximum Hamming distance (= dim bits).
    #[test]
    fn opposite_vectors_have_max_hamming_distance() {
        let dim = 8;
        let codec = make_codec(dim);
        let a = codec.encode(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
        let b = codec.encode(&[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]);
        let d = codec.fast_symmetric_distance(&a, &b);
        assert_eq!(d, dim as f32);
    }

    /// Verify the trait impl compiles via a generic function.
    fn use_vector_codec<C: VectorCodec>(c: &C, q: &[f32], v: &[f32]) -> f32 {
        let qv = c.encode(v);
        let qq = c.prepare_query(q);
        c.fast_symmetric_distance(&qv, &qv) + c.exact_asymmetric_distance(&qq, &qv)
    }

    #[test]
    fn trait_bounds_compile() {
        let codec = make_codec(8);
        let result = use_vector_codec(
            &codec,
            &[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0],
            &[-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0],
        );
        assert!(result.is_finite());
    }
}
