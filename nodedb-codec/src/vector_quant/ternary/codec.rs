// SPDX-License-Identifier: Apache-2.0

//! [`TernaryCodec`] — BitNet b1.58 ternary [`VectorCodec`] implementation.

use crate::vector_quant::codec::VectorCodec;
use crate::vector_quant::layout::{QuantHeader, QuantMode, UnifiedQuantizedVector};

use super::packing::{cold_to_hot, pack_hot, quantize, unpack_hot};
use super::simd::ternary_dot;

/// BitNet b1.58 ternary codec.
///
/// Stateless aside from `dim`. Per-vector scaling (absmean) is computed in
/// [`VectorCodec::encode`] and stored in the header's `global_scale` field.
#[non_exhaustive]
pub struct TernaryCodec {
    pub dim: usize,
}

impl TernaryCodec {
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

/// Owned ternary-quantized vector (wraps [`UnifiedQuantizedVector`]).
pub struct TernaryQuantized(pub UnifiedQuantizedVector);

impl AsRef<UnifiedQuantizedVector> for TernaryQuantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

/// Prepared ternary query: hot-packed trits + per-vector scale.
pub struct TernaryQuery {
    pub trits_hot: Vec<u8>,
    pub scale: f32,
}

/// Squared L2 norm of ternary trits scaled by `scale`.
fn scaled_norm_sq(hot: &[u8], dim: usize, scale: f32) -> f32 {
    let trits = unpack_hot(hot, dim);
    let count: i32 = trits.iter().map(|&t| (t != 0) as i32).sum();
    scale * scale * count as f32
}

/// `‖a·sa - b·sb‖² ≈ sa²·‖a‖² + sb²·‖b‖² - 2·sa·sb·dot(a,b)`
fn l2_from_dot(dot: i32, norm_a: f32, norm_b: f32, sa: f32, sb: f32) -> f32 {
    (norm_a + norm_b - 2.0 * sa * sb * dot as f32).max(0.0)
}

/// Expand cold-packed bits to hot if needed.
fn ensure_hot(packed: &[u8], quant_mode: u16, dim: usize) -> std::borrow::Cow<'_, [u8]> {
    if quant_mode == QuantMode::TernaryPacked as u16 {
        std::borrow::Cow::Owned(cold_to_hot(packed, dim))
    } else {
        std::borrow::Cow::Borrowed(packed)
    }
}

impl VectorCodec for TernaryCodec {
    type Quantized = TernaryQuantized;
    type Query = TernaryQuery;

    fn encode(&self, v: &[f32]) -> TernaryQuantized {
        let (trits, scale) = quantize(v);
        let hot = pack_hot(&trits);
        let header = QuantHeader {
            quant_mode: QuantMode::TernarySimd as u16,
            dim: self.dim as u16,
            global_scale: scale,
            residual_norm: 0.0,
            dot_quantized: 0.0,
            outlier_bitmask: 0,
            reserved: [0; 8],
        };
        let uqv = UnifiedQuantizedVector::without_outliers(header, &hot);
        TernaryQuantized(uqv)
    }

    fn prepare_query(&self, q: &[f32]) -> TernaryQuery {
        let (trits, scale) = quantize(q);
        TernaryQuery {
            trits_hot: pack_hot(&trits),
            scale,
        }
    }

    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32 {
        let qh = q.0.header();
        let vh = v.0.header();

        let q_hot = ensure_hot(q.0.packed_bits(), qh.quant_mode, self.dim);
        let v_hot = ensure_hot(v.0.packed_bits(), vh.quant_mode, self.dim);

        let dot = ternary_dot(&q_hot, &v_hot, self.dim);
        let norm_q = scaled_norm_sq(&q_hot, self.dim, qh.global_scale);
        let norm_v = scaled_norm_sq(&v_hot, self.dim, vh.global_scale);
        l2_from_dot(dot, norm_q, norm_v, qh.global_scale, vh.global_scale)
    }

    fn exact_asymmetric_distance(&self, q: &TernaryQuery, v: &Self::Quantized) -> f32 {
        let vh = v.0.header();

        let v_hot = ensure_hot(v.0.packed_bits(), vh.quant_mode, self.dim);

        let dot = ternary_dot(&q.trits_hot, &v_hot, self.dim);
        let norm_q = scaled_norm_sq(&q.trits_hot, self.dim, q.scale);
        let norm_v = scaled_norm_sq(&v_hot, self.dim, vh.global_scale);
        l2_from_dot(dot, norm_q, norm_v, q.scale, vh.global_scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_produces_hot_format_by_default() {
        let codec = TernaryCodec::new(8);
        let v = vec![1.0f32, -1.0, 0.5, -0.5, 0.1, -0.1, 0.9, -0.9];
        let q = codec.encode(&v);
        assert_eq!(q.0.header().quant_mode, QuantMode::TernarySimd as u16);
    }

    #[test]
    fn dot_product_self_approx_norm_sq() {
        let dim = 16;
        let codec = TernaryCodec::new(dim);
        let v: Vec<f32> = (0..dim)
            .map(|i| {
                if i % 3 == 0 {
                    1.0
                } else if i % 3 == 1 {
                    -1.0
                } else {
                    0.0
                }
            })
            .collect();
        let qv = codec.encode(&v);
        let dist = codec.fast_symmetric_distance(&qv, &qv);
        assert!(dist < 1e-4, "self-distance should be ~0, got {dist}");
    }
}
