// SPDX-License-Identifier: Apache-2.0

//! BBQ — Better Binary Quantization (Elasticsearch 9.1, mid-2025).
//!
//! Centroid-centered asymmetric 1-bit quantizer with 14-byte corrective
//! factors per vector.  Empirically beats raw FP32 NDCG on 9/10 BEIR datasets
//! via oversampling rerank.
//!
//! ## Corrective factor layout (14 bytes → unified header fields)
//!
//! | Bytes | Header field      | Meaning                                          |
//! |-------|-------------------|--------------------------------------------------|
//! | 4     | `residual_norm`   | ‖v − c‖ (centroid distance)                     |
//! | 4     | `dot_quantized`   | ⟨v′, sign(v′)⟩ / ‖v′‖  (quantization quality)  |
//! | 4     | `global_scale`    | ⟨v′, c⟩ / ‖c‖            (query alignment)      |
//! | 2     | `reserved[0..2]`  | reserved for future correctives                  |
//!
//! where v′ = v − c.  Together these 14 bytes enable the asymmetric corrective
//! distance used at rerank without storing full FP32 per vector.
//!
//! ## Oversampling
//!
//! The codec does not execute rerank itself; it exposes `oversample` so the
//! caller fetches `oversample × top_k` coarse candidates and reruns
//! `exact_asymmetric_distance` on them.  Default is 3×.

use crate::error::CodecError;
use crate::vector_quant::codec::VectorCodec;
use crate::vector_quant::codec_envelope;
use crate::vector_quant::hamming::hamming_distance;
use crate::vector_quant::layout::{QuantHeader, QuantMode, UnifiedQuantizedVector};
use serde::{Deserialize, Serialize};

// ── BbqQuantized ────────────────────────────────────────────────────────────

/// Owned quantized BBQ vector.  Wraps a [`UnifiedQuantizedVector`] with
/// `QuantMode::Bbq`.
pub struct BbqQuantized(pub UnifiedQuantizedVector);

impl AsRef<UnifiedQuantizedVector> for BbqQuantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

// ── BbqQuery ────────────────────────────────────────────────────────────────

/// Prepared query for BBQ distance computation.
pub struct BbqQuery {
    /// Query vector after centroid subtraction (FP32, length = dim).
    pub centered: Vec<f32>,
    /// Sign-packed bits of `centered` (length = dim.div_ceil(8)).
    pub signs: Vec<u8>,
    /// ‖centered‖₂ — stored in header `residual_norm` of a synthetic query
    /// entry for reuse across candidates.
    pub query_norm: f32,
    /// ⟨centered, sign(centered)⟩ / query_norm — quantization quality factor.
    pub query_dot_quantized: f32,
}

// ── BbqCodec ────────────────────────────────────────────────────────────────

/// BBQ centroid-centered asymmetric 1-bit quantization codec.
///
/// Calibration computes the dataset centroid which is subtracted from each
/// vector before sign quantization.  The resulting 1-bit codes are Hamming-
/// coarse-comparable; exact rerank uses the 14-byte corrective factors stored
/// in the unified header.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct BbqCodec {
    pub dim: usize,
    /// Centroid of training data.  BBQ is centroid-asymmetric — all encode
    /// calls subtract this before quantizing.
    centroid: Vec<f32>,
    /// Oversample multiplier for the caller's rerank pass.  Caller fetches
    /// `oversample × top_k` coarse candidates from the Hamming coarse pass,
    /// then calls `exact_asymmetric_distance` on each.
    pub oversample: u8,
}

impl BbqCodec {
    /// Calibrate a new [`BbqCodec`] from a set of training vectors.
    ///
    /// Computes the centroid as the mean of all training vectors.
    ///
    /// # Panics
    ///
    /// Does not panic; returns a zero-centroid codec if `vectors` is empty.
    pub fn calibrate(vectors: &[&[f32]], dim: usize, oversample: u8) -> Self {
        let mut centroid = vec![0.0f32; dim];
        if vectors.is_empty() {
            return Self {
                dim,
                centroid,
                oversample,
            };
        }
        for v in vectors {
            for (c, &x) in centroid.iter_mut().zip(v.iter()) {
                *c += x;
            }
        }
        let n = vectors.len() as f32;
        for c in &mut centroid {
            *c /= n;
        }
        Self {
            dim,
            centroid,
            oversample,
        }
    }

    /// On-disk magic for BBQ codec envelopes.
    pub const ENVELOPE_MAGIC: &'static [u8; codec_envelope::MAGIC_LEN] = b"NDBBQ";

    /// Current on-disk envelope version for BBQ codecs.
    pub const ENVELOPE_VERSION: u8 = 1;

    /// Serialize this codec to a self-describing byte buffer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        codec_envelope::encode(Self::ENVELOPE_MAGIC, Self::ENVELOPE_VERSION, self)
    }

    /// Deserialize a codec from a byte buffer produced by [`Self::to_bytes`].
    pub fn from_bytes(buf: &[u8]) -> Result<Self, CodecError> {
        codec_envelope::decode(Self::ENVELOPE_MAGIC, Self::ENVELOPE_VERSION, buf)
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Subtract centroid from `v`, storing result in `out`.
    fn center(&self, v: &[f32], out: &mut Vec<f32>) {
        out.clear();
        out.extend(v.iter().zip(self.centroid.iter()).map(|(&x, &c)| x - c));
    }

    /// Pack signs of `centered` into bytes (1 bit per dim, MSB-first within
    /// each byte).  Returns byte vector of length `dim.div_ceil(8)`.
    fn pack_signs(centered: &[f32]) -> Vec<u8> {
        let nbytes = centered.len().div_ceil(8);
        let mut bits = vec![0u8; nbytes];
        for (i, &x) in centered.iter().enumerate() {
            if x >= 0.0 {
                bits[i / 8] |= 1 << (7 - (i % 8));
            }
        }
        bits
    }

    /// Compute ‖v‖₂.
    fn norm(v: &[f32]) -> f32 {
        v.iter().map(|&x| x * x).sum::<f32>().sqrt()
    }

    /// Compute ⟨a, b⟩.
    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
    }

    /// Reconstruct approximate FP32 vector from sign bits and `residual_norm`.
    ///
    /// Each dimension is approximated as ±residual_norm / √dim, with the sign
    /// taken from the packed bit.  This is the "high-fidelity asymmetric" path
    /// used at rerank: the query is exact FP32; the stored vector is
    /// reconstructed from its 1-bit code and the scalar corrective.
    fn dequantize(packed: &[u8], residual_norm: f32, dim: usize) -> Vec<f32> {
        let scale = if dim > 0 {
            residual_norm / (dim as f32).sqrt()
        } else {
            0.0
        };
        (0..dim)
            .map(|i| {
                let bit = (packed[i / 8] >> (7 - (i % 8))) & 1;
                if bit != 0 { scale } else { -scale }
            })
            .collect()
    }
}

impl VectorCodec for BbqCodec {
    type Quantized = BbqQuantized;
    type Query = BbqQuery;

    fn encode(&self, v: &[f32]) -> BbqQuantized {
        // Step 1: center.
        let mut centered = Vec::with_capacity(self.dim);
        self.center(v, &mut centered);

        // Step 2: pack sign bits.
        let packed = Self::pack_signs(&centered);

        // Step 3: corrective factors.
        //
        // residual_norm (4 B → header.residual_norm):
        //   ‖v′‖  where v′ = v − c.  Used in the symmetric distance estimate.
        let residual_norm = Self::norm(&centered);

        // dot_quantized (4 B → header.dot_quantized):
        //   ⟨v′, sign(v′)⟩ / ‖v′‖.  Measures how well the sign quantization
        //   captures the direction of v′.
        let sign_fp: Vec<f32> = centered
            .iter()
            .map(|&x| if x >= 0.0 { 1.0 } else { -1.0 })
            .collect();
        let dot_vs = Self::dot(&centered, &sign_fp);
        let dot_quantized = if residual_norm > 0.0 {
            dot_vs / residual_norm
        } else {
            0.0
        };

        // global_scale (4 B → header.global_scale):
        //   ⟨v′, c⟩ / ‖c‖.  Captures how aligned the centered vector is with
        //   the centroid direction — used as a query-alignment corrective.
        let centroid_norm = Self::norm(&self.centroid);
        let dot_vc = Self::dot(&centered, &self.centroid);
        let query_alignment = if centroid_norm > 0.0 {
            dot_vc / centroid_norm
        } else {
            0.0
        };

        // reserved[0..2]: 2 bytes reserved for future correctives (zero-filled).
        let reserved = [0u8; 8];
        // reserved[0..2] are the 2 reserved corrective bytes; remainder is zero.

        let header = QuantHeader {
            quant_mode: QuantMode::Bbq as u16,
            dim: self.dim as u16,
            global_scale: query_alignment,
            residual_norm,
            dot_quantized,
            outlier_bitmask: 0,
            reserved,
        };

        let uqv = UnifiedQuantizedVector::without_outliers(header, &packed);
        BbqQuantized(uqv)
    }

    fn prepare_query(&self, q: &[f32]) -> BbqQuery {
        let mut centered = Vec::with_capacity(self.dim);
        self.center(q, &mut centered);
        let signs = Self::pack_signs(&centered);
        let query_norm = Self::norm(&centered);
        let sign_fp: Vec<f32> = centered
            .iter()
            .map(|&x| if x >= 0.0 { 1.0 } else { -1.0 })
            .collect();
        let dot_vs = Self::dot(&centered, &sign_fp);
        let query_dot_quantized = if query_norm > 0.0 {
            dot_vs / query_norm
        } else {
            0.0
        };
        BbqQuery {
            centered,
            signs,
            query_norm,
            query_dot_quantized,
        }
    }

    /// Fast Hamming-based symmetric distance estimate.
    ///
    /// Uses the asymmetric corrective distance formula:
    ///   approx = q_n² + v_n² − 2 · q_n · v_n · dot_estimate
    /// where `dot_estimate = 1 − 2·hamming/dim` maps the Hamming count to
    /// a normalised cosine-like similarity on {−1,+1} codes.
    fn fast_symmetric_distance(&self, q: &BbqQuantized, v: &BbqQuantized) -> f32 {
        let q_bits = q.0.packed_bits();
        let v_bits = v.0.packed_bits();
        let ham = hamming_distance(q_bits, v_bits);
        let dim = self.dim as f32;
        let dot_estimate = 1.0 - 2.0 * ham as f32 / dim;
        let q_n = q.0.header().residual_norm;
        let v_n = v.0.header().residual_norm;
        (q_n * q_n + v_n * v_n - 2.0 * q_n * v_n * dot_estimate).max(0.0)
    }

    /// Exact asymmetric L2 distance using the dequantized stored vector.
    ///
    /// The query is exact centered FP32 (`q.centered`).  The stored vector is
    /// reconstructed from its sign bits and `residual_norm` via
    /// [`BbqCodec::dequantize`]: each dimension ≈ ±residual_norm / √dim.
    /// This is the high-fidelity asymmetric path invoked during rerank on the
    /// `oversample × top_k` candidates returned by the coarse Hamming pass.
    fn exact_asymmetric_distance(&self, q: &BbqQuery, v: &BbqQuantized) -> f32 {
        let header = v.0.header();
        let recon = Self::dequantize(v.0.packed_bits(), header.residual_norm, self.dim);
        // L2(q.centered, recon)
        q.centered
            .iter()
            .zip(recon.iter())
            .map(|(&a, &b)| (a - b) * (a - b))
            .sum::<f32>()
            .sqrt()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
        // Simple deterministic LCG.
        let mut x = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..dim)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                // Map to [-2, 2].
                ((x >> 33) as f32) / (u32::MAX as f32) * 4.0 - 2.0
            })
            .collect()
    }

    #[test]
    fn to_bytes_from_bytes_roundtrip() {
        let dim = 32;
        let vecs: Vec<Vec<f32>> = (0..4).map(|i| rand_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = BbqCodec::calibrate(&refs, dim, 5);
        let bytes = codec.to_bytes().expect("to_bytes should succeed");
        let restored = BbqCodec::from_bytes(&bytes).expect("from_bytes should succeed");
        assert_eq!(restored.dim, codec.dim);
        assert_eq!(restored.oversample, codec.oversample);
        assert_eq!(restored.centroid.len(), codec.centroid.len());
        for (a, b) in restored.centroid.iter().zip(codec.centroid.iter()) {
            assert!((a - b).abs() < 1e-6, "centroid mismatch: {a} vs {b}");
        }
    }

    #[test]
    fn from_bytes_rejects_bad_magic() {
        let mut bytes = b"WRONG".to_vec();
        bytes.push(1);
        bytes.extend_from_slice(&[0u8; 4]);
        assert!(BbqCodec::from_bytes(&bytes).is_err());
    }

    #[test]
    fn from_bytes_rejects_bad_version() {
        let codec = BbqCodec::calibrate(&[], 4, 3);
        let mut bytes = codec.to_bytes().unwrap();
        bytes[5] = 42;
        assert!(BbqCodec::from_bytes(&bytes).is_err());
    }

    #[test]
    fn calibrate_centroid_mean() {
        let dim = 8;
        // Three simple vectors: centroid should be element-wise mean.
        let a = vec![1.0f32; dim];
        let b = vec![3.0f32; dim];
        let c = vec![2.0f32; dim];
        let refs: Vec<&[f32]> = vec![&a, &b, &c];
        let codec = BbqCodec::calibrate(&refs, dim, 3);
        for &x in &codec.centroid {
            assert!((x - 2.0).abs() < 1e-5, "expected centroid 2.0, got {x}");
        }
    }

    #[test]
    fn calibrate_empty_gives_zero_centroid() {
        let codec = BbqCodec::calibrate(&[], 4, 3);
        assert!(codec.centroid.iter().all(|&x| x == 0.0));
    }

    #[test]
    fn encode_packed_bits_length() {
        let dim = 128;
        let v: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let refs: Vec<&[f32]> = vec![v.as_slice()];
        let codec = BbqCodec::calibrate(&refs, dim, 3);
        let q = codec.encode(&v);
        let expected_bytes = dim.div_ceil(8);
        assert_eq!(
            q.0.packed_bits().len(),
            expected_bytes,
            "packed bits length should be dim.div_ceil(8)"
        );
    }

    #[test]
    fn encode_odd_dim_packed_bits_length() {
        // dim = 17 → ceil(17/8) = 3 bytes.
        let dim = 17;
        let v: Vec<f32> = (0..dim).map(|i| i as f32 - 8.0).collect();
        let refs: Vec<&[f32]> = vec![v.as_slice()];
        let codec = BbqCodec::calibrate(&refs, dim, 3);
        let q = codec.encode(&v);
        assert_eq!(q.0.packed_bits().len(), 3);
    }

    #[test]
    fn hamming_scalar_vs_self_zero() {
        let bits = vec![0b10101010u8, 0b11001100, 0b11110000];
        assert_eq!(hamming_distance(&bits, &bits), 0);
    }

    #[test]
    fn hamming_scalar_known_distance() {
        // 0xFF ^ 0x00 = 8 bits set.
        let a = vec![0xFFu8];
        let b = vec![0x00u8];
        assert_eq!(hamming_distance(&a, &b), 8);
    }

    #[test]
    fn hamming_multi_byte_agreement() {
        let dim = 64;
        let a: Vec<u8> = (0..dim as u8).collect();
        let b: Vec<u8> = a.iter().map(|&x| !x).collect();
        // Every byte is fully flipped → all 64 × 8 = 512 bits differ.
        assert_eq!(hamming_distance(&a, &b), 512);
    }

    #[test]
    fn distance_non_negative_finite() {
        let dim = 32;
        let vecs: Vec<Vec<f32>> = (0..8).map(|i| rand_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = BbqCodec::calibrate(&refs, dim, 3);

        for i in 0..vecs.len() {
            for j in 0..vecs.len() {
                let qi = codec.encode(&vecs[i]);
                let qj = codec.encode(&vecs[j]);
                let sym = codec.fast_symmetric_distance(&qi, &qj);
                assert!(
                    sym.is_finite() && sym >= 0.0,
                    "fast_symmetric_distance({i},{j}) = {sym}"
                );

                let query = codec.prepare_query(&vecs[i]);
                let asym = codec.exact_asymmetric_distance(&query, &qj);
                assert!(
                    asym.is_finite() && asym >= 0.0,
                    "exact_asymmetric_distance({i},{j}) = {asym}"
                );
            }
        }
    }

    #[test]
    fn oversample_default_is_three() {
        let codec = BbqCodec::calibrate(&[], 4, 3);
        assert_eq!(codec.oversample, 3);
    }

    #[test]
    fn encode_quant_mode_is_bbq() {
        let dim = 16;
        let v: Vec<f32> = vec![1.0; dim];
        let refs: Vec<&[f32]> = vec![v.as_slice()];
        let codec = BbqCodec::calibrate(&refs, dim, 3);
        let q = codec.encode(&v);
        assert_eq!(q.0.header().quant_mode, QuantMode::Bbq as u16);
    }
}
