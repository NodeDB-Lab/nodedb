// SPDX-License-Identifier: Apache-2.0

//! RaBitQ — 1-bit quantization with O(1/√D) MSE error bound (SIGMOD 2024).
//!
//! Algorithm outline
//! -----------------
//! 1. **Calibration**: compute centroid `c` over training vectors.
//! 2. **Rotation**: apply a randomised Walsh-Hadamard transform (WHT) with a
//!    seed-derived signed-diagonal matrix `D` to the residual `v - c`.
//!    - WHT requires a power-of-2 length; dimensions that are not pow2 are
//!      zero-padded before the transform then truncated after.
//!    - `D` is a vector of `±1` scalars derived deterministically from
//!      `rotation_seed` using an xorshift64 generator.
//! 3. **Encoding**: `code = sign(R·(v-c))`, packed 1-bit-per-dimension.
//! 4. **Distance estimation** (L2):
//!    `‖q-v‖² ≈ ‖v-c‖² + ‖q-c‖² − 2‖v-c‖‖q-c‖·(1 − 2·hamming/D)`
//!    with `O(1/√D)` MSE — see SIGMOD 2024 Theorem 4.
//!
//! IP / raw inner-product bias
//! ---------------------------
//! The angular estimator above is MSE-optimal for L2 and cosine. For raw IP
//! it carries a systematic bias. When `bias_correct = true` the codec
//! subtracts `dot_quantized` (stored in the [`QuantHeader`]) from the
//! asymmetric distance to partially compensate, following the TurboQuant-style
//! QJL residual correction. This is off by default; consumers that use raw IP
//! (RAG, attention scores) should enable it.
//!
//! [`rand_xoshiro`] is **not** a dependency of `nodedb-codec`. The signed-
//! diagonal flip vector is derived from `rotation_seed` via an inline
//! xorshift64 generator — no external crate required.

use crate::error::CodecError;
use crate::vector_quant::codec::VectorCodec;
use crate::vector_quant::codec_envelope;
use crate::vector_quant::hamming::hamming_distance;
use crate::vector_quant::layout::{QuantHeader, QuantMode, UnifiedQuantizedVector};
use serde::{Deserialize, Serialize};

// ── Xorshift64 (inline PRNG) ────────────────────────────────────────────────

/// Minimal xorshift64 PRNG for deterministic signed-diagonal generation.
#[inline]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

// ── WHT helpers ──────────────────────────────────────────────────────────────

/// Next power-of-two ≥ `n`, returning `n` itself if already pow2.
#[inline]
fn next_pow2(n: usize) -> usize {
    if n.is_power_of_two() {
        n
    } else {
        n.next_power_of_two()
    }
}

/// In-place Walsh-Hadamard Transform of a power-of-2 length slice.
/// O(N log N) butterfly. Does not normalise by 1/√N (sign-only code
/// does not require normalisation).
fn wht_inplace(buf: &mut [f32]) {
    let n = buf.len();
    debug_assert!(n.is_power_of_two());
    let mut step = 1usize;
    while step < n {
        let mut i = 0usize;
        while i < n {
            for j in i..i + step {
                let a = buf[j];
                let b = buf[j + step];
                buf[j] = a + b;
                buf[j + step] = a - b;
            }
            i += step * 2;
        }
        step *= 2;
    }
}

// ── Sign-pack / unpack helpers ───────────────────────────────────────────────

/// Pack `dim` signs from `rotated` (negative = 1-bit, non-negative = 0-bit)
/// into ceil(dim/8) bytes, LSB-first.
fn sign_pack(rotated: &[f32], dim: usize) -> Vec<u8> {
    let nbytes = dim.div_ceil(8);
    let mut out = vec![0u8; nbytes];
    for (i, &v) in rotated.iter().take(dim).enumerate() {
        if v < 0.0 {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// Dequantize sign-packed bits back to ±1 values (for dot_quantized calc).
fn sign_unpack(packed: &[u8], dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            if packed[i / 8] & (1 << (i % 8)) != 0 {
                -1.0f32
            } else {
                1.0f32
            }
        })
        .collect()
}

// ── RaBitQCodec ──────────────────────────────────────────────────────────────

/// RaBitQ codec: 1-bit quantization with O(1/√D) MSE error bound.
///
/// See module-level documentation for algorithm details.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct RaBitQCodec {
    pub dim: usize,
    /// Mean of training vectors; subtracted from each vector before rotation.
    centroid: Vec<f32>,
    /// Seed for the signed-diagonal flip vector used by the WHT rotation.
    rotation_seed: u64,
    /// If `true`, subtract `dot_quantized` from the asymmetric distance
    /// estimate as a TurboQuant-style IP-bias correction term.
    pub bias_correct: bool,
}

impl RaBitQCodec {
    /// Calibrate a new codec from a set of training vectors.
    ///
    /// - Computes the centroid as the coordinate-wise mean of `vectors`.
    /// - Stores `rotation_seed` for reproducible signed-diagonal generation.
    ///
    /// # Errors (none — returns Self directly)
    ///
    /// Returns a zero-centroid codec if `vectors` is empty.
    pub fn calibrate(vectors: &[&[f32]], dim: usize, rotation_seed: u64) -> Self {
        let centroid = if vectors.is_empty() {
            vec![0.0f32; dim]
        } else {
            let n = vectors.len() as f32;
            let mut c = vec![0.0f32; dim];
            for v in vectors {
                for (ci, &vi) in c.iter_mut().zip(v.iter()) {
                    *ci += vi;
                }
            }
            c.iter_mut().for_each(|x| *x /= n);
            c
        };
        Self {
            dim,
            centroid,
            rotation_seed,
            bias_correct: false,
        }
    }

    /// On-disk magic for RaBitQ codec envelopes.
    pub const ENVELOPE_MAGIC: &'static [u8; codec_envelope::MAGIC_LEN] = b"NDRBQ";

    /// Current on-disk envelope version for RaBitQ codecs.
    pub const ENVELOPE_VERSION: u8 = 1;

    /// Serialize this codec to a self-describing byte buffer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        codec_envelope::encode(Self::ENVELOPE_MAGIC, Self::ENVELOPE_VERSION, self)
    }

    /// Deserialize a codec from a byte buffer produced by [`Self::to_bytes`].
    pub fn from_bytes(buf: &[u8]) -> Result<Self, CodecError> {
        codec_envelope::decode(Self::ENVELOPE_MAGIC, Self::ENVELOPE_VERSION, buf)
    }

    /// Apply the randomised WHT rotation to a residual vector.
    ///
    /// Steps:
    /// 1. Apply signed-diagonal `D` (deterministic from `rotation_seed`).
    /// 2. Zero-pad to the next power of two if `dim` is not pow2.
    /// 3. WHT in-place.
    /// 4. Truncate back to `dim`.
    pub fn apply_rotation(&self, v: &[f32]) -> Vec<f32> {
        let dim = self.dim;
        let pow2 = next_pow2(dim);

        // Signed-diagonal multiply: generate ±1 per dim from seed.
        let mut seed = self.rotation_seed;
        let mut buf = vec![0.0f32; pow2];
        for (i, &vi) in v.iter().take(dim).enumerate() {
            let flip = if xorshift64(&mut seed) & 1 == 0 {
                1.0f32
            } else {
                -1.0f32
            };
            buf[i] = vi * flip;
        }
        // Trailing elements remain zero (pad).

        wht_inplace(&mut buf);
        buf.truncate(dim);
        buf
    }

    /// Encode a single vector into a [`UnifiedQuantizedVector`] with
    /// `QuantMode::RaBitQ`.
    ///
    /// The header fields populated are:
    /// - `global_scale` = `residual_norm` (‖v−c‖); both store the same value
    ///   so that consumers that use either field without context still have the
    ///   magnitude available.
    /// - `residual_norm` = ‖v−c‖.
    /// - `dot_quantized` = ⟨residual, dequantised_sign_vector⟩ / ‖v−c‖;
    ///   used for IP-bias correction when `bias_correct = true`.
    fn encode_inner(&self, v: &[f32]) -> UnifiedQuantizedVector {
        let dim = self.dim;

        // Step 1: residual = v - centroid
        let residual: Vec<f32> = v
            .iter()
            .zip(self.centroid.iter())
            .map(|(&vi, &ci)| vi - ci)
            .collect();

        // Step 2: ‖residual‖
        let residual_norm = residual.iter().map(|x| x * x).sum::<f32>().sqrt();

        // Step 3: rotate
        let rotated = self.apply_rotation(&residual);

        // Step 4: sign-pack → 1-bit code
        let packed = sign_pack(&rotated, dim);

        // Step 5: compute dot_quantized = ⟨residual, R⁻¹·sign(rotated)⟩ / ‖residual‖
        // Inverse rotation of the sign vector, then dot with original residual.
        let signs_fp = sign_unpack(&packed, dim);
        // Inverse WHT rotation: apply WHT again then re-apply D⁻¹ = D (since D² = I).
        let pow2 = next_pow2(dim);
        let mut sign_buf = vec![0.0f32; pow2];
        for (i, &s) in signs_fp.iter().enumerate() {
            sign_buf[i] = s;
        }
        wht_inplace(&mut sign_buf);
        // Re-apply signed diagonal (D is its own inverse since flips are ±1)
        let mut seed = self.rotation_seed;
        for x in sign_buf.iter_mut().take(dim) {
            let flip = if xorshift64(&mut seed) & 1 == 0 {
                1.0f32
            } else {
                -1.0f32
            };
            *x *= flip;
        }
        let dot_raw: f32 = residual
            .iter()
            .zip(sign_buf.iter().take(dim))
            .map(|(&r, &s)| r * s)
            .sum();
        let dot_quantized = if residual_norm > 0.0 {
            dot_raw / residual_norm
        } else {
            0.0
        };

        let header = QuantHeader {
            quant_mode: QuantMode::RaBitQ as u16,
            dim: dim as u16,
            global_scale: residual_norm,
            residual_norm,
            dot_quantized,
            outlier_bitmask: 0,
            reserved: [0u8; 8],
        };

        UnifiedQuantizedVector::without_outliers(header, &packed)
    }
}

// ── Quantized / Query newtypes ───────────────────────────────────────────────

/// Packed 1-bit quantized vector produced by [`RaBitQCodec::encode`].
pub struct RaBitQQuantized(UnifiedQuantizedVector);

impl AsRef<UnifiedQuantizedVector> for RaBitQQuantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

/// Prepared query for [`RaBitQCodec`] distance computations.
pub struct RaBitQQuery {
    /// Sign-packed rotated query (same bit layout as the stored codes).
    pub rotated_signs: Vec<u8>,
    /// ‖q − centroid‖.
    pub query_norm: f32,
}

// ── VectorCodec impl ─────────────────────────────────────────────────────────

impl VectorCodec for RaBitQCodec {
    type Quantized = RaBitQQuantized;
    type Query = RaBitQQuery;

    fn encode(&self, v: &[f32]) -> Self::Quantized {
        RaBitQQuantized(self.encode_inner(v))
    }

    fn prepare_query(&self, q: &[f32]) -> Self::Query {
        let dim = self.dim;
        let residual: Vec<f32> = q
            .iter()
            .zip(self.centroid.iter())
            .map(|(&qi, &ci)| qi - ci)
            .collect();
        let query_norm = residual.iter().map(|x| x * x).sum::<f32>().sqrt();
        let rotated = self.apply_rotation(&residual);
        let rotated_signs = sign_pack(&rotated, dim);
        RaBitQQuery {
            rotated_signs,
            query_norm,
        }
    }

    /// Symmetric distance estimate: both `q` and `v` are quantized.
    ///
    /// `approx_l2 = ‖v-c‖² + ‖q-c‖² − 2·‖v-c‖·‖q-c‖·(1 − 2·hamming/D)`
    ///
    /// The angular factor `1 − 2·hamming/D` approximates `cos(θ)` between
    /// the two sign-vectors. Error bound: O(1/√D) MSE.
    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32 {
        let qh = q.0.header();
        let vh = v.0.header();
        let qb = q.0.packed_bits();
        let vb = v.0.packed_bits();
        let h = hamming_distance(qb, vb);
        let dim = self.dim as f32;
        let dot_estimate = 1.0 - 2.0 * h as f32 / dim;
        let approx = qh.residual_norm * qh.residual_norm + vh.residual_norm * vh.residual_norm
            - 2.0 * qh.residual_norm * vh.residual_norm * dot_estimate;
        approx.max(0.0)
    }

    /// Asymmetric distance estimate: `q` is a prepared [`RaBitQQuery`], `v`
    /// is a stored quantized vector.
    ///
    /// Uses `query_norm` (exact ‖q−c‖) against `v.residual_norm` (exact ‖v−c‖)
    /// for higher fidelity than the symmetric variant.
    ///
    /// If `self.bias_correct = true`, subtract `v.dot_quantized` as a first-
    /// order IP-bias correction term (TurboQuant-style).
    fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32 {
        let vh = v.0.header();
        let vb = v.0.packed_bits();
        let h = hamming_distance(&q.rotated_signs, vb);
        let dim = self.dim as f32;
        let dot_estimate = 1.0 - 2.0 * h as f32 / dim;
        let mut approx = q.query_norm * q.query_norm + vh.residual_norm * vh.residual_norm
            - 2.0 * q.query_norm * vh.residual_norm * dot_estimate;
        if self.bias_correct {
            approx -= vh.dot_quantized;
        }
        approx.max(0.0)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vec(seed: u64, dim: usize) -> Vec<f32> {
        let mut s = seed | 1;
        (0..dim)
            .map(|_| {
                let v = xorshift64(&mut s);
                // Map to [-1, 1]
                (v as f32 / u64::MAX as f32) * 2.0 - 1.0
            })
            .collect()
    }

    #[test]
    fn to_bytes_from_bytes_roundtrip() {
        let dim = 64;
        let vecs: Vec<Vec<f32>> = (0..4).map(|i| random_vec(i as u64, dim)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = RaBitQCodec::calibrate(&refs, dim, 0xABCD_1234_5678_EF01);
        let bytes = codec.to_bytes().expect("to_bytes should succeed");
        let restored = RaBitQCodec::from_bytes(&bytes).expect("from_bytes should succeed");
        assert_eq!(restored.dim, codec.dim);
        assert_eq!(restored.rotation_seed, codec.rotation_seed);
        assert_eq!(restored.bias_correct, codec.bias_correct);
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
        assert!(RaBitQCodec::from_bytes(&bytes).is_err());
    }

    #[test]
    fn from_bytes_rejects_bad_version() {
        let codec = RaBitQCodec::calibrate(&[], 4, 1);
        let mut bytes = codec.to_bytes().unwrap();
        bytes[5] = 99;
        assert!(RaBitQCodec::from_bytes(&bytes).is_err());
    }

    #[test]
    fn apply_rotation_different_seeds_differ() {
        let dim = 64;
        let v: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let codec_a = RaBitQCodec::calibrate(&[], dim, 0xDEAD_BEEF_1234_5678);
        let codec_b = RaBitQCodec::calibrate(&[], dim, 0xCAFE_BABE_0000_0001);
        let rot_a = codec_a.apply_rotation(&v);
        let rot_b = codec_b.apply_rotation(&v);
        // Different seeds → different rotation outputs.
        let differ = rot_a
            .iter()
            .zip(rot_b.iter())
            .any(|(a, b)| (a - b).abs() > 1e-6);
        assert!(differ, "different seeds must produce different rotations");
    }

    #[test]
    fn encode_roundtrip_preserves_residual_norm() {
        let dim = 128;
        let vecs: Vec<Vec<f32>> = (0..16).map(|i| random_vec(i as u64, dim)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = RaBitQCodec::calibrate(&refs, dim, 42);
        let v = random_vec(99, dim);
        let q = codec.encode(&v);
        let h = q.0.header();
        // residual_norm should be finite and equal to global_scale.
        assert!(h.residual_norm.is_finite() && h.residual_norm >= 0.0);
        assert!((h.global_scale - h.residual_norm).abs() < 1e-6);
    }

    #[test]
    fn distance_non_negative_finite() {
        let dim = 64;
        let vecs: Vec<Vec<f32>> = (0..8).map(|i| random_vec(i as u64, dim)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = RaBitQCodec::calibrate(&refs, dim, 7);
        let v1 = codec.encode(&random_vec(100, dim));
        let v2 = codec.encode(&random_vec(200, dim));
        let sym = codec.fast_symmetric_distance(&v1, &v2);
        assert!(sym.is_finite() && sym >= 0.0, "sym distance: {sym}");
        let q = codec.prepare_query(&random_vec(300, dim));
        let asym = codec.exact_asymmetric_distance(&q, &v2);
        assert!(asym.is_finite() && asym >= 0.0, "asym distance: {asym}");
    }

    #[test]
    fn calibrate_identical_vectors_zero_residual() {
        let dim = 32;
        let v: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let refs = vec![v.as_slice(); 16];
        let codec = RaBitQCodec::calibrate(&refs, dim, 1);
        // Centroid == v, so residual = 0 → residual_norm = 0.
        let q = codec.encode(&v);
        assert!(
            q.0.header().residual_norm < 1e-5,
            "residual_norm should be ~0 for vector equal to centroid"
        );
    }
}
