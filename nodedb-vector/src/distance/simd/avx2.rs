// SPDX-License-Identifier: Apache-2.0

//! AVX2+FMA kernels for x86_64.

#![cfg(target_arch = "x86_64")]

pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 l2: length mismatch");
    // SAFETY: caller verified avx2+fma via is_x86_feature_detected.
    unsafe { l2_squared_impl(a, b) }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn l2_squared_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 l2_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut sum = _mm256_setzero_ps();
        let chunks = n / 8;
        for i in 0..chunks {
            let off = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(off));
            let vb = _mm256_loadu_ps(b.as_ptr().add(off));
            let diff = _mm256_sub_ps(va, vb);
            sum = _mm256_fmadd_ps(diff, diff, sum);
        }
        let mut result = hsum256(sum);
        for i in (chunks * 8)..n {
            let d = a[i] - b[i];
            result += d * d;
        }
        result
    }
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 cosine: length mismatch");
    unsafe { cosine_impl(a, b) }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 cosine_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut vdot = _mm256_setzero_ps();
        let mut vna = _mm256_setzero_ps();
        let mut vnb = _mm256_setzero_ps();
        let chunks = n / 8;
        for i in 0..chunks {
            let off = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(off));
            let vb = _mm256_loadu_ps(b.as_ptr().add(off));
            vdot = _mm256_fmadd_ps(va, vb, vdot);
            vna = _mm256_fmadd_ps(va, va, vna);
            vnb = _mm256_fmadd_ps(vb, vb, vnb);
        }
        let mut dot = hsum256(vdot);
        let mut na = hsum256(vna);
        let mut nb = hsum256(vnb);
        for i in (chunks * 8)..n {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        let denom = (na * nb).sqrt();
        if denom < f32::EPSILON {
            1.0
        } else {
            (1.0 - dot / denom).max(0.0)
        }
    }
}

pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 ip: length mismatch");
    unsafe { ip_impl(a, b) }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx2 ip_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut vdot = _mm256_setzero_ps();
        let chunks = n / 8;
        for i in 0..chunks {
            let off = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(off));
            let vb = _mm256_loadu_ps(b.as_ptr().add(off));
            vdot = _mm256_fmadd_ps(va, vb, vdot);
        }
        let mut dot = hsum256(vdot);
        for i in (chunks * 8)..n {
            dot += a[i] * b[i];
        }
        -dot
    }
}

/// Horizontal sum of 8 × f32 in a __m256.
#[target_feature(enable = "avx2")]
unsafe fn hsum256(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    let hi = _mm256_extractf128_ps(v, 1);
    let lo = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    _mm_cvtss_f32(sums2)
}

use super::bbq::{l2_scalar_from_bytes, recon_scale};
/// Safe entry for `SimdRuntime`; the feature guard lives in `SimdRuntime::detect`.
pub fn l2_bbq(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    // SAFETY: selected only when `detect()` observed this tier's features.
    unsafe { l2_bbq_impl(centered, packed, residual_norm, dim) }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn l2_bbq_impl(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    use std::arch::x86_64::*;

    let scale = recon_scale(residual_norm, dim);
    let scale_v = _mm256_set1_ps(scale);
    let mut acc = _mm256_setzero_ps();

    let mut i = 0;
    while i + 8 <= dim {
        // SAFETY: `i + 8 <= dim` and the caller guarantees
        // `centered.len() >= dim * 4`, so the 32-byte unaligned load stays in
        // bounds; `i / 8` is in range because eight dims consume one byte.
        let q = unsafe { _mm256_loadu_ps(centered.as_ptr().add(i * 4).cast::<f32>()) };
        let byte = unsafe { *packed.get_unchecked(i / 8) } as usize;
        let signs = unsafe { _mm256_load_ps(SIGN_LANES[byte].0.as_ptr()) };
        let recon = _mm256_mul_ps(signs, scale_v);
        let d = _mm256_sub_ps(q, recon);
        acc = _mm256_fmadd_ps(d, d, acc);
        i += 8;
    }

    let sum = unsafe { hsum256(acc) } + l2_scalar_from_bytes(centered, packed, scale, i, dim);
    sum.sqrt()
}

/// `±1.0` lane patterns for every packed byte, MSB-first, 32-byte aligned for
/// an aligned load. Precomputed `reverse_bits` mapping (dim `k` → lane `k`).
#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[derive(Clone, Copy)]
#[repr(align(32))]
struct Aligned8([f32; 8]);

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
static SIGN_LANES: [Aligned8; 256] = build_sign_lanes();

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
const fn build_sign_lanes() -> [Aligned8; 256] {
    let mut table = [Aligned8([0.0; 8]); 256];
    let mut byte = 0usize;
    while byte < 256 {
        let mut lane = 0usize;
        while lane < 8 {
            let bit = (byte >> (7 - lane)) & 1;
            table[byte].0[lane] = if bit == 1 { 1.0 } else { -1.0 };
            lane += 1;
        }
        byte += 1;
    }
    table
}
