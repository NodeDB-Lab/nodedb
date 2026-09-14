// SPDX-License-Identifier: Apache-2.0

//! AVX-512 kernels for x86_64.

#![cfg(target_arch = "x86_64")]

pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx512 l2: length mismatch");
    unsafe { l2_impl(a, b) }
}

#[target_feature(enable = "avx512f")]
unsafe fn l2_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx512 l2_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut sum = _mm512_setzero_ps();
        let chunks = n / 16;
        for i in 0..chunks {
            let off = i * 16;
            let va = _mm512_loadu_ps(a.as_ptr().add(off));
            let vb = _mm512_loadu_ps(b.as_ptr().add(off));
            let diff = _mm512_sub_ps(va, vb);
            sum = _mm512_fmadd_ps(diff, diff, sum);
        }
        let mut result = _mm512_reduce_add_ps(sum);
        for i in (chunks * 16)..n {
            let d = a[i] - b[i];
            result += d * d;
        }
        result
    }
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx512 cosine: length mismatch");
    unsafe { cosine_impl(a, b) }
}

#[target_feature(enable = "avx512f")]
unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx512 cosine_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut vdot = _mm512_setzero_ps();
        let mut vna = _mm512_setzero_ps();
        let mut vnb = _mm512_setzero_ps();
        let chunks = n / 16;
        for i in 0..chunks {
            let off = i * 16;
            let va = _mm512_loadu_ps(a.as_ptr().add(off));
            let vb = _mm512_loadu_ps(b.as_ptr().add(off));
            vdot = _mm512_fmadd_ps(va, vb, vdot);
            vna = _mm512_fmadd_ps(va, va, vna);
            vnb = _mm512_fmadd_ps(vb, vb, vnb);
        }
        let mut dot = _mm512_reduce_add_ps(vdot);
        let mut na = _mm512_reduce_add_ps(vna);
        let mut nb = _mm512_reduce_add_ps(vnb);
        for i in (chunks * 16)..n {
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
    assert_eq!(a.len(), b.len(), "avx512 ip: length mismatch");
    unsafe { ip_impl(a, b) }
}

#[target_feature(enable = "avx512f")]
unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "avx512 ip_impl: length mismatch");
    unsafe {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut vdot = _mm512_setzero_ps();
        let chunks = n / 16;
        for i in 0..chunks {
            let off = i * 16;
            let va = _mm512_loadu_ps(a.as_ptr().add(off));
            let vb = _mm512_loadu_ps(b.as_ptr().add(off));
            vdot = _mm512_fmadd_ps(va, vb, vdot);
        }
        let mut dot = _mm512_reduce_add_ps(vdot);
        for i in (chunks * 16)..n {
            dot += a[i] * b[i];
        }
        -dot
    }
}

use super::bbq::{l2_scalar_from_bytes, recon_scale};
/// Safe entry for `SimdRuntime`; the feature guard lives in `SimdRuntime::detect`.
pub fn l2_bbq(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    // SAFETY: selected only when `detect()` observed this tier's features.
    unsafe { l2_bbq_impl(centered, packed, residual_norm, dim) }
}

#[target_feature(enable = "avx512f")]
unsafe fn l2_bbq_impl(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    use std::arch::x86_64::*;

    let scale = recon_scale(residual_norm, dim);
    let pos = _mm512_set1_ps(scale);
    let neg = _mm512_set1_ps(-scale);
    let mut acc = _mm512_setzero_ps();

    let mut i = 0;
    while i + 16 <= dim {
        // SAFETY: `i + 16 <= dim` and the caller guarantees
        // `centered.len() >= dim * 4`; two packed bytes are in range because
        // 16 dims consume exactly two bytes.
        // SAFETY: in-bounds per the comment above.
        let q = unsafe { _mm512_loadu_ps(centered.as_ptr().add(i * 4).cast::<f32>()) };
        let b0 = unsafe { *packed.get_unchecked(i / 8) };
        let b1 = unsafe { *packed.get_unchecked(i / 8 + 1) };
        let mask: __mmask16 = (b0.reverse_bits() as u16) | ((b1.reverse_bits() as u16) << 8);
        let recon = _mm512_mask_blend_ps(mask, neg, pos);
        let d = _mm512_sub_ps(q, recon);
        acc = _mm512_fmadd_ps(d, d, acc);
        i += 16;
    }

    let sum = _mm512_reduce_add_ps(acc) + l2_scalar_from_bytes(centered, packed, scale, i, dim);
    sum.sqrt()
}
