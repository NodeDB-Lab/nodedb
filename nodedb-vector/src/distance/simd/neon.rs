// SPDX-License-Identifier: Apache-2.0

//! NEON kernels for ARM64.

#![cfg(target_arch = "aarch64")]

pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "neon l2: length mismatch");
    unsafe { l2_impl(a, b) }
}

unsafe fn l2_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "neon l2_impl: length mismatch");
    unsafe {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut sum = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            let diff = vsubq_f32(va, vb);
            sum = vfmaq_f32(sum, diff, diff);
        }
        let mut result = vaddvq_f32(sum);
        for i in (chunks * 4)..n {
            let d = a[i] - b[i];
            result += d * d;
        }
        result
    }
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "neon cosine: length mismatch");
    unsafe { cosine_impl(a, b) }
}

unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "neon cosine_impl: length mismatch");
    unsafe {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut vdot = vdupq_n_f32(0.0);
        let mut vna = vdupq_n_f32(0.0);
        let mut vnb = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            vdot = vfmaq_f32(vdot, va, vb);
            vna = vfmaq_f32(vna, va, va);
            vnb = vfmaq_f32(vnb, vb, vb);
        }
        let mut dot = vaddvq_f32(vdot);
        let mut na = vaddvq_f32(vna);
        let mut nb = vaddvq_f32(vnb);
        for i in (chunks * 4)..n {
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
    assert_eq!(a.len(), b.len(), "neon ip: length mismatch");
    unsafe { ip_impl(a, b) }
}

unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "neon ip_impl: length mismatch");
    unsafe {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut vdot = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            vdot = vfmaq_f32(vdot, va, vb);
        }
        let mut dot = vaddvq_f32(vdot);
        for i in (chunks * 4)..n {
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

/// Per-byte sign weights, MSB-first: the two 4-lane masks a packed byte
/// expands to.
static BIT_WEIGHTS: [[u32; 4]; 2] = [[0x80, 0x40, 0x20, 0x10], [0x08, 0x04, 0x02, 0x01]];

#[target_feature(enable = "neon")]
unsafe fn l2_bbq_impl(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    use std::arch::aarch64::*;

    let scale = recon_scale(residual_norm, dim);
    let pos = vdupq_n_f32(scale);
    let neg = vdupq_n_f32(-scale);
    let mut acc_lo = vdupq_n_f32(0.0);
    let mut acc_hi = vdupq_n_f32(0.0);

    // SAFETY: constant tables, always valid.
    let (weights_lo, weights_hi) = unsafe {
        (
            vld1q_u32(BIT_WEIGHTS[0].as_ptr()),
            vld1q_u32(BIT_WEIGHTS[1].as_ptr()),
        )
    };

    let mut i = 0;
    while i + 8 <= dim {
        // SAFETY: `i + 8 <= dim` and the caller guarantees the byte slices.
        let byte = unsafe { *packed.get_unchecked(i / 8) };
        // Broadcast the byte and test it against the per-dim weights: lane k of
        // each mask is 0xFFFF_FFFF when dim (i + k) has a set sign bit.
        let bits = vdupq_n_u32(byte as u32);
        let mask_lo = vtstq_u32(bits, weights_lo);
        let mask_hi = vtstq_u32(bits, weights_hi);
        let q_lo = unsafe { vld1q_f32(centered.as_ptr().add(i * 4).cast::<f32>()) };
        let q_hi = unsafe { vld1q_f32(centered.as_ptr().add((i + 4) * 4).cast::<f32>()) };
        let recon_lo = vbslq_f32(mask_lo, pos, neg);
        let recon_hi = vbslq_f32(mask_hi, pos, neg);
        let d_lo = vsubq_f32(q_lo, recon_lo);
        let d_hi = vsubq_f32(q_hi, recon_hi);
        acc_lo = vfmaq_f32(acc_lo, d_lo, d_lo);
        acc_hi = vfmaq_f32(acc_hi, d_hi, d_hi);
        i += 8;
    }

    let acc = vaddq_f32(acc_lo, acc_hi);
    // SAFETY: lane extraction of a register.
    let sum = vaddvq_f32(acc) + l2_scalar_from_bytes(centered, packed, scale, i, dim);
    sum.sqrt()
}
