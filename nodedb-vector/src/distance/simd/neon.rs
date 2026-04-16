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
