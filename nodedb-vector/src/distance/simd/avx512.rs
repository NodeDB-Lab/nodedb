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
