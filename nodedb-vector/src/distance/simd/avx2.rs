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
