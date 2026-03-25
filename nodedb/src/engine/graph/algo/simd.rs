//! SIMD-accelerated kernels for graph algorithm hot loops.
//!
//! Runtime dispatch selects the fastest available instruction set:
//! - **AVX-512**: 8 × f64 per instruction (Intel Xeon, AMD Zen 4+)
//! - **AVX2+FMA**: 4 × f64 per instruction (most x86_64 since 2013)
//! - **NEON**: 2 × f64 per instruction (ARM64: Graviton, Apple Silicon)
//! - **Scalar fallback**: auto-vectorized by LLVM
//!
//! Kernels provided:
//! - `fill_f64`: broadcast a value into an f64 slice
//! - `l1_norm_delta`: L1 norm of element-wise difference `|a[i] - b[i]|`
//! - `dangling_sum`: sum elements where mask is true (dangling node rank)
//! - `sorted_intersection_count`: count common elements in two sorted u32 slices

/// Runtime-selected SIMD kernel set for graph algorithms.
pub struct GraphSimd {
    pub fill_f64: fn(&mut [f64], f64),
    pub l1_norm_delta: fn(&[f64], &[f64]) -> f64,
    pub dangling_sum: fn(&[f64], &[bool]) -> f64,
    pub sorted_intersection_count: fn(&[u32], &[u32]) -> usize,
    pub name: &'static str,
}

impl GraphSimd {
    /// Detect CPU features and select the best kernels.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    fill_f64: avx512::fill_f64,
                    l1_norm_delta: avx512::l1_norm_delta,
                    dangling_sum: avx512::dangling_sum,
                    sorted_intersection_count: scalar::sorted_intersection_count,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                return Self {
                    fill_f64: avx2::fill_f64,
                    l1_norm_delta: avx2::l1_norm_delta,
                    dangling_sum: avx2::dangling_sum,
                    sorted_intersection_count: scalar::sorted_intersection_count,
                    name: "avx2",
                };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                fill_f64: neon::fill_f64,
                l1_norm_delta: neon::l1_norm_delta,
                dangling_sum: neon::dangling_sum,
                sorted_intersection_count: scalar::sorted_intersection_count,
                name: "neon",
            };
        }

        #[allow(unreachable_code)]
        Self {
            fill_f64: scalar::fill_f64,
            l1_norm_delta: scalar::l1_norm_delta,
            dangling_sum: scalar::dangling_sum,
            sorted_intersection_count: scalar::sorted_intersection_count,
            name: "scalar",
        }
    }
}

// ══════════════════════════════════════════════════════════════════
//  Scalar fallback (auto-vectorized by LLVM)
// ══════════════════════════════════════════════════════════════════

pub mod scalar {
    /// Broadcast fill.
    pub fn fill_f64(dst: &mut [f64], val: f64) {
        for d in dst.iter_mut() {
            *d = val;
        }
    }

    /// L1 norm of element-wise difference.
    pub fn l1_norm_delta(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        let mut sum = 0.0f64;
        for i in 0..n {
            sum += (a[i] - b[i]).abs();
        }
        sum
    }

    /// Sum of a[i] where mask[i] is true.
    pub fn dangling_sum(a: &[f64], mask: &[bool]) -> f64 {
        let n = a.len().min(mask.len());
        let mut sum = 0.0f64;
        for i in 0..n {
            if mask[i] {
                sum += a[i];
            }
        }
        sum
    }

    /// Count common elements in two sorted u32 slices (merge intersection).
    pub fn sorted_intersection_count(a: &[u32], b: &[u32]) -> usize {
        let (mut i, mut j) = (0, 0);
        let mut count = 0;
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    count += 1;
                    i += 1;
                    j += 1;
                }
            }
        }
        count
    }
}

// ══════════════════════════════════════════════════════════════════
//  AVX-512 (x86_64, 8 × f64 per op)
// ══════════════════════════════════════════════════════════════════

#[cfg(target_arch = "x86_64")]
pub mod avx512 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    const LANE: usize = 8; // 512 bits / 64 bits

    pub fn fill_f64(dst: &mut [f64], val: f64) {
        // SAFETY: avx512f feature checked at runtime dispatch.
        unsafe {
            let n = dst.len();
            let vec_val = _mm512_set1_pd(val);
            let mut i = 0;
            while i + LANE <= n {
                _mm512_storeu_pd(dst.as_mut_ptr().add(i), vec_val);
                i += LANE;
            }
            while i < n {
                *dst.get_unchecked_mut(i) = val;
                i += 1;
            }
        }
    }

    pub fn l1_norm_delta(a: &[f64], b: &[f64]) -> f64 {
        unsafe {
            let n = a.len().min(b.len());
            let mut acc = _mm512_setzero_pd();
            let sign_mask = _mm512_set1_pd(-0.0);
            let mut i = 0;

            while i + LANE <= n {
                let va = _mm512_loadu_pd(a.as_ptr().add(i));
                let vb = _mm512_loadu_pd(b.as_ptr().add(i));
                let diff = _mm512_sub_pd(va, vb);
                let abs_diff = _mm512_andnot_pd(sign_mask, diff);
                acc = _mm512_add_pd(acc, abs_diff);
                i += LANE;
            }

            let mut sum = _mm512_reduce_add_pd(acc);
            while i < n {
                sum += (*a.get_unchecked(i) - *b.get_unchecked(i)).abs();
                i += 1;
            }
            sum
        }
    }

    pub fn dangling_sum(a: &[f64], mask: &[bool]) -> f64 {
        let n = a.len().min(mask.len());
        let mut sum = 0.0f64;
        for i in 0..n {
            if mask[i] {
                sum += a[i];
            }
        }
        sum
    }
}

// ══════════════════════════════════════════════════════════════════
//  AVX2 + FMA (x86_64, 4 × f64 per op)
// ══════════════════════════════════════════════════════════════════

#[cfg(target_arch = "x86_64")]
pub mod avx2 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    const LANE: usize = 4; // 256 bits / 64 bits

    pub fn fill_f64(dst: &mut [f64], val: f64) {
        // SAFETY: avx2 feature checked at runtime dispatch.
        unsafe {
            let n = dst.len();
            let vec_val = _mm256_set1_pd(val);
            let mut i = 0;
            while i + LANE <= n {
                _mm256_storeu_pd(dst.as_mut_ptr().add(i), vec_val);
                i += LANE;
            }
            while i < n {
                *dst.get_unchecked_mut(i) = val;
                i += 1;
            }
        }
    }

    pub fn l1_norm_delta(a: &[f64], b: &[f64]) -> f64 {
        // SAFETY: avx2 feature checked at runtime dispatch.
        unsafe {
            let n = a.len().min(b.len());
            let mut acc = _mm256_setzero_pd();
            let sign_mask = _mm256_set1_pd(-0.0);
            let mut i = 0;

            while i + LANE <= n {
                let va = _mm256_loadu_pd(a.as_ptr().add(i));
                let vb = _mm256_loadu_pd(b.as_ptr().add(i));
                let diff = _mm256_sub_pd(va, vb);
                let abs_diff = _mm256_andnot_pd(sign_mask, diff);
                acc = _mm256_add_pd(acc, abs_diff);
                i += LANE;
            }

            let hi = _mm256_extractf128_pd(acc, 1);
            let lo = _mm256_castpd256_pd128(acc);
            let sum128 = _mm_add_pd(lo, hi);
            let sum_hi = _mm_unpackhi_pd(sum128, sum128);
            let final_sum = _mm_add_sd(sum128, sum_hi);
            let mut sum = _mm_cvtsd_f64(final_sum);

            while i < n {
                sum += (*a.get_unchecked(i) - *b.get_unchecked(i)).abs();
                i += 1;
            }
            sum
        }
    }

    pub fn dangling_sum(a: &[f64], mask: &[bool]) -> f64 {
        let n = a.len().min(mask.len());
        let mut sum = 0.0f64;
        for i in 0..n {
            if mask[i] {
                sum += a[i];
            }
        }
        sum
    }
}

// ══════════════════════════════════════════════════════════════════
//  NEON (aarch64, 2 × f64 per op)
// ══════════════════════════════════════════════════════════════════

#[cfg(target_arch = "aarch64")]
pub mod neon {
    use std::arch::aarch64::*;

    const LANE: usize = 2; // 128 bits / 64 bits

    pub fn fill_f64(dst: &mut [f64], val: f64) {
        // SAFETY: NEON is guaranteed on aarch64.
        unsafe { fill_f64_inner(dst, val) }
    }

    unsafe fn fill_f64_inner(dst: &mut [f64], val: f64) {
        let n = dst.len();
        let vec_val = vdupq_n_f64(val);
        let mut i = 0;
        while i + LANE <= n {
            vst1q_f64(dst.as_mut_ptr().add(i), vec_val);
            i += LANE;
        }
        while i < n {
            *dst.get_unchecked_mut(i) = val;
            i += 1;
        }
    }

    pub fn l1_norm_delta(a: &[f64], b: &[f64]) -> f64 {
        unsafe { l1_norm_delta_inner(a, b) }
    }

    unsafe fn l1_norm_delta_inner(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        let mut acc = vdupq_n_f64(0.0);
        let mut i = 0;

        while i + LANE <= n {
            let va = vld1q_f64(a.as_ptr().add(i));
            let vb = vld1q_f64(b.as_ptr().add(i));
            let diff = vsubq_f64(va, vb);
            let abs_diff = vabsq_f64(diff);
            acc = vaddq_f64(acc, abs_diff);
            i += LANE;
        }

        let mut sum = vgetq_lane_f64(acc, 0) + vgetq_lane_f64(acc, 1);

        while i < n {
            sum += (*a.get_unchecked(i) - *b.get_unchecked(i)).abs();
            i += 1;
        }
        sum
    }

    pub fn dangling_sum(a: &[f64], mask: &[bool]) -> f64 {
        let n = a.len().min(mask.len());
        let mut sum = 0.0f64;
        for i in 0..n {
            if mask[i] {
                sum += a[i];
            }
        }
        sum
    }
}

// ══════════════════════════════════════════════════════════════════
//  Public convenience functions (use global dispatch)
// ══════════════════════════════════════════════════════════════════

use std::sync::OnceLock;

static RUNTIME: OnceLock<GraphSimd> = OnceLock::new();

/// Get the global SIMD runtime (initialized on first call).
pub fn runtime() -> &'static GraphSimd {
    RUNTIME.get_or_init(GraphSimd::detect)
}

/// SIMD-accelerated broadcast fill.
pub fn simd_fill_f64(dst: &mut [f64], val: f64) {
    (runtime().fill_f64)(dst, val);
}

/// SIMD-accelerated L1 norm of element-wise difference.
pub fn simd_l1_norm_delta(a: &[f64], b: &[f64]) -> f64 {
    (runtime().l1_norm_delta)(a, b)
}

/// SIMD-accelerated dangling node rank sum.
pub fn simd_dangling_sum(ranks: &[f64], is_dangling: &[bool]) -> f64 {
    (runtime().dangling_sum)(ranks, is_dangling)
}

/// SIMD/scalar sorted set intersection count.
pub fn simd_sorted_intersection_count(a: &[u32], b: &[u32]) -> usize {
    (runtime().sorted_intersection_count)(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_f64_works() {
        const TEST_VAL: f64 = 0.75;
        let mut buf = vec![0.0f64; 17]; // Non-aligned size to test tail.
        simd_fill_f64(&mut buf, TEST_VAL);
        for &v in &buf {
            assert_eq!(v, TEST_VAL);
        }
    }

    #[test]
    fn l1_norm_delta_basic() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5];
        let result = simd_l1_norm_delta(&a, &b);
        assert!(
            (result - 4.5).abs() < 1e-10,
            "L1 delta = {result}, expected 4.5"
        );
    }

    #[test]
    fn l1_norm_delta_identical() {
        let a = vec![1.0; 100];
        let b = vec![1.0; 100];
        assert!(simd_l1_norm_delta(&a, &b).abs() < 1e-15);
    }

    #[test]
    fn l1_norm_delta_empty() {
        assert_eq!(simd_l1_norm_delta(&[], &[]), 0.0);
    }

    #[test]
    fn dangling_sum_works() {
        let ranks = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let mask = vec![true, false, true, false, true]; // 0.1 + 0.3 + 0.5 = 0.9
        let result = simd_dangling_sum(&ranks, &mask);
        assert!((result - 0.9).abs() < 1e-10, "dangling sum = {result}");
    }

    #[test]
    fn dangling_sum_all_dangling() {
        let ranks = vec![0.25; 4];
        let mask = vec![true; 4];
        let result = simd_dangling_sum(&ranks, &mask);
        assert!((result - 1.0).abs() < 1e-10);
    }

    #[test]
    fn dangling_sum_none_dangling() {
        let ranks = vec![0.25; 4];
        let mask = vec![false; 4];
        assert!(simd_dangling_sum(&ranks, &mask).abs() < 1e-15);
    }

    #[test]
    fn sorted_intersection_count_basic() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 3, 5, 8, 9, 11];
        assert_eq!(simd_sorted_intersection_count(&a, &b), 3); // {3, 5, 9}
    }

    #[test]
    fn sorted_intersection_count_disjoint() {
        let a = vec![1, 3, 5];
        let b = vec![2, 4, 6];
        assert_eq!(simd_sorted_intersection_count(&a, &b), 0);
    }

    #[test]
    fn sorted_intersection_count_identical() {
        let a = vec![1, 2, 3, 4, 5];
        assert_eq!(simd_sorted_intersection_count(&a, &a), 5);
    }

    #[test]
    fn sorted_intersection_count_empty() {
        assert_eq!(simd_sorted_intersection_count(&[], &[1, 2, 3]), 0);
    }

    #[test]
    fn runtime_detection() {
        let rt = runtime();
        // Should detect something — at minimum "scalar".
        assert!(!rt.name.is_empty());
    }

    #[test]
    fn large_l1_norm() {
        // Test with sizes that exercise SIMD lanes + scalar tail.
        for size in [7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1000] {
            let a: Vec<f64> = (0..size).map(|i| i as f64 * 0.1).collect();
            let b: Vec<f64> = (0..size).map(|i| i as f64 * 0.1 + 0.01).collect();
            let result = simd_l1_norm_delta(&a, &b);
            let expected = size as f64 * 0.01;
            assert!(
                (result - expected).abs() < 1e-6,
                "size={size}: got {result}, expected {expected}"
            );
        }
    }
}
