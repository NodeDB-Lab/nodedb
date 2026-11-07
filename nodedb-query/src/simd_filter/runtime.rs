// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Runtime dispatch
// ---------------------------------------------------------------------------

use super::scalar::{
    CmpOp, scalar_cmp_f64, scalar_cmp_i64, scalar_eq_u32, scalar_ne_u32, scalar_range_i64,
};

/// SIMD runtime for filter-to-bitmask operations.
pub struct FilterSimdRuntime {
    /// `values[i] == target` → bit i set.
    pub eq_u32: fn(&[u32], u32) -> Vec<u64>,
    /// `values[i] != target` → bit i set.
    pub ne_u32: fn(&[u32], u32) -> Vec<u64>,
    /// `values[i] > threshold` → bit i set.
    pub gt_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] >= threshold` → bit i set.
    pub gte_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] < threshold` → bit i set.
    pub lt_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] <= threshold` → bit i set.
    pub lte_f64: fn(&[f64], f64) -> Vec<u64>,
    /// `values[i] > threshold` → bit i set (i64).
    pub gt_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] >= threshold` → bit i set (i64).
    pub gte_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] < threshold` → bit i set (i64).
    pub lt_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `values[i] <= threshold` → bit i set (i64).
    pub lte_i64: fn(&[i64], i64) -> Vec<u64>,
    /// `min <= values[i] <= max` → bit i set.
    pub range_i64: fn(&[i64], i64, i64) -> Vec<u64>,
    pub name: &'static str,
}

impl FilterSimdRuntime {
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    eq_u32: super::avx512::avx512_eq_u32,
                    ne_u32: super::avx512::avx512_ne_u32,
                    gt_f64: super::avx512::avx512_gt_f64,
                    gte_f64: super::avx512::avx512_gte_f64,
                    lt_f64: super::avx512::avx512_lt_f64,
                    lte_f64: super::avx512::avx512_lte_f64,
                    gt_i64: super::avx512::avx512_gt_i64,
                    gte_i64: super::avx512::avx512_gte_i64,
                    lt_i64: super::avx512::avx512_lt_i64,
                    lte_i64: super::avx512::avx512_lte_i64,
                    range_i64: super::avx512::avx512_range_i64,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") {
                return Self {
                    eq_u32: super::avx2::avx2_eq_u32,
                    ne_u32: super::avx2::avx2_ne_u32,
                    gt_f64: super::avx2::avx2_gt_f64,
                    gte_f64: super::avx2::avx2_gte_f64,
                    lt_f64: super::avx2::avx2_lt_f64,
                    lte_f64: super::avx2::avx2_lte_f64,
                    gt_i64: super::avx2::avx2_gt_i64,
                    gte_i64: super::avx2::avx2_gte_i64,
                    lt_i64: super::avx2::avx2_lt_i64,
                    lte_i64: super::avx2::avx2_lte_i64,
                    range_i64: super::avx2::avx2_range_i64,
                    name: "avx2",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                eq_u32: super::neon::neon_eq_u32,
                ne_u32: super::neon::neon_ne_u32,
                gt_f64: super::neon::neon_gt_f64,
                gte_f64: super::neon::neon_gte_f64,
                lt_f64: super::neon::neon_lt_f64,
                lte_f64: super::neon::neon_lte_f64,
                gt_i64: super::neon::neon_gt_i64,
                gte_i64: super::neon::neon_gte_i64,
                lt_i64: super::neon::neon_lt_i64,
                lte_i64: super::neon::neon_lte_i64,
                range_i64: super::neon::neon_range_i64,
                name: "neon",
            };
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Self {
                eq_u32: super::wasm::wasm_eq_u32,
                ne_u32: super::wasm::wasm_ne_u32,
                gt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gt),
                gte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gte),
                lt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lt),
                lte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lte),
                gt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gt),
                gte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gte),
                lt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lt),
                lte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lte),
                range_i64: scalar_range_i64,
                name: if cfg!(target_feature = "simd128") {
                    "wasm-simd128"
                } else {
                    "scalar"
                },
            };
        }
        #[allow(unreachable_code)]
        Self {
            eq_u32: scalar_eq_u32,
            ne_u32: scalar_ne_u32,
            gt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gt),
            gte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Gte),
            lt_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lt),
            lte_f64: |v, t| scalar_cmp_f64(v, t, CmpOp::Lte),
            gt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gt),
            gte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Gte),
            lt_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lt),
            lte_i64: |v, t| scalar_cmp_i64(v, t, CmpOp::Lte),
            range_i64: scalar_range_i64,
            name: "scalar",
        }
    }
}

static FILTER_RUNTIME: std::sync::OnceLock<FilterSimdRuntime> = std::sync::OnceLock::new();

/// Get the global filter SIMD runtime.
pub fn filter_runtime() -> &'static FilterSimdRuntime {
    FILTER_RUNTIME.get_or_init(FilterSimdRuntime::detect)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd_filter::bitmask::{bitmask_to_indices, popcount};

    #[test]
    fn runtime_detects() {
        let rt = filter_runtime();
        assert!(!rt.name.is_empty());
    }

    #[test]
    fn eq_u32_basic() {
        let rt = filter_runtime();
        let values: Vec<u32> = (0..100).collect();
        let mask = (rt.eq_u32)(&values, 42);
        assert_eq!(popcount(&mask), 1);
        let indices = bitmask_to_indices(&mask);
        assert_eq!(indices, vec![42]);
    }

    #[test]
    fn ne_u32_basic() {
        let rt = filter_runtime();
        let values: Vec<u32> = (0..100).collect();
        let mask = (rt.ne_u32)(&values, 42);
        assert_eq!(popcount(&mask), 99);
    }

    #[test]
    fn eq_u32_repeated() {
        let rt = filter_runtime();
        // 1000 values cycling through 0..8.
        let values: Vec<u32> = (0..1000).map(|i| (i % 8) as u32).collect();
        let mask = (rt.eq_u32)(&values, 3);
        assert_eq!(popcount(&mask), 125); // 1000/8
        let indices = bitmask_to_indices(&mask);
        assert!(indices.iter().all(|&i| values[i as usize] == 3));
    }

    #[test]
    fn gt_f64_basic() {
        let rt = filter_runtime();
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let mask = (rt.gt_f64)(&values, 500.0);
        assert_eq!(popcount(&mask), 499); // 501..999
    }

    #[test]
    fn gte_f64_basic() {
        let rt = filter_runtime();
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let mask = (rt.gte_f64)(&values, 500.0);
        assert_eq!(popcount(&mask), 500); // 500..999
    }

    #[test]
    fn range_i64_basic() {
        let rt = filter_runtime();
        let values: Vec<i64> = (0..1000).collect();
        let mask = (rt.range_i64)(&values, 100, 200);
        assert_eq!(popcount(&mask), 101); // 100..=200
    }

    #[test]
    fn empty_input() {
        let rt = filter_runtime();
        assert!(popcount(&(rt.eq_u32)(&[], 0)) == 0);
        assert!(popcount(&(rt.gt_f64)(&[], 0.0)) == 0);
        assert!(popcount(&(rt.range_i64)(&[], 0, 100)) == 0);
    }

    #[test]
    fn large_input_eq_u32() {
        let rt = filter_runtime();
        let n: u32 = 10_000;
        let values: Vec<u32> = (0..n).map(|i| i % 256).collect();
        let mask = (rt.eq_u32)(&values, 0);
        // 0, 256, 512, ... → ceil(n/256) occurrences.
        let expected = values.iter().filter(|&&v| v == 0).count() as u64;
        assert_eq!(popcount(&mask), expected);
        let indices = bitmask_to_indices(&mask);
        assert!(indices.iter().all(|&i| values[i as usize] == 0));
    }

    #[test]
    fn i64_comparisons() {
        let rt = filter_runtime();
        let values: Vec<i64> = (0..100).collect();

        assert_eq!(popcount(&(rt.gt_i64)(&values, 50)), 49);
        assert_eq!(popcount(&(rt.gte_i64)(&values, 50)), 50);
        assert_eq!(popcount(&(rt.lt_i64)(&values, 50)), 50);
        assert_eq!(popcount(&(rt.lte_i64)(&values, 50)), 51);
    }
}
