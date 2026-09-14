// SPDX-License-Identifier: Apache-2.0

//! Runtime SIMD detection and dispatch table.

use super::bbq;
use super::hamming::fast_hamming;
use super::scalar::{scalar_cosine, scalar_ip, scalar_l2};
use crate::distance::typed_scalar;

#[cfg(target_arch = "x86_64")]
use super::{avx2, avx512};

#[cfg(target_arch = "aarch64")]
use super::neon;

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
use super::wasm_simd128;

/// Function pointer type for half-precision byte-level distance kernels.
type HalfFn = fn(&[u8], &[u8], usize) -> f32;

/// Fused BBQ decode-and-distance: centred query bytes, packed sign
/// bits, and the candidate's residual scale. No intermediate Vec<f32>.
pub type BbqFn = fn(&[u8], &[u8], f32, usize) -> f32;

/// Selected SIMD runtime — function pointers to the best available kernels.
pub struct SimdRuntime {
    pub l2_squared: fn(&[f32], &[f32]) -> f32,
    pub cosine_distance: fn(&[f32], &[f32]) -> f32,
    pub neg_inner_product: fn(&[f32], &[f32]) -> f32,
    pub hamming: fn(&[u8], &[u8]) -> u32,
    pub name: &'static str,
    /// F16 fused decode-and-compute kernels (no intermediate Vec<f32>).
    pub l2_squared_f16: HalfFn,
    pub cosine_distance_f16: HalfFn,
    pub neg_inner_product_f16: HalfFn,
    /// BF16 fused decode-and-compute kernels.
    pub l2_squared_bf16: HalfFn,
    pub cosine_distance_bf16: HalfFn,
    pub neg_inner_product_bf16: HalfFn,
    /// Fused BBQ kernels (see `BbqFn`).
    pub l2_bbq: BbqFn,
}

impl SimdRuntime {
    /// Detect CPU features and select the best kernels.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            let has_vpopcntdq = std::is_x86_feature_detected!("avx512vpopcntdq");

            if std::is_x86_feature_detected!("avx512f") {
                let name = if has_vpopcntdq {
                    "avx512+vpopcntdq"
                } else {
                    "avx512"
                };
                let rt = Self {
                    l2_squared: avx512::l2_squared,
                    cosine_distance: avx512::cosine_distance,
                    neg_inner_product: avx512::neg_inner_product,
                    hamming: fast_hamming,
                    name,
                    l2_squared_f16: typed_scalar::l2_squared_f16,
                    cosine_distance_f16: typed_scalar::cosine_f16,
                    neg_inner_product_f16: typed_scalar::neg_inner_product_f16,
                    l2_squared_bf16: typed_scalar::l2_squared_bf16,
                    cosine_distance_bf16: typed_scalar::cosine_bf16,
                    neg_inner_product_bf16: typed_scalar::neg_inner_product_bf16,
                    l2_bbq: avx512::l2_bbq,
                };
                tracing::info!(kernel = rt.name, "vector SIMD kernel selected");
                debug_assert!(
                    !has_vpopcntdq || rt.name == "avx512+vpopcntdq",
                    "AVX-512 VPOPCNTDQ available but kernel did not select it"
                );
                return rt;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                let rt = Self {
                    l2_squared: avx2::l2_squared,
                    cosine_distance: avx2::cosine_distance,
                    neg_inner_product: avx2::neg_inner_product,
                    hamming: fast_hamming,
                    name: "avx2+fma",
                    l2_squared_f16: typed_scalar::l2_squared_f16,
                    cosine_distance_f16: typed_scalar::cosine_f16,
                    neg_inner_product_f16: typed_scalar::neg_inner_product_f16,
                    l2_squared_bf16: typed_scalar::l2_squared_bf16,
                    cosine_distance_bf16: typed_scalar::cosine_bf16,
                    neg_inner_product_bf16: typed_scalar::neg_inner_product_bf16,
                    l2_bbq: avx2::l2_bbq,
                };
                tracing::info!(kernel = rt.name, "vector SIMD kernel selected");
                return rt;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            let rt = Self {
                l2_squared: neon::l2_squared,
                cosine_distance: neon::cosine_distance,
                neg_inner_product: neon::neg_inner_product,
                hamming: fast_hamming,
                name: "neon",
                l2_squared_f16: typed_scalar::l2_squared_f16,
                cosine_distance_f16: typed_scalar::cosine_f16,
                neg_inner_product_f16: typed_scalar::neg_inner_product_f16,
                l2_squared_bf16: typed_scalar::l2_squared_bf16,
                cosine_distance_bf16: typed_scalar::cosine_bf16,
                neg_inner_product_bf16: typed_scalar::neg_inner_product_bf16,
                l2_bbq: neon::l2_bbq,
            };
            tracing::info!(kernel = rt.name, "vector SIMD kernel selected");
            return rt;
        }
        // The tier module is itself compile-time gated on `target_feature =
        // "simd128"`, so this arm's guard must match that gate exactly: no
        // runtime probe exists for wasm features.
        #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
        {
            let rt = Self {
                l2_squared: wasm_simd128::l2_squared,
                cosine_distance: wasm_simd128::cosine_distance,
                neg_inner_product: wasm_simd128::neg_inner_product,
                hamming: fast_hamming,
                name: "wasm-simd128",
                l2_squared_f16: typed_scalar::l2_squared_f16,
                cosine_distance_f16: typed_scalar::cosine_f16,
                neg_inner_product_f16: typed_scalar::neg_inner_product_f16,
                l2_squared_bf16: typed_scalar::l2_squared_bf16,
                cosine_distance_bf16: typed_scalar::cosine_bf16,
                neg_inner_product_bf16: typed_scalar::neg_inner_product_bf16,
                l2_bbq: wasm_simd128::l2_bbq,
            };
            tracing::info!(kernel = rt.name, "vector SIMD kernel selected");
            return rt;
        }
        #[allow(unreachable_code)]
        {
            let rt = Self {
                l2_squared: scalar_l2,
                cosine_distance: scalar_cosine,
                neg_inner_product: scalar_ip,
                hamming: fast_hamming,
                name: "scalar",
                l2_squared_f16: typed_scalar::l2_squared_f16,
                cosine_distance_f16: typed_scalar::cosine_f16,
                neg_inner_product_f16: typed_scalar::neg_inner_product_f16,
                l2_squared_bf16: typed_scalar::l2_squared_bf16,
                cosine_distance_bf16: typed_scalar::cosine_bf16,
                neg_inner_product_bf16: typed_scalar::neg_inner_product_bf16,
                l2_bbq: bbq::l2_bbq,
            };
            tracing::info!(kernel = rt.name, "vector SIMD kernel selected");
            rt
        }
    }
}

/// Global SIMD runtime — initialized once, used everywhere.
static RUNTIME: std::sync::OnceLock<SimdRuntime> = std::sync::OnceLock::new();

/// Get the global SIMD runtime (auto-detects on first call).
pub fn runtime() -> &'static SimdRuntime {
    RUNTIME.get_or_init(SimdRuntime::detect)
}

#[cfg(test)]
mod tests {
    use super::super::hamming::fast_hamming;
    use super::super::scalar::{scalar_cosine, scalar_ip, scalar_l2};
    use super::*;

    #[test]
    fn runtime_detects_features() {
        let rt = SimdRuntime::detect();
        assert!(!rt.name.is_empty());
        tracing::info!("SIMD runtime: {}", rt.name);
    }

    #[test]
    fn l2_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01 + 0.001).collect();

        let simd_result = (rt.l2_squared)(&a, &b);
        let scalar_result = scalar_l2(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.01,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn cosine_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..768).map(|i| (i as f32).sin()).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32).cos()).collect();

        let simd_result = (rt.cosine_distance)(&a, &b);
        let scalar_result = scalar_cosine(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.001,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn ip_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..128).map(|i| (i as f32) * 0.1).collect();
        let b: Vec<f32> = (0..128).map(|i| (i as f32) * 0.2).collect();

        let simd_result = (rt.neg_inner_product)(&a, &b);
        let scalar_result = scalar_ip(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.1,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn hamming_matches() {
        let a = vec![0b10101010u8; 16];
        let b = vec![0b01010101u8; 16];
        assert_eq!(fast_hamming(&a, &b), 128);
    }

    #[test]
    fn small_vectors() {
        let rt = runtime();
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        let l2 = (rt.l2_squared)(&a, &b);
        assert!((l2 - 27.0).abs() < 0.01);
    }
}
