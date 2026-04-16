//! Runtime SIMD detection and dispatch table.

use super::hamming::fast_hamming;
use super::scalar::{scalar_cosine, scalar_ip, scalar_l2};

#[cfg(target_arch = "x86_64")]
use super::{avx2, avx512};

#[cfg(target_arch = "aarch64")]
use super::neon;

/// Selected SIMD runtime — function pointers to the best available kernels.
pub struct SimdRuntime {
    pub l2_squared: fn(&[f32], &[f32]) -> f32,
    pub cosine_distance: fn(&[f32], &[f32]) -> f32,
    pub neg_inner_product: fn(&[f32], &[f32]) -> f32,
    pub hamming: fn(&[u8], &[u8]) -> u32,
    pub name: &'static str,
}

impl SimdRuntime {
    /// Detect CPU features and select the best kernels.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    l2_squared: avx512::l2_squared,
                    cosine_distance: avx512::cosine_distance,
                    neg_inner_product: avx512::neg_inner_product,
                    hamming: fast_hamming,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                return Self {
                    l2_squared: avx2::l2_squared,
                    cosine_distance: avx2::cosine_distance,
                    neg_inner_product: avx2::neg_inner_product,
                    hamming: fast_hamming,
                    name: "avx2+fma",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                l2_squared: neon::l2_squared,
                cosine_distance: neon::cosine_distance,
                neg_inner_product: neon::neg_inner_product,
                hamming: fast_hamming,
                name: "neon",
            };
        }
        #[allow(unreachable_code)]
        Self {
            l2_squared: scalar_l2,
            cosine_distance: scalar_cosine,
            neg_inner_product: scalar_ip,
            hamming: fast_hamming,
            name: "scalar",
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
