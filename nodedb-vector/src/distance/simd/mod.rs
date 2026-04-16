//! Runtime SIMD dispatch for vector distance and bitmap operations.

pub mod hamming;
pub mod runtime;
pub mod scalar;

#[cfg(target_arch = "x86_64")]
pub mod avx2;
#[cfg(target_arch = "x86_64")]
pub mod avx512;
#[cfg(target_arch = "aarch64")]
pub mod neon;

pub use runtime::{SimdRuntime, runtime};
