// SPDX-License-Identifier: Apache-2.0

//! Runtime SIMD dispatch for vector distance and bitmap operations.

pub mod bbq;
pub mod hamming;
pub mod runtime;
pub mod scalar;

#[cfg(target_arch = "x86_64")]
pub mod avx2;
#[cfg(target_arch = "x86_64")]
pub mod avx512;
#[cfg(target_arch = "aarch64")]
pub mod neon;
#[cfg(target_arch = "wasm32")]
pub mod wasm_simd128;

pub use runtime::{SimdRuntime, runtime};
