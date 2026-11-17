// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! GPU-accelerated vector index build (NVIDIA cuVS / CAGRA).
//!
//! This crate is feature-gated: enable `cuvs` to compile against the CUDA
//! toolkit. When the feature is off (the default), all entry points return
//! [`GpuError::NotCompiled`] so that downstream code can probe support
//! cleanly without conditional imports.
//!
//! ## Modes
//!
//! - **Build-on-GPU / serve-on-CPU** (default): graph constructed via cuVS
//!   on the GPU, then loaded onto the CPU Vamana serve path. Targets ~40×
//!   build-time speedup vs CPU.
//! - **Build-and-serve-on-GPU**: high-throughput tier where queries also
//!   execute against a CAGRA fixed-degree flat-graph index resident on the
//!   GPU. Warp-aligned, no thread divergence on neighbor updates.
//!
//! Lite / WASM builds never see this crate — it is opt-in for the Origin
//! server profile only.

pub mod cagra;
pub mod cuvs_build;
pub mod error;

pub use cagra::CagraIndex;
pub use cuvs_build::{GpuVamanaBuildOptions, build_vamana_on_gpu};
pub use error::GpuError;
