// SPDX-License-Identifier: Apache-2.0

//! Vector search primitives shared by Origin, Lite, and WASM: HNSW + Vamana
//! indexes, scalar / SIMD distance kernels, the quantization codec frontier
//! (SQ8, PQ, IVF-PQ, OPQ, RaBitQ, BBQ, Ternary BitNet 1.58, Binary), the
//! VectorCollection runtime with mmap NVMe segments and background builder,
//! and the cost-model planner inputs (target_recall, oversample, ef_search,
//! query_dim, meta_token_budget, quantization).
//!
//! This crate has no platform-required cargo features for v0.1.0 — SIMD
//! kernels are gated by `#[cfg(target_arch)]` and dispatch happens at
//! runtime. The optional `acorn-baseline` feature retains the old ACORN-1
//! filtered-traversal heuristic for benchmarking against NaviX; not for
//! production use.

pub mod batch_distance;
pub mod codec_index;
pub mod delta;
pub mod distance;
pub mod dtype;
pub mod error;
pub mod hnsw;
pub mod hybrid;
pub mod matryoshka;
pub mod multivec;
pub mod quantize;
pub mod rerank;
#[cfg(test)]
mod test_support;
pub mod vamana;
pub mod visibility;

pub use distance::DistanceMetric;
pub use error::VectorError;
pub use hnsw::{HnswIndex, HnswParams, SearchResult};
pub use nodedb_types::Surrogate;
pub use quantize::Sq8Codec;
pub use visibility::NodeFilter;

// NaviX adaptive-local filtered traversal (VLDB 2025).
pub mod navix;

// SIEVE workload-driven subindex collection for stable predicates (SIEVE 2025).
pub mod sieve;

// Cost-based multidimensional vector query planner.
pub mod planner;

// Origin-only modules (always compiled for native targets).
pub mod adaptive_filter;
pub mod flat;
pub mod index_config;

// IVF-PQ index (large datasets).
pub mod ivf;

// NVMe mmap tier (requires libc + memmap2; not available on wasm32).
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_segment;

// Storage abstraction for HNSW vector data (Origin + Lite impls).
#[cfg(not(target_arch = "wasm32"))]
pub mod segment_backing;

// Background HNSW builder thread (depends on collection; not on wasm32).
#[cfg(not(target_arch = "wasm32"))]
pub mod builder;

// Full VectorCollection with segment lifecycle (not on wasm32).
#[cfg(not(target_arch = "wasm32"))]
pub mod collection;

// Re-exports for unconditionally compiled types.
pub use adaptive_filter::{
    FilterStrategy, FilterThresholds, adaptive_search, estimate_selectivity, select_strategy,
};
#[cfg(not(target_arch = "wasm32"))]
pub use builder::{BuildSender, CompleteReceiver, build_segment};
#[cfg(not(target_arch = "wasm32"))]
pub use collection::{
    BuildComplete, BuildKind, BuildRequest, BuiltSegment, CodecPlan, InstallOutcome, SegmentCodec,
    StorageTier, VectorCollection,
};
pub use flat::FlatIndex;
pub use index_config::{IndexConfig, IndexType};
pub use ivf::{IvfPqIndex, IvfPqParams};
#[cfg(not(target_arch = "wasm32"))]
pub use segment_backing::{PlainMmapBacking, VectorSegmentBacking};
