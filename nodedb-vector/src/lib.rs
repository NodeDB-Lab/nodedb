pub mod batch_distance;
pub mod codec_index;
pub mod delta;
pub mod distance;
pub mod error;
pub mod hnsw;
pub mod hybrid;
pub mod matryoshka;
pub mod multivec;
pub mod quantize;
pub mod vamana;

pub use distance::DistanceMetric;
pub use error::VectorError;
pub use hnsw::{HnswIndex, HnswParams, SearchResult};
pub use nodedb_types::Surrogate;
pub use quantize::Sq8Codec;

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
#[cfg(feature = "ivf")]
pub mod ivf;

// NVMe mmap tier (requires libc).
#[cfg(feature = "collection")]
pub mod mmap_segment;

// Background HNSW builder thread.
#[cfg(feature = "collection")]
pub mod builder;

// Full VectorCollection with segment lifecycle.
#[cfg(feature = "collection")]
pub mod collection;

// Re-exports for feature-gated types.
pub use adaptive_filter::{
    FilterStrategy, FilterThresholds, adaptive_search, estimate_selectivity, select_strategy,
};
#[cfg(feature = "collection")]
pub use builder::{BuildSender, CompleteReceiver};
#[cfg(feature = "collection")]
pub use collection::{BuildComplete, BuildRequest, StorageTier, VectorCollection};
pub use flat::FlatIndex;
pub use index_config::{IndexConfig, IndexType};
#[cfg(feature = "ivf")]
pub use ivf::{IvfPqIndex, IvfPqParams};
