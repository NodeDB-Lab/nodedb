//! CSR adjacency index — re-exported from the shared `nodedb-graph` crate.
//!
//! The core CSR implementation (index, weights, compaction, persistence,
//! statistics, memory tracking, traversal) lives in `nodedb-graph`.
//! This module re-exports everything and adds Origin-specific methods
//! that depend on Origin-only types (e.g., `EdgeStore`).

pub mod rebuild;

// Re-export all shared crate types.
pub use nodedb_graph::csr::index::{CsrIndex, Direction};
pub use nodedb_graph::csr::memory;
pub use nodedb_graph::csr::slice_accessors;
pub use nodedb_graph::csr::statistics::{self, GraphStatistics};
pub use nodedb_graph::csr::weights::{self, extract_weight_from_properties};
