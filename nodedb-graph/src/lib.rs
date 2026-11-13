// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Graph engine primitives shared by Origin, Lite, and WASM: CSR adjacency
//! index, traversal algorithms (PageRank, WCC, LabelPropagation, LCC, SSSP,
//! Betweenness, Closeness, Harmonic, Degree, Louvain, Triangles, Diameter,
//! k-Core), MATCH pattern engine, and the sharded BSP execution path used
//! by the distributed graph overlay.
//!
//! Graph is a cross-engine *overlay* — it does not own row storage. Edges
//! and nodes are projected from any data-bearing collection (typically a
//! `document_strict` collection) via `EDGE` and `NODE` declarations.

pub mod bfs_params;
pub mod csr;
pub mod error;
pub mod overlay_delta;
pub mod params;
pub mod path_overlay;
pub mod path_params;
pub mod properties;
pub mod sharded;
#[cfg(test)]
mod test_support;
pub mod traversal;
pub mod traversal_options;
pub mod traversal_overlay;
pub mod traversal_surrogate;

pub use bfs_params::BfsParams;
pub use csr::extract_weight_from_properties;
pub use csr::{CsrIndex, Direction, LocalNodeId};
pub use csr::{DegreeHistogram, GraphStatistics, LabelStats};
pub use error::{GraphError, MAX_EDGE_LABELS, MAX_NODES_PER_CSR};
pub use overlay_delta::GraphOverlayDelta;
pub use params::{AlgoColumnType, AlgoParams, GraphAlgorithm};
pub use path_params::ShortestPathParams;
pub use properties::{decode_edge_properties, encode_edge_properties, property_weight};
pub use sharded::ShardedCsrIndex;
pub use traversal_options::{GraphTraversalOptions, MAX_GRAPH_TRAVERSAL_DEPTH};
pub use traversal_surrogate::{SurrogateBfsParams, SurrogateHops};
