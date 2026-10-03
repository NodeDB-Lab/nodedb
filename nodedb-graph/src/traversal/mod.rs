// SPDX-License-Identifier: Apache-2.0

//! Graph traversal algorithms over CSR adjacency: BFS, bidirectional
//! shortest path, and subgraph materialization.
//!
//! Every algorithm respects a max-visited cap, so supernode fan-out cannot
//! consume unbounded memory. Each traversal records node access for hot/cold
//! partition decisions and prefetches frontier neighbors.

pub mod bfs;
pub mod shortest_path;
pub mod subgraph;

pub use nodedb_types::config::tuning::DEFAULT_MAX_VISITED;
