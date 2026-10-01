// SPDX-License-Identifier: Apache-2.0

//! Graph traversal algorithms over CSR adjacency.

pub mod bfs;
pub mod shortest_path;
pub mod subgraph;

pub use nodedb_types::config::tuning::DEFAULT_MAX_VISITED;
