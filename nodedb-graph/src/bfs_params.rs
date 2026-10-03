// SPDX-License-Identifier: Apache-2.0

//! Parameter bundle for BFS traversal, shared by the durable and
//! read-your-own-writes (overlay) traversal paths.

use crate::csr::Direction;

/// Bundled BFS traversal arguments. Keeps `traverse_bfs` / `traverse_bfs_overlay`
/// / `traverse_bfs_dense` under clippy's argument-count limit without changing
/// traversal semantics.
pub struct BfsParams<'a> {
    pub start_nodes: &'a [&'a str],
    /// Empty keeps every edge. Otherwise an edge whose label is any listed
    /// label passes.
    pub label_filter: &'a [&'a str],
    pub direction: Direction,
    pub max_depth: usize,
    pub max_visited: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}
