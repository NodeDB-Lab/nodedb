// SPDX-License-Identifier: Apache-2.0

//! Parameter bundle for bidirectional shortest-path search, shared by the
//! durable dense path and the read-your-own-writes (overlay) path.

/// Bundled `shortest_path` arguments. Keeps `shortest_path` /
/// `shortest_path_dense` / `shortest_path_overlay` under clippy's
/// argument-count limit once the overlay parameter is threaded through,
/// without changing traversal semantics.
pub struct ShortestPathParams<'a> {
    pub src: &'a str,
    pub dst: &'a str,
    /// Empty permits every label. Otherwise, any listed label matches.
    pub label_filter: &'a [&'a str],
    pub max_depth: usize,
    pub max_visited: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}
