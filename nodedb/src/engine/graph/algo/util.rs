//! Shared graph algorithm utilities.

use crate::engine::graph::csr::CsrIndex;

/// Collect undirected neighbors of a node (out + in, deduplicated).
pub fn undirected_neighbors(csr: &CsrIndex, node: u32) -> Vec<u32> {
    let mut neighbors: Vec<u32> = csr.iter_out_edges(node).map(|(_, dst)| dst).collect();
    for (_, src) in csr.iter_in_edges(node) {
        if !neighbors.contains(&src) {
            neighbors.push(src);
        }
    }
    neighbors
}
