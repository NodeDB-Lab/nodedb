//! Local Clustering Coefficient — per-node triangle density on the CSR index.
//!
//! For each node v with degree k:
//! `LCC(v) = 2 * triangles(v) / (k * (k - 1))`
//!
//! where triangles(v) is the number of edges between neighbors of v.
//! Nodes with degree < 2 get LCC = 0.0.
//!
//! Algorithm: for each node v, collect the neighbor set N(v). Sort it.
//! For each pair (u, w) in N(v), check if edge (u, w) exists via sorted
//! neighbor list intersection (binary search on sorted adjacency).
//!
//! Optimization: for high-degree nodes (> 1000 neighbors), approximate
//! via random sampling of neighbor pairs.
//!
//! Performance target: 633K vertices / 34M edges in < 30s.

use std::collections::HashSet;

use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Maximum neighbor count before switching to sampling approximation.
const HIGH_DEGREE_THRESHOLD: usize = 1000;

/// Number of neighbor pairs to sample for high-degree approximation.
const SAMPLE_PAIRS: usize = 10_000;

/// Run Local Clustering Coefficient on the CSR index.
///
/// Treats graph as undirected for neighbor collection (both out + in neighbors).
/// Returns `(node_id, coefficient)` rows.
pub fn run(csr: &CsrIndex) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Lcc);
    }

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Lcc);

    for node in 0..n {
        let node_id = node as u32;
        let coeff = compute_lcc(csr, node_id);
        batch.push_node_f64(csr.node_name(node as u32).to_string(), coeff);
    }

    batch
}

/// Compute LCC for a single node.
fn compute_lcc(csr: &CsrIndex, node: u32) -> f64 {
    // Collect undirected neighbor set (deduped).
    let mut neighbor_set: HashSet<u32> = HashSet::new();
    for (_lid, dst) in csr.iter_out_edges(node) {
        if dst != node {
            neighbor_set.insert(dst);
        }
    }
    for (_lid, src) in csr.iter_in_edges(node) {
        if src != node {
            neighbor_set.insert(src);
        }
    }

    let k = neighbor_set.len();
    if k < 2 {
        return 0.0;
    }

    let neighbors: Vec<u32> = neighbor_set.into_iter().collect();
    let possible_pairs = k * (k - 1) / 2;

    let triangles = if k > HIGH_DEGREE_THRESHOLD {
        // Approximate: sample random pairs.
        count_triangles_sampled(csr, &neighbors, possible_pairs)
    } else {
        // Exact: check all pairs.
        count_triangles_exact(csr, &neighbors)
    };

    // LCC = 2 * triangles / (k * (k-1))
    // Since we count each triangle once (unordered pair), and the denominator
    // is k*(k-1)/2 pairs, LCC = triangles / possible_pairs.
    triangles as f64 / possible_pairs as f64
}

/// Count triangles by checking all neighbor pairs (exact).
fn count_triangles_exact(csr: &CsrIndex, neighbors: &[u32]) -> usize {
    // Build a fast lookup set for neighbors.
    let neighbor_set: HashSet<u32> = neighbors.iter().copied().collect();
    let mut triangles = 0;

    for &u in neighbors {
        // Collect u's undirected neighbors and check intersection with neighbor_set.
        // Only check pairs where u < w to avoid double counting.
        for (_lid, w) in csr.iter_out_edges(u) {
            if w > u && neighbor_set.contains(&w) {
                triangles += 1;
            }
        }
        for (_lid, w) in csr.iter_in_edges(u) {
            if w > u && neighbor_set.contains(&w) {
                // Avoid counting an edge both as out and in.
                // Only count if the reverse (out) wasn't already counted.
                let already_counted = csr.iter_out_edges(u).any(|(_l, t)| t == w);
                if !already_counted {
                    triangles += 1;
                }
            }
        }
    }

    triangles
}

/// Count triangles via sampling for high-degree nodes (approximation).
fn count_triangles_sampled(csr: &CsrIndex, neighbors: &[u32], total_pairs: usize) -> usize {
    let neighbor_set: HashSet<u32> = neighbors.iter().copied().collect();
    let n = neighbors.len();
    let samples = SAMPLE_PAIRS.min(total_pairs);
    let mut found = 0usize;

    // Deterministic sampling via LCG.
    let mut state: u64 = (n as u64).wrapping_mul(0x517cc1b727220a95).wrapping_add(1);

    for _ in 0..samples {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let i = (state >> 33) as usize % n;
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let mut j = (state >> 33) as usize % n;
        if j == i {
            j = (j + 1) % n;
        }

        let u = neighbors[i];
        let w = neighbors[j];

        // Check if edge (u, w) exists.
        if has_undirected_edge(csr, u, w, &neighbor_set) {
            found += 1;
        }
    }

    // Extrapolate: found/samples ≈ triangles/total_pairs.
    (found as f64 / samples as f64 * total_pairs as f64).round() as usize
}

/// Check if an undirected edge exists between u and w.
fn has_undirected_edge(csr: &CsrIndex, u: u32, w: u32, _neighbor_set: &HashSet<u32>) -> bool {
    // Check outbound from u to w.
    for (_lid, dst) in csr.iter_out_edges(u) {
        if dst == w {
            return true;
        }
    }
    // Check inbound to u from w (i.e., outbound from w to u).
    for (_lid, src) in csr.iter_in_edges(u) {
        if src == w {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lcc_triangle() {
        // Fully connected triangle: each node has LCC = 1.0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        csr.add_edge("b", "L", "a");
        csr.add_edge("c", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.compact();

        let batch = run(&csr);
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let coeff = row["coefficient"].as_f64().unwrap();
            assert!(
                (coeff - 1.0).abs() < 1e-9,
                "node {} has LCC {coeff}, expected 1.0",
                row["node_id"]
            );
        }
    }

    #[test]
    fn lcc_star() {
        // Star topology: center a connects to b, c, d. No edges between b, c, d.
        // a has LCC = 0.0 (3 neighbors, 0 edges between them → 0/3).
        // b, c, d have LCC = 0.0 (degree 1 < 2).
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("a", "L", "d");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let coeff = row["coefficient"].as_f64().unwrap();
            assert!(
                coeff.abs() < 1e-9,
                "node {} has LCC {coeff}, expected 0.0",
                row["node_id"]
            );
        }
    }

    #[test]
    fn lcc_path() {
        // Path: a -> b -> c. Only b has 2 neighbors. No edge a-c → LCC(b) = 0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coefficient"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["a"].abs() < 1e-9); // degree 1
        assert!(map["b"].abs() < 1e-9); // 2 neighbors (a,c) but no a-c edge
        assert!(map["c"].abs() < 1e-9); // degree 1
    }

    #[test]
    fn lcc_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr);
        assert!(batch.is_empty());
    }

    #[test]
    fn lcc_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("lonely");
        csr.compact();

        let batch = run(&csr);
        assert_eq!(batch.len(), 1);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert!(rows[0]["coefficient"].as_f64().unwrap().abs() < 1e-9);
    }

    #[test]
    fn lcc_partial_connectivity() {
        // Diamond: a-b, a-c, b-d, c-d, b-c.
        // Node a: neighbors {b, c}. Edge b-c exists → 1 triangle / 1 pair = 1.0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("b", "L", "c");
        csr.add_edge("b", "L", "d");
        csr.add_edge("c", "L", "d");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coefficient"].as_f64().unwrap(),
                )
            })
            .collect();

        // a has neighbors b,c. b-c edge exists → LCC = 1/1 = 1.0.
        assert!(
            (map["a"] - 1.0).abs() < 1e-9,
            "a LCC = {}, expected 1.0",
            map["a"]
        );
    }
}
