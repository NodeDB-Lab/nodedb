//! Graph Diameter and Eccentricity on the CSR index.
//!
//! Eccentricity(v) = max distance from v to any reachable node.
//! Diameter = max eccentricity over all nodes.
//! Radius = min eccentricity over all nodes (excluding isolated nodes).
//!
//! Modes:
//! - **EXACT**: BFS from every node. O(V * (V + E)). Guaranteed correct.
//! - **APPROXIMATE** (default): double-sweep BFS. BFS from random node,
//!   BFS from farthest found. The longest path is a 2-approximation
//!   of the diameter. O(V + E).

use std::collections::VecDeque;

use super::params::AlgoParams;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Diameter/Eccentricity computation on the CSR index.
///
/// `params.mode`: "EXACT" or "APPROXIMATE" (default).
/// Returns `(diameter, radius)` as a single row.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Diameter);
    }

    let mode = params
        .mode
        .as_deref()
        .unwrap_or("APPROXIMATE")
        .to_uppercase();

    let (diameter, radius) = if mode == "EXACT" {
        compute_exact(csr, n)
    } else {
        compute_approximate(csr, n)
    };

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Diameter);
    batch.push_diameter(diameter, radius);
    batch
}

/// Exact computation: BFS from every node, compute all eccentricities.
fn compute_exact(csr: &CsrIndex, n: usize) -> (i64, i64) {
    let mut max_ecc = 0i64;
    let mut min_ecc = i64::MAX;

    for v in 0..n {
        let ecc = bfs_eccentricity(csr, v as u32, n);
        if ecc > 0 {
            // Only consider nodes that reach at least one other node.
            max_ecc = max_ecc.max(ecc);
            min_ecc = min_ecc.min(ecc);
        }
    }

    if min_ecc == i64::MAX {
        min_ecc = 0;
    }

    (max_ecc, min_ecc)
}

/// Approximate computation: double-sweep BFS.
///
/// 1. BFS from node 0 (or first non-isolated node), find farthest node `f`.
/// 2. BFS from `f`, find farthest node `g` — distance(f, g) ≈ diameter.
///
/// This gives a 2-approximation: diameter/2 <= result <= diameter.
/// In practice, often exact for small-world graphs.
fn compute_approximate(csr: &CsrIndex, n: usize) -> (i64, i64) {
    // Find first non-isolated node.
    let start = (0..n)
        .find(|&i| csr.out_degree_raw(i as u32) > 0 || csr.in_degree_raw(i as u32) > 0)
        .unwrap_or(0);

    // First sweep: find farthest node from start.
    let (farthest1, _dist1) = bfs_farthest(csr, start as u32, n);

    // Second sweep: find farthest node from farthest1.
    let (_farthest2, dist2) = bfs_farthest(csr, farthest1, n);

    let diameter = dist2 as i64;

    // For radius approximation, use diameter/2 (rough but consistent with
    // the 2-approximation bound).
    let radius = (diameter + 1) / 2;

    (diameter, radius)
}

/// BFS from source, return eccentricity (max distance to any reachable node).
fn bfs_eccentricity(csr: &CsrIndex, source: u32, n: usize) -> i64 {
    let mut dist = vec![u32::MAX; n];
    dist[source as usize] = 0;
    let mut queue = VecDeque::new();
    queue.push_back(source);
    let mut max_dist = 0u32;

    while let Some(v) = queue.pop_front() {
        let d = dist[v as usize];
        for neighbor in undirected_neighbors(csr, v) {
            let ni = neighbor as usize;
            if dist[ni] == u32::MAX {
                dist[ni] = d + 1;
                if dist[ni] > max_dist {
                    max_dist = dist[ni];
                }
                queue.push_back(neighbor);
            }
        }
    }

    max_dist as i64
}

/// BFS from source, return (farthest node, distance to farthest node).
fn bfs_farthest(csr: &CsrIndex, source: u32, n: usize) -> (u32, u32) {
    let mut dist = vec![u32::MAX; n];
    dist[source as usize] = 0;
    let mut queue = VecDeque::new();
    queue.push_back(source);
    let mut farthest = source;
    let mut max_dist = 0u32;

    while let Some(v) = queue.pop_front() {
        let d = dist[v as usize];
        for neighbor in undirected_neighbors(csr, v) {
            let ni = neighbor as usize;
            if dist[ni] == u32::MAX {
                dist[ni] = d + 1;
                if dist[ni] > max_dist {
                    max_dist = dist[ni];
                    farthest = neighbor;
                }
                queue.push_back(neighbor);
            }
        }
    }

    (farthest, max_dist)
}

use super::util::undirected_neighbors;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diameter_path() {
        // a - b - c - d (path of length 3).
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "d").unwrap();
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("EXACT".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        assert_eq!(rows[0]["diameter"].as_i64().unwrap(), 3);
        assert_eq!(rows[0]["radius"].as_i64().unwrap(), 2);
    }

    #[test]
    fn diameter_triangle() {
        let mut csr = CsrIndex::new();
        for (s, d) in &[("a", "b"), ("b", "c"), ("c", "a")] {
            csr.add_edge(s, "L", d).unwrap();
        }
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("EXACT".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        // Directed triangle: diameter depends on direction. With undirected
        // treatment, max distance is 1 (all directly connected).
        let d = rows[0]["diameter"].as_i64().unwrap();
        assert!((1..=2).contains(&d));
    }

    #[test]
    fn diameter_approximate() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "d").unwrap();
        csr.add_edge("d", "L", "e").unwrap();
        csr.compact();

        // Approximate should give a reasonable result.
        let batch = run(&csr, &AlgoParams::default());
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        let d = rows[0]["diameter"].as_i64().unwrap();
        // Double-sweep gives exact result for paths.
        assert_eq!(d, 4);
    }

    #[test]
    fn diameter_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr, &AlgoParams::default()).is_empty());
    }

    #[test]
    fn diameter_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("solo");
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("EXACT".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(rows[0]["diameter"].as_i64().unwrap(), 0);
    }
}
