//! Closeness Centrality — inverse sum of shortest-path distances.
//!
//! `CC(v) = (r - 1) / sum(shortest_path(v, u))` for all reachable u,
//! where r = number of reachable nodes from v.
//!
//! Uses Wasserman-Faust normalization for disconnected graphs:
//! `CC(v) = ((r - 1) / (n - 1)) * ((r - 1) / sum(d(v, u)))`
//! This normalizes by both reachability and graph size.
//!
//! BFS from each node (unweighted). O(V * (V + E)).

use std::collections::VecDeque;

use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Closeness Centrality on the CSR index.
///
/// Returns `(node_id, centrality)` sorted by centrality descending.
/// Uses Wasserman-Faust normalization for disconnected graphs.
pub fn run(csr: &CsrIndex) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Closeness);
    }

    let mut scored: Vec<(usize, f64)> = Vec::with_capacity(n);

    for v in 0..n {
        let (dist_sum, reachable) = bfs_distances(csr, v as u32, n);
        let cc = if reachable > 1 && dist_sum > 0.0 {
            let r = reachable as f64;
            let nm1 = (n - 1) as f64;
            // Wasserman-Faust: scale by reachability fraction.
            ((r - 1.0) / nm1) * ((r - 1.0) / dist_sum)
        } else {
            0.0
        };
        scored.push((v, cc));
    }

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Closeness);
    for (node, centrality) in scored {
        batch.push_node_f64(csr.node_name_raw(node as u32).to_string(), centrality);
    }
    batch
}

/// BFS from source, return (sum of distances, count of reachable nodes including source).
fn bfs_distances(csr: &CsrIndex, source: u32, n: usize) -> (f64, usize) {
    let mut dist = vec![u32::MAX; n];
    dist[source as usize] = 0;
    let mut queue = VecDeque::new();
    queue.push_back(source);
    let mut sum = 0u64;
    let mut reachable = 1usize;

    while let Some(v) = queue.pop_front() {
        let d = dist[v as usize];
        // Undirected: both out and in edges.
        for neighbor in undirected_neighbors(csr, v) {
            let ni = neighbor as usize;
            if dist[ni] == u32::MAX {
                dist[ni] = d + 1;
                sum += dist[ni] as u64;
                reachable += 1;
                queue.push_back(neighbor);
            }
        }
    }

    (sum as f64, reachable)
}

use super::util::undirected_neighbors;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closeness_path() {
        // a - b - c (path). Middle node b should have highest closeness.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["centrality"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["b"] > map["a"], "b should have higher closeness than a");
        assert!(map["b"] > map["c"], "b should have higher closeness than c");
    }

    #[test]
    fn closeness_complete_graph() {
        // Fully connected: all nodes equidistant → equal closeness.
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("b", "c"),
            ("c", "b"),
            ("a", "c"),
            ("c", "a"),
        ] {
            csr.add_edge(s, "L", d).unwrap();
        }
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let vals: Vec<f64> = rows
            .iter()
            .map(|r| r["centrality"].as_f64().unwrap())
            .collect();

        assert!((vals[0] - vals[1]).abs() < 1e-9);
        assert!((vals[1] - vals[2]).abs() < 1e-9);
        assert!((vals[0] - 1.0).abs() < 1e-9); // CC = 1.0 for complete graph
    }

    #[test]
    fn closeness_disconnected() {
        // a-b connected, c isolated. Wasserman-Faust gives c = 0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_node("c");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["centrality"].as_f64().unwrap(),
                )
            })
            .collect();

        assert!(map["c"].abs() < 1e-9, "isolated node should have CC=0");
        assert!(map["a"] > 0.0);
    }

    #[test]
    fn closeness_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr).is_empty());
    }
}
