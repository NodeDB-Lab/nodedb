//! Degree Centrality — normalized degree per node.
//!
//! `DC(v) = degree(v) / (n - 1)` where degree is in-degree, out-degree,
//! or total depending on the direction parameter.
//!
//! O(V) — reads degrees directly from CSR offset arrays.

use super::params::AlgoParams;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Degree Centrality on the CSR index.
///
/// Direction parameter: "in", "out", or "both" (default: "both").
/// Returns `(node_id, centrality)` rows sorted by centrality descending.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Degree);
    }

    let dir = params.direction.as_deref().unwrap_or("BOTH").to_uppercase();

    let normalizer = if n > 1 { (n - 1) as f64 } else { 1.0 };

    let mut scored: Vec<(usize, f64)> = (0..n)
        .map(|i| {
            let node_id = i as u32;
            let deg = match dir.as_str() {
                "IN" => csr.in_degree_raw(node_id),
                "OUT" => csr.out_degree_raw(node_id),
                _ => csr.out_degree_raw(node_id) + csr.in_degree_raw(node_id),
            };
            (i, deg as f64 / normalizer)
        })
        .collect();

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Degree);
    for (node, centrality) in scored {
        batch.push_node_f64(csr.node_name_raw(node as u32).to_string(), centrality);
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn degree_star_topology() {
        // Hub a connects to b, c, d.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.add_edge("a", "L", "d").unwrap();
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
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

        // a has out-degree 3 + in-degree 0 = 3 total. n-1 = 3. DC = 1.0.
        assert!((map["a"] - 1.0).abs() < 1e-9);
        // b has out-degree 0 + in-degree 1 = 1. DC = 1/3.
        assert!((map["b"] - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn degree_out_direction() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("a", "L", "c").unwrap();
        csr.compact();

        let params = AlgoParams {
            direction: Some("OUT".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params);
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

        assert!((map["a"] - 1.0).abs() < 1e-9); // out-degree 2, n-1=2
        assert!(map["b"].abs() < 1e-9); // out-degree 0
    }

    #[test]
    fn degree_empty_graph() {
        let csr = CsrIndex::new();
        assert!(run(&csr, &AlgoParams::default()).is_empty());
    }

    #[test]
    fn degree_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("solo");
        csr.compact();
        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 1);
    }
}
