//! Single-Source Shortest Path — Dijkstra's algorithm on weighted CSR.
//!
//! Uses `BinaryHeap` (min-heap via `Reverse`) with lazy deletion:
//! re-insert with lower distance, skip stale entries on pop.
//! O((V + E) log V).
//!
//! Requires weighted edges in CSR (see `csr::weights`). Unweighted edges
//! default to weight 1.0.
//!
//! Performance target: 633K vertices / 34M edges in < 5s from any source.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::params::AlgoParams;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run SSSP (Dijkstra) from a source node on the CSR index.
///
/// Returns `(node_id, distance)` for all reachable nodes. Unreachable nodes
/// get `f64::INFINITY`. Returns an error if the source node doesn't exist.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> Result<AlgoResultBatch, crate::Error> {
    let n = csr.node_count();
    if n == 0 {
        return Ok(AlgoResultBatch::new(GraphAlgorithm::Sssp));
    }

    let source = params
        .source_node
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "SSSP requires source_node parameter".into(),
        })?;

    let source_id = csr
        .node_id(source)
        .ok_or_else(|| crate::Error::BadRequest {
            detail: format!("source node '{source}' not found in graph"),
        })?;

    // Dijkstra requires non-negative edge weights. Pre-scan for negatives
    // to fail fast with a clear error rather than silently producing wrong results.
    if csr.has_weights() {
        for node in 0..n {
            for (_lid, _dst, w) in csr.iter_out_edges_weighted(node as u32) {
                if w < 0.0 {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "SSSP (Dijkstra) requires non-negative edge weights, found {w} on edge from '{}'",
                            csr.node_name(node as u32)
                        ),
                    });
                }
            }
        }
    }

    let mut dist = vec![f64::INFINITY; n];
    dist[source_id as usize] = 0.0;

    // Min-heap: (Reverse(distance), node_id).
    // Reverse wraps f64 for min-heap behavior with Rust's max-heap BinaryHeap.
    let mut heap: BinaryHeap<Reverse<(OrdF64, u32)>> = BinaryHeap::new();
    heap.push(Reverse((OrdF64(0.0), source_id)));

    while let Some(Reverse((OrdF64(d), u))) = heap.pop() {
        // Skip stale entries (lazy deletion).
        if d > dist[u as usize] {
            continue;
        }

        // Relax outbound edges.
        for (_lid, v, weight) in csr.iter_out_edges_weighted(u) {
            let new_dist = d + weight;
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                heap.push(Reverse((OrdF64(new_dist), v)));
            }
        }
    }

    // Build result batch.
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Sssp);
    for (node, &d) in dist.iter().enumerate() {
        batch.push_node_f64(csr.node_name(node as u32).to_string(), d);
    }
    Ok(batch)
}

/// Newtype wrapper for f64 that implements `Ord` for use in `BinaryHeap`.
///
/// NaN is treated as greater than all other values (pushed to bottom of
/// min-heap). This is safe because edge weights should never be NaN.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Greater)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weighted_graph() -> CsrIndex {
        // a --(2.0)--> b --(3.0)--> c
        // a --(10.0)-> c  (direct but longer)
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.0);
        csr.add_edge_weighted("b", "R", "c", 3.0);
        csr.add_edge_weighted("a", "R", "c", 10.0);
        csr.compact();
        csr
    }

    fn parse_results(batch: &AlgoResultBatch) -> std::collections::HashMap<String, f64> {
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        rows.iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap().to_string(),
                    // f64::INFINITY serializes as null in JSON.
                    r["distance"].as_f64().unwrap_or(f64::INFINITY),
                )
            })
            .collect()
    }

    #[test]
    fn sssp_shortest_via_relay() {
        let csr = weighted_graph();
        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let dists = parse_results(&batch);

        assert_eq!(dists["a"], 0.0);
        assert_eq!(dists["b"], 2.0);
        assert_eq!(dists["c"], 5.0); // via b (2+3) < direct (10)
    }

    #[test]
    fn sssp_unreachable_node() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 1.0);
        csr.add_node("island");
        csr.compact();

        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let dists = parse_results(&batch);

        assert_eq!(dists["a"], 0.0);
        assert_eq!(dists["b"], 1.0);
        assert!(dists["island"].is_infinite());
    }

    #[test]
    fn sssp_unweighted_defaults_to_one() {
        // Unweighted edges default to 1.0.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "d");
        csr.compact();

        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let dists = parse_results(&batch);

        assert_eq!(dists["a"], 0.0);
        assert_eq!(dists["b"], 1.0);
        assert_eq!(dists["c"], 2.0);
        assert_eq!(dists["d"], 3.0);
    }

    #[test]
    fn sssp_missing_source() {
        let csr = CsrIndex::new();
        let params = AlgoParams {
            source_node: Some("nonexistent".into()),
            ..Default::default()
        };
        let result = run(&csr, &params);
        // Empty graph returns empty batch (not error) since node_count is 0.
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn sssp_missing_source_in_nonempty_graph() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.compact();

        let params = AlgoParams {
            source_node: Some("nonexistent".into()),
            ..Default::default()
        };
        let result = run(&csr, &params);
        assert!(result.is_err());
    }

    #[test]
    fn sssp_no_source_param() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.compact();

        let params = AlgoParams::default();
        let result = run(&csr, &params);
        assert!(result.is_err());
    }

    #[test]
    fn sssp_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("solo");
        csr.compact();

        let params = AlgoParams {
            source_node: Some("solo".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let dists = parse_results(&batch);
        assert_eq!(dists["solo"], 0.0);
    }

    #[test]
    fn sssp_to_record_batch() {
        let csr = weighted_graph();
        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.schema().field(0).name(), "node_id");
        assert_eq!(rb.schema().field(1).name(), "distance");
    }

    #[test]
    fn sssp_diamond_graph() {
        // Diamond: a -> b (1), a -> c (4), b -> d (2), c -> d (1)
        // Shortest a->d: a->b->d = 3 (not a->c->d = 5)
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 1.0);
        csr.add_edge_weighted("a", "R", "c", 4.0);
        csr.add_edge_weighted("b", "R", "d", 2.0);
        csr.add_edge_weighted("c", "R", "d", 1.0);
        csr.compact();

        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let batch = run(&csr, &params).unwrap();
        let dists = parse_results(&batch);

        assert_eq!(dists["a"], 0.0);
        assert_eq!(dists["b"], 1.0);
        assert_eq!(dists["c"], 4.0);
        assert_eq!(dists["d"], 3.0); // via b
    }

    #[test]
    fn sssp_rejects_negative_weights() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", -1.0);
        csr.compact();

        let params = AlgoParams {
            source_node: Some("a".into()),
            ..Default::default()
        };
        let result = run(&csr, &params);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("non-negative"), "error: {err}");
    }
}
