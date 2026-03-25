//! Betweenness Centrality — Brandes' algorithm on the CSR index.
//!
//! For each source node s, run BFS from s, then accumulate dependency
//! scores on the reverse traversal. O(V * E) for unweighted graphs.
//!
//! For large graphs (> 100K nodes): approximate via random sampling of
//! source nodes (`sample_size` parameter). Sampling provides a good
//! estimate with O(sample_size * E) cost.

use std::collections::VecDeque;

use super::params::AlgoParams;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Betweenness Centrality on the CSR index.
///
/// `params.sample_size`: if set, only sample that many source nodes
/// (approximate). If `None`, compute exact centrality (all sources).
///
/// Returns `(node_id, centrality)` sorted by centrality descending.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Betweenness);
    }

    let mut cb = vec![0.0f64; n];

    // Determine source nodes: all or sampled subset.
    let sources: Vec<usize> = match params.sample_size {
        Some(sample) if sample < n => {
            // Deterministic sampling via LCG.
            let mut state: u64 = (n as u64).wrapping_mul(0x517cc1b727220a95).wrapping_add(1);
            let mut selected = Vec::with_capacity(sample);
            let mut used = vec![false; n];
            while selected.len() < sample {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let idx = (state >> 33) as usize % n;
                if !used[idx] {
                    used[idx] = true;
                    selected.push(idx);
                }
            }
            selected
        }
        _ => (0..n).collect(),
    };

    let scale = if params.sample_size.is_some() && sources.len() < n {
        // Scale approximate centrality to estimate full centrality.
        n as f64 / sources.len() as f64
    } else {
        1.0
    };

    // Brandes' algorithm: BFS from each source, then reverse accumulation.
    for &s in &sources {
        brandes_from_source(csr, s, n, &mut cb);
    }

    // Scale and normalize.
    if scale != 1.0 {
        for c in cb.iter_mut() {
            *c *= scale;
        }
    }

    // Build result sorted by centrality descending.
    let mut scored: Vec<(usize, f64)> = cb.into_iter().enumerate().collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Betweenness);
    for (node, centrality) in scored {
        batch.push_node_f64(csr.node_name(node as u32).to_string(), centrality);
    }
    batch
}

/// Run BFS from source `s` and accumulate betweenness contributions.
///
/// Brandes' single-source step:
/// 1. BFS from s, recording predecessors and shortest-path counts.
/// 2. Reverse BFS order, accumulating dependency scores.
fn brandes_from_source(csr: &CsrIndex, s: usize, n: usize, cb: &mut [f64]) {
    let mut stack: Vec<usize> = Vec::with_capacity(n);
    let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut sigma = vec![0.0f64; n]; // shortest-path count
    let mut dist = vec![-1i64; n]; // BFS distance (-1 = unvisited)
    let mut delta = vec![0.0f64; n]; // dependency accumulator

    sigma[s] = 1.0;
    dist[s] = 0;

    let mut queue = VecDeque::new();
    queue.push_back(s);

    // BFS phase: discover shortest paths and predecessors.
    while let Some(v) = queue.pop_front() {
        stack.push(v);

        // Iterate undirected neighbors (out + in for undirected betweenness).
        let neighbors = undirected_neighbors(csr, v as u32);
        for w in neighbors {
            let w = w as usize;
            if dist[w] < 0 {
                // First visit.
                dist[w] = dist[v] + 1;
                queue.push_back(w);
            }
            if dist[w] == dist[v] + 1 {
                // w is a successor of v on a shortest path.
                sigma[w] += sigma[v];
                predecessors[w].push(v);
            }
        }
    }

    // Reverse accumulation phase.
    while let Some(w) = stack.pop() {
        for &v in &predecessors[w] {
            let contrib = (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            delta[v] += contrib;
        }
        if w != s {
            cb[w] += delta[w];
        }
    }
}

use super::util::undirected_neighbors;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn betweenness_path_graph() {
        // a -> b -> c -> d (linear path)
        // b and c are on all shortest paths between endpoints.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "d");
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

        // b and c should have highest betweenness (they bridge a-d).
        assert!(map["b"] > map["a"]);
        assert!(map["c"] > map["d"]);
        // Endpoints have zero betweenness (no shortest paths pass through them).
        assert!(map["a"].abs() < 1e-9);
        assert!(map["d"].abs() < 1e-9);
    }

    #[test]
    fn betweenness_triangle() {
        // Fully connected triangle: all betweenness = 0.
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("b", "c"),
            ("c", "b"),
            ("a", "c"),
            ("c", "a"),
        ] {
            csr.add_edge(s, "L", d);
        }
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let c = row["centrality"].as_f64().unwrap();
            assert!(
                c.abs() < 1e-9,
                "node {} has BC {c}, expected 0",
                row["node_id"]
            );
        }
    }

    #[test]
    fn betweenness_star() {
        // Hub a connects to b, c, d. All shortest paths b-c, b-d, c-d go through a.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("a", "L", "d");
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

        assert!(map["a"] > map["b"]);
        assert!(map["b"].abs() < 1e-9);
    }

    #[test]
    fn betweenness_with_sampling() {
        let mut csr = CsrIndex::new();
        for i in 0..20 {
            csr.add_edge(&format!("n{i}"), "L", &format!("n{}", i + 1));
        }
        csr.compact();

        let params = AlgoParams {
            sample_size: Some(5),
            ..Default::default()
        };
        let batch = run(&csr, &params);
        assert_eq!(batch.len(), 21);
    }

    #[test]
    fn betweenness_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr, &AlgoParams::default()).is_empty());
    }
}
