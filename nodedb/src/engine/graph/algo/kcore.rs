//! k-Core Decomposition — Batagelj-Zaversnik peeling algorithm.
//!
//! Iteratively removes nodes with degree < k, increasing k until the
//! graph is empty. Returns the coreness value per node: the maximum k
//! such that the node belongs to the k-core.
//!
//! Algorithm: O(E) — Batagelj-Zaversnik. Maintain a degree array, process
//! nodes in order of degree, decrement neighbor degrees when removing a node.
//!
//! ArcadeDB doesn't have this — competitive differentiator.
//! Useful for: identifying dense subgraphs, network resilience, community structure.

use std::collections::HashSet;

use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run k-Core Decomposition on the CSR index.
///
/// Returns `(node_id, coreness)` rows sorted by coreness descending.
pub fn run(csr: &CsrIndex) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::KCore);
    }

    // Build undirected neighbor lists and compute initial degrees.
    let neighbor_lists: Vec<HashSet<u32>> = (0..n)
        .map(|i| {
            let node = i as u32;
            let mut set = HashSet::new();
            for (_, dst) in csr.iter_out_edges(node) {
                if dst != node {
                    set.insert(dst);
                }
            }
            for (_, src) in csr.iter_in_edges(node) {
                if src != node {
                    set.insert(src);
                }
            }
            set
        })
        .collect();

    let mut degree: Vec<usize> = neighbor_lists.iter().map(|s| s.len()).collect();
    let max_degree = degree.iter().copied().max().unwrap_or(0);

    // Batagelj-Zaversnik: sort nodes by degree using bin-sort.
    // bin[d] = list of nodes with current degree d.
    let mut bin: Vec<Vec<usize>> = vec![Vec::new(); max_degree + 1];
    for (i, &d) in degree.iter().enumerate() {
        bin[d].push(i);
    }

    // Position of each node in the processing order (for O(1) degree update).
    let mut coreness = vec![0usize; n];
    let mut processed = vec![false; n];

    // Process nodes in order of non-decreasing degree.
    for k in 0..=max_degree {
        // Process all nodes currently in bin[k].
        // Note: nodes may be added to bin[k] during processing as their
        // neighbors' degrees decrease.
        let mut idx = 0;
        while idx < bin[k].len() {
            let v = bin[k][idx];
            idx += 1;

            if processed[v] {
                continue;
            }
            processed[v] = true;
            coreness[v] = k;

            // For each unprocessed neighbor, decrease their degree.
            for &u in &neighbor_lists[v] {
                let ui = u as usize;
                if !processed[ui] && degree[ui] > k {
                    let old_deg = degree[ui];
                    degree[ui] = old_deg - 1;
                    let new_deg = degree[ui];
                    // Move u from bin[old_deg] to bin[new_deg].
                    // Since new_deg might be k (current level), add to current bin.
                    bin[new_deg].push(ui);
                }
            }
        }
    }

    // Build result sorted by coreness descending.
    let mut scored: Vec<(usize, usize)> = coreness.into_iter().enumerate().collect();
    scored.sort_by_key(|&(_, k)| std::cmp::Reverse(k));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::KCore);
    for (node, k) in scored {
        batch.push_node_i64(csr.node_name(node as u32).to_string(), k as i64);
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kcore_triangle() {
        // Fully connected triangle: each node has degree 2 → all in 2-core.
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

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        for row in &rows {
            assert_eq!(row["coreness"].as_i64().unwrap(), 2);
        }
    }

    #[test]
    fn kcore_star() {
        // Hub a connects to b, c, d. a has degree 3, leaves have degree 1.
        // 1-core includes all. 2-core: none (removing leaves drops a to 0).
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("a", "L", "d");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coreness"].as_i64().unwrap(),
                )
            })
            .collect();

        // All nodes have coreness 1 (1-core is the whole graph, 2-core is empty).
        assert_eq!(map["a"], 1);
        assert_eq!(map["b"], 1);
    }

    #[test]
    fn kcore_with_dense_subgraph() {
        // K4 (a,b,c,d fully connected) + pendant e connected only to a.
        // K4 nodes: coreness 3. e: coreness 1.
        let mut csr = CsrIndex::new();
        let k4 = ["a", "b", "c", "d"];
        for i in 0..4 {
            for j in 0..4 {
                if i != j {
                    csr.add_edge(k4[i], "L", k4[j]);
                }
            }
        }
        csr.add_edge("a", "L", "e");
        csr.add_edge("e", "L", "a");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coreness"].as_i64().unwrap(),
                )
            })
            .collect();

        assert_eq!(map["a"], 3);
        assert_eq!(map["b"], 3);
        assert_eq!(map["c"], 3);
        assert_eq!(map["d"], 3);
        assert_eq!(map["e"], 1);
    }

    #[test]
    fn kcore_isolated_node() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_node("isolated");
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["coreness"].as_i64().unwrap(),
                )
            })
            .collect();

        assert_eq!(map["isolated"], 0);
        assert_eq!(map["a"], 1);
    }

    #[test]
    fn kcore_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr).is_empty());
    }
}
