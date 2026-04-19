//! Weakly Connected Components — Union-Find with path compression and
//! union-by-rank on the CSR index.
//!
//! Treats the graph as undirected: for each edge (u, v), union(u, v).
//! Single pass over all outbound edges is sufficient since the CSR stores
//! both directions; iterating only outbound avoids double-counting.
//!
//! Performance target: 633K vertices / 34M edges in < 5s.

use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Weakly Connected Components on the CSR index.
///
/// Returns an `AlgoResultBatch` with `(node_id, component_id)` rows.
/// Component IDs are the dense node ID of the component root (deterministic).
pub fn run(csr: &CsrIndex) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Wcc);
    }

    let mut uf = UnionFind::new(n);

    // Single pass over all outbound edges. Since every edge (u, v) appears
    // as an outbound edge from u, this covers all edges without needing
    // inbound iteration.
    for u in 0..n {
        for (_lid, v) in csr.iter_out_edges_raw(u as u32) {
            uf.union(u, v as usize);
        }
    }

    // Also scan inbound edges to handle directed-only edges that might
    // not appear in both CSR halves for WCC's undirected semantics.
    for v in 0..n {
        for (_lid, u) in csr.iter_in_edges_raw(v as u32) {
            uf.union(u as usize, v);
        }
    }

    // Build result.
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Wcc);
    for node in 0..n {
        let component = uf.find(node);
        batch.push_node_i64(csr.node_name_raw(node as u32).to_string(), component as i64);
    }
    batch
}

/// Disjoint-set (Union-Find) with path compression and union-by-rank.
///
/// Amortized near-O(1) per find/union (inverse Ackermann).
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    /// Find the root of the set containing `x` with path compression.
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            // Path halving: point to grandparent.
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    /// Union the sets containing `a` and `b` by rank.
    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        // Union by rank: attach shorter tree under taller tree.
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wcc_single_component() {
        // a -> b -> c -> a (cycle — all connected)
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        csr.compact();

        let batch = run(&csr);
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let components: Vec<i64> = rows
            .iter()
            .map(|r| r["component_id"].as_i64().unwrap())
            .collect();

        // All same component.
        assert_eq!(components[0], components[1]);
        assert_eq!(components[1], components[2]);
    }

    #[test]
    fn wcc_two_components() {
        // Component 1: a -> b
        // Component 2: c -> d
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("c", "L", "d").unwrap();
        csr.compact();

        let batch = run(&csr);
        assert_eq!(batch.len(), 4);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["component_id"].as_i64().unwrap(),
                )
            })
            .collect();

        assert_eq!(map["a"], map["b"]);
        assert_eq!(map["c"], map["d"]);
        assert_ne!(map["a"], map["c"]);
    }

    #[test]
    fn wcc_directed_edges_treated_as_undirected() {
        // a -> b (only outbound). WCC should still put them in same component.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["component_id"].as_i64().unwrap(),
                )
            })
            .collect();

        assert_eq!(map["a"], map["b"]);
    }

    #[test]
    fn wcc_isolated_nodes() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_node("isolated");
        csr.compact();

        let batch = run(&csr);
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: std::collections::HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["component_id"].as_i64().unwrap(),
                )
            })
            .collect();

        assert_eq!(map["a"], map["b"]);
        assert_ne!(map["a"], map["isolated"]);
    }

    #[test]
    fn wcc_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr);
        assert!(batch.is_empty());
    }

    #[test]
    fn wcc_chain_graph() {
        // a -> b -> c -> d -> e (single chain = single component)
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "d").unwrap();
        csr.add_edge("d", "L", "e").unwrap();
        csr.compact();

        let batch = run(&csr);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let components: std::collections::HashSet<i64> = rows
            .iter()
            .map(|r| r["component_id"].as_i64().unwrap())
            .collect();

        assert_eq!(components.len(), 1, "chain should be one component");
    }
}
