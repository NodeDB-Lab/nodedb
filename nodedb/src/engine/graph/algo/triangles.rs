//! Triangle Counting — global and per-node triangle enumeration.
//!
//! Two modes:
//! 1. **Per-node** (default): triangles incident to each node.
//! 2. **Global**: total triangle count.
//!
//! Algorithm: sorted-merge intersection on CSR neighbor lists.
//! For each edge (u, v) where u < v, count common neighbors w > v via
//! sorted set intersection. This guarantees each triangle {u, v, w}
//! with u < v < w is found exactly once.
//!
//! SIMD-accelerated sorted intersection via `simd::simd_sorted_intersection_count`.
//! O(E * sqrt(E)) for the node-iterator algorithm.

use super::params::AlgoParams;
use super::result::AlgoResultBatch;
use super::simd;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Triangle Counting on the CSR index.
///
/// `params.mode`: "GLOBAL" returns a single row with total count,
/// "PER_NODE" (default) returns per-node triangle counts.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::Triangles);
    }

    let mode = params.mode.as_deref().unwrap_or("PER_NODE").to_uppercase();

    // Build sorted undirected neighbor lists for each node.
    // Sorted lists enable SIMD-accelerated merge intersection.
    let sorted_neighbors: Vec<Vec<u32>> = (0..n)
        .map(|i| {
            let node = i as u32;
            let mut neighbor_set = std::collections::HashSet::new();
            for (_, dst) in csr.iter_out_edges(node) {
                if dst != node {
                    neighbor_set.insert(dst);
                }
            }
            for (_, src) in csr.iter_in_edges(node) {
                if src != node {
                    neighbor_set.insert(src);
                }
            }
            let mut neighbors: Vec<u32> = neighbor_set.into_iter().collect();
            neighbors.sort_unstable();
            neighbors
        })
        .collect();

    let mut per_node = vec![0u64; n];
    let mut global_count = 0u64;

    // For each edge (u, v) where u < v, count common neighbors w > v
    // via sorted intersection of neighbors[u] ∩ neighbors[v], restricted
    // to elements > v.
    for u in 0..n {
        for &v in &sorted_neighbors[u] {
            if (v as usize) <= u {
                continue; // Only process u < v.
            }

            // Get the portions of each neighbor list that are > v.
            // Since lists are sorted, binary search for v+1.
            let u_tail = tail_after(&sorted_neighbors[u], v);
            let v_tail = tail_after(&sorted_neighbors[v as usize], v);

            // SIMD-accelerated sorted intersection count.
            let common = simd::simd_sorted_intersection_count(u_tail, v_tail);

            if common > 0 {
                let c = common as u64;
                global_count += c;
                per_node[u] += c;
                per_node[v as usize] += c;

                // Credit each w individually.
                let mut i = 0;
                let mut j = 0;
                while i < u_tail.len() && j < v_tail.len() {
                    match u_tail[i].cmp(&v_tail[j]) {
                        std::cmp::Ordering::Less => i += 1,
                        std::cmp::Ordering::Greater => j += 1,
                        std::cmp::Ordering::Equal => {
                            per_node[u_tail[i] as usize] += 1;
                            i += 1;
                            j += 1;
                        }
                    }
                }
            }
        }
    }

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Triangles);

    if mode == "GLOBAL" {
        batch.push_node_i64("__global__".to_string(), global_count as i64);
    } else {
        for (node, &count) in per_node.iter().enumerate().take(n) {
            batch.push_node_i64(csr.node_name(node as u32).to_string(), count as i64);
        }
    }

    batch
}

/// Get the tail of a sorted slice after a given value (elements > val).
fn tail_after(sorted: &[u32], val: u32) -> &[u32] {
    // Binary search for the first element > val.
    match sorted.binary_search(&(val + 1)) {
        Ok(pos) => &sorted[pos..],
        Err(pos) => &sorted[pos..],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fully_connected_triangle() -> CsrIndex {
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
        csr
    }

    #[test]
    fn triangle_count_single_triangle() {
        let csr = fully_connected_triangle();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("GLOBAL".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(rows[0]["triangles"].as_i64().unwrap(), 1);
    }

    #[test]
    fn triangle_per_node() {
        let csr = fully_connected_triangle();

        let batch = run(&csr, &AlgoParams::default());
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        for row in &rows {
            assert_eq!(row["triangles"].as_i64().unwrap(), 1);
        }
    }

    #[test]
    fn triangle_no_triangles() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("GLOBAL".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(rows[0]["triangles"].as_i64().unwrap(), 0);
    }

    #[test]
    fn triangle_two_triangles() {
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("a", "c"),
            ("c", "a"),
            ("b", "c"),
            ("c", "b"),
            ("b", "d"),
            ("d", "b"),
            ("c", "d"),
            ("d", "c"),
        ] {
            csr.add_edge(s, "L", d);
        }
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                mode: Some("GLOBAL".into()),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(rows[0]["triangles"].as_i64().unwrap(), 2);
    }

    #[test]
    fn triangle_empty() {
        let csr = CsrIndex::new();
        assert!(run(&csr, &AlgoParams::default()).is_empty());
    }

    #[test]
    fn tail_after_basic() {
        let sorted = vec![1, 3, 5, 7, 9];
        assert_eq!(tail_after(&sorted, 3), &[5, 7, 9]);
        assert_eq!(tail_after(&sorted, 0), &[1, 3, 5, 7, 9]);
        assert_eq!(tail_after(&sorted, 9), &[] as &[u32]);
        assert_eq!(tail_after(&sorted, 4), &[5, 7, 9]);
    }
}
