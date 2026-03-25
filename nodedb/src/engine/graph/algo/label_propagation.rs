//! Community Detection — Label Propagation Algorithm (LPA) on the CSR index.
//!
//! Each node adopts the most frequent label among its neighbors (ties broken
//! by smallest label for determinism). Iteration order is randomized with a
//! deterministic seed to avoid wave propagation artifacts.
//!
//! Performance target: 633K vertices / 34M edges in < 15s for 10 iterations.

use std::collections::HashMap;

use super::params::AlgoParams;
use super::progress::ProgressReporter;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run Label Propagation on the CSR index.
///
/// Returns an `AlgoResultBatch` with `(node_id, community_id)` rows.
/// Community IDs are dense node IDs — the label that "won" for each node.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::LabelPropagation);
    }

    let max_iter = params.iterations(10);
    let mut reporter = ProgressReporter::new(GraphAlgorithm::LabelPropagation, max_iter, None, n);

    // Initialize: each node is its own community.
    let mut labels: Vec<u32> = (0..n as u32).collect();

    // Deterministic node ordering — shuffled via Fisher-Yates with a
    // simple LCG PRNG seeded from node count (deterministic across runs
    // for the same graph). Randomized iteration order is critical:
    // sequential order causes wave propagation where early nodes dominate.
    let mut order: Vec<usize> = (0..n).collect();

    for iter in 1..=max_iter {
        // Shuffle order with deterministic seed (iteration-dependent so
        // each iteration processes in a different order).
        shuffle_deterministic(
            &mut order,
            (n as u64).wrapping_mul(iter as u64).wrapping_add(42),
        );

        let mut changed = 0usize;

        for &node in &order {
            let node_id = node as u32;

            // Count neighbor labels (both directions for undirected community detection).
            let mut label_counts: HashMap<u32, u32> = HashMap::new();

            for (_lid, neighbor) in csr.iter_out_edges(node_id) {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }
            for (_lid, neighbor) in csr.iter_in_edges(node_id) {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }

            if label_counts.is_empty() {
                continue; // Isolated node keeps its own label.
            }

            // Find the most frequent label. Ties broken by smallest label ID
            // for determinism.
            let Some(&max_count) = label_counts.values().max() else {
                continue;
            };
            let Some(best_label) = label_counts
                .iter()
                .filter(|&(_, count)| *count == max_count)
                .map(|(&label, _)| label)
                .min()
            else {
                continue;
            };

            if labels[node] != best_label {
                labels[node] = best_label;
                changed += 1;
            }
        }

        reporter.report_iteration(iter, Some(changed as f64));

        if changed == 0 {
            break; // Converged — no label changes.
        }
    }

    reporter.finish();

    // Build result.
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::LabelPropagation);
    for (node, &label) in labels.iter().enumerate() {
        batch.push_node_i64(csr.node_name(node as u32).to_string(), label as i64);
    }
    batch
}

/// Fisher-Yates shuffle with a simple LCG PRNG for deterministic ordering.
///
/// Uses a 64-bit linear congruential generator (Knuth's constants).
/// Not cryptographic — deterministic is the goal.
fn shuffle_deterministic(order: &mut [usize], seed: u64) {
    let mut state = seed | 1; // Ensure non-zero.
    let n = order.len();
    for i in (1..n).rev() {
        // LCG step.
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let j = (state >> 33) as usize % (i + 1);
        order.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_prop_triangle() {
        // Fully connected triangle — all should be same community.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        csr.add_edge("b", "L", "a");
        csr.add_edge("c", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let communities: Vec<i64> = rows
            .iter()
            .map(|r| r["community_id"].as_i64().unwrap())
            .collect();

        assert_eq!(communities[0], communities[1]);
        assert_eq!(communities[1], communities[2]);
    }

    #[test]
    fn label_prop_two_communities() {
        // Two cliques connected by a single bridge.
        // Clique 1: a-b-c (fully connected)
        // Clique 2: d-e-f (fully connected)
        // Bridge: c-d
        let mut csr = CsrIndex::new();
        for (s, d) in &[
            ("a", "b"),
            ("b", "a"),
            ("a", "c"),
            ("c", "a"),
            ("b", "c"),
            ("c", "b"),
            ("d", "e"),
            ("e", "d"),
            ("d", "f"),
            ("f", "d"),
            ("e", "f"),
            ("f", "e"),
            ("c", "d"),
            ("d", "c"),
        ] {
            csr.add_edge(s, "L", d);
        }
        csr.compact();

        let batch = run(
            &csr,
            &AlgoParams {
                max_iterations: Some(20),
                ..Default::default()
            },
        );
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let map: HashMap<&str, i64> = rows
            .iter()
            .map(|r| {
                (
                    r["node_id"].as_str().unwrap(),
                    r["community_id"].as_i64().unwrap(),
                )
            })
            .collect();

        // Within each clique, all should have the same label.
        assert_eq!(map["a"], map["b"]);
        assert_eq!(map["a"], map["c"]);
        assert_eq!(map["d"], map["e"]);
        assert_eq!(map["d"], map["f"]);
    }

    #[test]
    fn label_prop_isolated_node() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_node("isolated");
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn label_prop_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr, &AlgoParams::default());
        assert!(batch.is_empty());
    }

    #[test]
    fn label_prop_deterministic() {
        // Same graph, same params → same result.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        csr.compact();

        let params = AlgoParams::default();
        let r1 = run(&csr, &params).to_json().unwrap();
        let r2 = run(&csr, &params).to_json().unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn shuffle_deterministic_produces_permutation() {
        let mut order: Vec<usize> = (0..10).collect();
        shuffle_deterministic(&mut order, 12345);

        // Should still contain all elements.
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(sorted, (0..10).collect::<Vec<_>>());

        // Should be different from identity (overwhelmingly likely for n=10).
        assert_ne!(order, (0..10).collect::<Vec<_>>());
    }
}
