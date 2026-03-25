//! PageRank — link analysis via power iteration on the CSR index.
//!
//! Algorithm: `PR(v) = (1 - d) / N + d * sum(PR(u) / out_degree(u))` for each
//! in-neighbor u. Iterates until L1 norm of rank delta < tolerance or
//! max_iterations reached. Dangling nodes (zero out-degree) redistribute
//! their rank uniformly across all nodes.
//!
//! Uses two rank vectors (current + next) to avoid allocation per iteration.
//! Performance target: 633K vertices / 34M edges in < 10s for 20 iterations.

use super::params::AlgoParams;
use super::progress::ProgressReporter;
use super::result::AlgoResultBatch;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::engine::graph::csr::CsrIndex;

/// Run PageRank on the CSR index.
///
/// Returns an `AlgoResultBatch` with `(node_id, rank)` rows sorted by rank
/// descending.
pub fn run(csr: &CsrIndex, params: &AlgoParams) -> AlgoResultBatch {
    let n = csr.node_count();
    if n == 0 {
        return AlgoResultBatch::new(GraphAlgorithm::PageRank);
    }

    let damping = params.damping_factor();
    let max_iter = params.iterations(20);
    let tolerance = params.convergence_tolerance();

    let mut reporter =
        ProgressReporter::new(GraphAlgorithm::PageRank, max_iter, Some(tolerance), n);

    // Initialize ranks uniformly.
    let init_rank = 1.0 / n as f64;
    let mut rank = vec![init_rank; n];
    let mut next_rank = vec![0.0f64; n];

    // Precompute out-degrees for all nodes. A node with out_degree 0 is a
    // dangling node — its rank mass is redistributed uniformly.
    let out_degrees: Vec<usize> = (0..n).map(|i| csr.out_degree(i as u32)).collect();

    let teleport = (1.0 - damping) / n as f64;

    for iter in 1..=max_iter {
        // Accumulate dangling node rank mass.
        let dangling_sum: f64 = (0..n)
            .filter(|&i| out_degrees[i] == 0)
            .map(|i| rank[i])
            .sum();

        let dangling_contrib = damping * dangling_sum / n as f64;

        // Base rank: teleport + dangling redistribution.
        for r in next_rank.iter_mut() {
            *r = teleport + dangling_contrib;
        }

        // Contribution from in-neighbors: for each node v, accumulate
        // rank[u] / out_degree[u] for each in-neighbor u.
        //
        // Iterate via outbound edges: for each node u with out_degree > 0,
        // distribute rank[u] / out_degree[u] to each outbound neighbor.
        // This is cache-friendlier than iterating inbound edges because the
        // CSR outbound arrays are contiguous.
        for u in 0..n {
            let deg = out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = damping * rank[u] / deg as f64;
            for (_lid, dst) in csr.iter_out_edges(u as u32) {
                next_rank[dst as usize] += contrib;
            }
        }

        // Compute L1 norm of rank delta for convergence check.
        let delta: f64 = rank
            .iter()
            .zip(next_rank.iter())
            .map(|(old, new)| (old - new).abs())
            .sum();

        // Swap rank vectors (avoids allocation).
        std::mem::swap(&mut rank, &mut next_rank);

        reporter.report_iteration(iter, Some(delta));

        if delta < tolerance {
            break;
        }
    }

    reporter.finish();

    // Build result batch sorted by rank descending.
    let mut indexed: Vec<(usize, f64)> = rank.into_iter().enumerate().collect();
    indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
    for (node_id, r) in indexed {
        batch.push_node_f64(csr.node_name(node_id as u32).to_string(), r);
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triangle_csr() -> CsrIndex {
        // a -> b -> c -> a (cycle)
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        csr.compact();
        csr
    }

    #[test]
    fn pagerank_uniform_cycle() {
        let csr = triangle_csr();
        let params = AlgoParams::default();
        let batch = run(&csr, &params);

        // Symmetric cycle → all ranks equal ≈ 1/3.
        assert_eq!(batch.len(), 3);
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        for row in &rows {
            let rank = row["rank"].as_f64().unwrap();
            assert!((rank - 1.0 / 3.0).abs() < 1e-6, "rank {rank} != 1/3");
        }
    }

    #[test]
    fn pagerank_star_topology() {
        // Hub a points to b, c, d. No one points to a.
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("a", "L", "d");
        csr.compact();

        let params = AlgoParams {
            max_iterations: Some(50),
            ..Default::default()
        };
        let batch = run(&csr, &params);

        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let ranks: std::collections::HashMap<&str, f64> = rows
            .iter()
            .map(|r| (r["node_id"].as_str().unwrap(), r["rank"].as_f64().unwrap()))
            .collect();

        // b, c, d should have higher rank than a (they receive links).
        // a is a dangling-target: no one links to a, but dangling redistribution
        // from b/c/d (which have zero out-degree) gives a some rank.
        assert!(
            ranks["b"] > ranks["a"],
            "b={} should > a={}",
            ranks["b"],
            ranks["a"]
        );
    }

    #[test]
    fn pagerank_empty_graph() {
        let csr = CsrIndex::new();
        let batch = run(&csr, &AlgoParams::default());
        assert!(batch.is_empty());
    }

    #[test]
    fn pagerank_single_node() {
        let mut csr = CsrIndex::new();
        csr.add_node("lonely");
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn pagerank_dangling_nodes() {
        // a -> b, c is dangling (no outbound edges).
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_node("c"); // dangling
        csr.compact();

        let batch = run(&csr, &AlgoParams::default());
        assert_eq!(batch.len(), 3);

        // All ranks should sum to ~1.0.
        let json = batch.to_json().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        let total: f64 = rows.iter().map(|r| r["rank"].as_f64().unwrap()).sum();
        assert!((total - 1.0).abs() < 1e-6, "total rank {total} != 1.0");
    }

    #[test]
    fn pagerank_converges() {
        let csr = triangle_csr();
        let params = AlgoParams {
            tolerance: Some(1e-10),
            max_iterations: Some(100),
            ..Default::default()
        };
        let batch = run(&csr, &params);
        // Should converge before 100 iterations on a 3-node cycle.
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn pagerank_to_record_batch() {
        let csr = triangle_csr();
        let batch = run(&csr, &AlgoParams::default());
        let rb = batch.to_record_batch().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.schema().field(0).name(), "node_id");
        assert_eq!(rb.schema().field(1).name(), "rank");
    }
}
