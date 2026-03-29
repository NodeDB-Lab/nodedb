//! Edge table statistics for query planning and optimization.
//!
//! Maintains per-label edge counts and degree distribution histograms.
//! Updated incrementally on `add_edge`/`remove_edge` and fully recomputed
//! on `compact`. Used by the MATCH pattern compiler for join order
//! optimization (most selective label first).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::CsrIndex;

/// Per-label edge statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelStats {
    /// Number of edges with this label.
    pub edge_count: usize,
    /// Number of distinct source nodes that have this label on an outbound edge.
    pub distinct_sources: usize,
    /// Number of distinct destination nodes that have this label on an inbound edge.
    pub distinct_targets: usize,
}

/// Degree distribution histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegreeHistogram {
    pub min: usize,
    pub max: usize,
    pub avg: f64,
    pub p50: usize,
    pub p95: usize,
    pub p99: usize,
}

/// Complete graph statistics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStatistics {
    /// Total number of nodes.
    pub node_count: usize,
    /// Total number of edges (all labels).
    pub edge_count: usize,
    /// Number of distinct edge labels.
    pub label_count: usize,
    /// Per-label statistics.
    pub label_stats: HashMap<String, LabelStats>,
    /// Out-degree distribution across all nodes.
    pub out_degree_histogram: DegreeHistogram,
    /// In-degree distribution across all nodes.
    pub in_degree_histogram: DegreeHistogram,
}

impl CsrIndex {
    /// Compute full graph statistics from the current CSR state.
    ///
    /// O(V + E) — iterates all nodes and edges once. Intended for query
    /// planning, not hot-path execution.
    pub fn compute_statistics(&self) -> GraphStatistics {
        let n = self.node_count();
        if n == 0 {
            return GraphStatistics {
                node_count: 0,
                edge_count: 0,
                label_count: 0,
                label_stats: HashMap::new(),
                out_degree_histogram: DegreeHistogram {
                    min: 0,
                    max: 0,
                    avg: 0.0,
                    p50: 0,
                    p95: 0,
                    p99: 0,
                },
                in_degree_histogram: DegreeHistogram {
                    min: 0,
                    max: 0,
                    avg: 0.0,
                    p50: 0,
                    p95: 0,
                    p99: 0,
                },
            };
        }

        // Per-label counters.
        let mut label_edge_count: HashMap<u16, usize> = HashMap::new();
        let mut label_sources: HashMap<u16, std::collections::HashSet<u32>> = HashMap::new();
        let mut label_targets: HashMap<u16, std::collections::HashSet<u32>> = HashMap::new();

        // Degree arrays.
        let mut out_degrees: Vec<usize> = Vec::with_capacity(n);
        let mut in_degrees: Vec<usize> = Vec::with_capacity(n);

        let mut total_edges = 0usize;

        for node in 0..n {
            let node_id = node as u32;
            let mut out_deg = 0usize;
            let mut in_deg = 0usize;

            for (lid, dst) in self.iter_out_edges(node_id) {
                out_deg += 1;
                total_edges += 1;
                *label_edge_count.entry(lid).or_insert(0) += 1;
                label_sources.entry(lid).or_default().insert(node_id);
                label_targets.entry(lid).or_default().insert(dst);
            }

            for (_lid, _src) in self.iter_in_edges(node_id) {
                in_deg += 1;
            }

            out_degrees.push(out_deg);
            in_degrees.push(in_deg);
        }

        // Build per-label stats.
        let mut label_stats = HashMap::new();
        for (&lid, &count) in &label_edge_count {
            let label_name = self.label_name(lid).to_string();
            label_stats.insert(
                label_name,
                LabelStats {
                    edge_count: count,
                    distinct_sources: label_sources.get(&lid).map_or(0, |s| s.len()),
                    distinct_targets: label_targets.get(&lid).map_or(0, |s| s.len()),
                },
            );
        }

        GraphStatistics {
            node_count: n,
            edge_count: total_edges,
            label_count: label_edge_count.len(),
            label_stats,
            out_degree_histogram: compute_histogram(&out_degrees),
            in_degree_histogram: compute_histogram(&in_degrees),
        }
    }

    /// Get the edge count for a specific label. O(E) unless cached.
    ///
    /// Returns 0 if the label doesn't exist.
    pub fn label_edge_count(&self, label: &str) -> usize {
        let Some(lid) = self.label_id(label) else {
            return 0;
        };

        let n = self.node_count();
        let mut count = 0usize;
        for node in 0..n {
            for (l, _dst) in self.iter_out_edges(node as u32) {
                if l == lid {
                    count += 1;
                }
            }
        }
        count
    }

    /// Estimate the selectivity of a label: edge_count / total_edges.
    ///
    /// Returns 1.0 for unknown labels (conservative — assume all edges).
    /// Returns 0.0 for graphs with no edges.
    pub fn label_selectivity(&self, label: &str) -> f64 {
        let total = self.edge_count();
        if total == 0 {
            return 0.0;
        }
        let count = self.label_edge_count(label);
        if count == 0 {
            return 1.0; // Unknown label → conservative estimate.
        }
        count as f64 / total as f64
    }
}

/// Compute degree distribution histogram from a degree array.
fn compute_histogram(degrees: &[usize]) -> DegreeHistogram {
    if degrees.is_empty() {
        return DegreeHistogram {
            min: 0,
            max: 0,
            avg: 0.0,
            p50: 0,
            p95: 0,
            p99: 0,
        };
    }

    let mut sorted = degrees.to_vec();
    sorted.sort_unstable();

    let n = sorted.len();
    let sum: usize = sorted.iter().sum();

    DegreeHistogram {
        min: sorted[0],
        max: sorted[n - 1],
        avg: sum as f64 / n as f64,
        p50: sorted[n / 2],
        p95: sorted[(n as f64 * 0.95) as usize],
        p99: sorted[((n as f64 * 0.99) as usize).min(n - 1)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_empty_graph() {
        let csr = CsrIndex::new();
        let stats = csr.compute_statistics();
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.edge_count, 0);
        assert_eq!(stats.label_count, 0);
    }

    #[test]
    fn statistics_basic() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.add_edge("a", "LIKES", "c");
        csr.compact();

        let stats = csr.compute_statistics();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 3);
        assert_eq!(stats.label_count, 2);

        let knows = &stats.label_stats["KNOWS"];
        assert_eq!(knows.edge_count, 2);
        assert_eq!(knows.distinct_sources, 2);
        assert_eq!(knows.distinct_targets, 2);

        let likes = &stats.label_stats["LIKES"];
        assert_eq!(likes.edge_count, 1);
    }

    #[test]
    fn degree_histogram_values() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("a", "L", "d");
        csr.add_edge("b", "L", "c");
        csr.compact();

        let stats = csr.compute_statistics();
        assert_eq!(stats.out_degree_histogram.min, 0);
        assert_eq!(stats.out_degree_histogram.max, 3);
        assert!(stats.out_degree_histogram.avg > 0.0);
    }

    #[test]
    fn label_edge_count_direct() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.add_edge("a", "LIKES", "c");
        csr.compact();

        assert_eq!(csr.label_edge_count("KNOWS"), 2);
        assert_eq!(csr.label_edge_count("LIKES"), 1);
        assert_eq!(csr.label_edge_count("NONEXISTENT"), 0);
    }

    #[test]
    fn label_selectivity_values() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.add_edge("a", "LIKES", "c");
        csr.compact();

        let sel_knows = csr.label_selectivity("KNOWS");
        let sel_likes = csr.label_selectivity("LIKES");

        assert!((sel_knows - 2.0 / 3.0).abs() < 1e-9);
        assert!((sel_likes - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(csr.label_selectivity("NONEXISTENT"), 1.0);
    }

    #[test]
    fn statistics_serde_roundtrip() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.compact();

        let stats = csr.compute_statistics();
        let json = serde_json::to_string(&stats).unwrap();
        let parsed: GraphStatistics = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.node_count, stats.node_count);
        assert_eq!(parsed.edge_count, stats.edge_count);
    }

    #[test]
    fn statistics_with_buffer_edges() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        // Don't compact — edges in buffer.
        let stats = csr.compute_statistics();
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.label_stats["KNOWS"].edge_count, 1);
    }
}
