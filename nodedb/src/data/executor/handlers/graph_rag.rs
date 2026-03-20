//! GraphRAG fusion handler: vector search, graph expansion, RRF scoring.
//!
//! Pipeline:
//! 1. Vector engine returns top-K semantically similar nodes.
//! 2. Result node IDs feed into graph traversal as start nodes.
//! 3. Graph-expanded result set is scored by hop distance.
//! 4. RRF fuses vector_score and graph_score into unified ranking.
//! 5. Final top-N results are materialized.
//!
//! BFS expansion is bounded by a per-query memory budget derived from the
//! node count. If the budget is exceeded, expansion stops early and results
//! are marked as truncated.

use std::collections::{HashMap, HashSet, VecDeque};

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::engine::graph::edge_store::Direction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Maximum bytes a single GraphRAG BFS expansion may allocate for
/// intermediate node tracking.
const BFS_MEMORY_BUDGET_BYTES: usize = 256 * 1024;

/// Estimated per-node memory cost in the BFS working set.
const BFS_BYTES_PER_NODE: usize = 192;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_rag_fusion(
        &self,
        task: &ExecutionTask,
        tenant_id: u32,
        collection: &str,
        query_vector: &[f32],
        vector_top_k: usize,
        edge_label: &Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        rrf_k: (f64, f64),
        max_visited: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            "graph rag fusion"
        );

        let index_key = CoreLoop::vector_index_key(tenant_id, collection, "");
        let Some(index) = self.vector_indexes.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if index.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        let ef = vector_top_k.saturating_mul(4).max(64);
        let vector_results = index.search(query_vector, vector_top_k, ef);

        if vector_results.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        let mut vector_scores: HashMap<String, (usize, f32)> = HashMap::new();
        for (rank, result) in vector_results.iter().enumerate() {
            let node_id = result.id.to_string();
            vector_scores.insert(node_id, (rank, result.distance));
        }

        let start_ids: Vec<&str> = vector_scores.keys().map(String::as_str).collect();
        let (expanded_nodes, hop_distances, bfs_truncated) = self.bfs_with_distances(
            &start_ids,
            edge_label.as_deref(),
            direction,
            expansion_depth,
            max_visited,
        );

        let (vector_k, graph_k) = rrf_k;

        let mut graph_ranked: Vec<(&str, usize)> = expanded_nodes
            .iter()
            .map(|node| {
                let dist = hop_distances
                    .get(node.as_str())
                    .copied()
                    .unwrap_or(usize::MAX);
                (node.as_str(), dist)
            })
            .collect();
        graph_ranked.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));

        let mut rrf_scores: HashMap<&str, f64> = HashMap::new();

        for (node_id, (rank, _)) in &vector_scores {
            let contribution = 1.0 / (vector_k + *rank as f64 + 1.0);
            *rrf_scores.entry(node_id.as_str()).or_default() += contribution;
        }

        for (graph_rank, (node_id, _)) in graph_ranked.iter().enumerate() {
            let contribution = 1.0 / (graph_k + graph_rank as f64 + 1.0);
            *rrf_scores.entry(node_id).or_default() += contribution;
        }

        let mut fused: Vec<_> = rrf_scores.into_iter().collect();
        fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        fused.truncate(final_top_k);

        use super::super::response_codec::*;

        let results: Vec<GraphRagResult> = fused
            .iter()
            .map(|(node_id, rrf_score)| {
                let (vector_rank, vector_distance) = vector_scores
                    .get(*node_id)
                    .map(|(rank, dist)| (Some(*rank), Some(*dist)))
                    .unwrap_or((None, None));
                let hop_distance = hop_distances.get(*node_id).copied();

                GraphRagResult {
                    node_id: node_id.to_string(),
                    rrf_score: *rrf_score,
                    vector_rank,
                    vector_distance,
                    hop_distance,
                }
            })
            .collect();

        let response_body = GraphRagResponse {
            results,
            metadata: GraphRagMetadata {
                vector_candidates: vector_results.len(),
                graph_expanded: expanded_nodes.len(),
                truncated: bfs_truncated,
                watermark_lsn: self.watermark.as_u64(),
            },
        };

        match encode(&response_body) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph rag fusion serialization failed");
                self.response_error(task, ErrorCode::Internal { detail: e })
            }
        }
    }

    /// BFS traversal that also tracks hop distances from start nodes.
    fn bfs_with_distances(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> (Vec<String>, HashMap<String, usize>, bool) {
        let budget_node_limit = BFS_MEMORY_BUDGET_BYTES / BFS_BYTES_PER_NODE;
        let effective_limit = max_visited.min(budget_node_limit);

        let mut visited: HashSet<String> = HashSet::with_capacity(effective_limit.min(1024));
        let mut distances: HashMap<String, usize> =
            HashMap::with_capacity(effective_limit.min(1024));
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        let mut truncated = false;

        for &node in start_nodes {
            let owned = node.to_string();
            if visited.insert(owned.clone()) {
                distances.insert(owned.clone(), 0);
                queue.push_back((owned, 0));
            }
        }

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            if visited.len() >= effective_limit {
                truncated = true;
                break;
            }

            let neighbors = self.csr.neighbors(&node, label_filter, direction);
            for (_, neighbor) in &neighbors {
                if visited.len() >= effective_limit {
                    truncated = true;
                    break;
                }
                if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    distances.insert(neighbor.clone(), depth + 1);
                    queue.push_back((neighbor.clone(), depth + 1));
                }
            }

            if truncated {
                break;
            }
        }

        if truncated {
            warn!(
                core = self.core_id,
                visited = visited.len(),
                limit = effective_limit,
                budget_limit = budget_node_limit,
                max_visited,
                "GraphRAG BFS truncated: memory budget or max_visited reached"
            );
        }

        let nodes: Vec<String> = visited.into_iter().collect();
        (nodes, distances, truncated)
    }
}
