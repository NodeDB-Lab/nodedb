// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane handler for `GraphOp::WccSuperstep` — runs ONE distributed
//! Weakly Connected Components contraction round on this shard's local CSR
//! partition.
//!
//! Single-round primitive (NOT iterative, unlike BSP PageRank): each shard
//! computes connected components over its OWNED nodes only, then the
//! Control-Plane coordinator stitches every shard's result into one global
//! union-find over node names. All per-round state is carried in the
//! `GraphOp::WccSuperstep` plan variant and returned in [`WccSuperstepResult`].
//!
//! Ownership model (identical to BSP PageRank): each round builds a
//! collection-scoped CSR via `build_csr_for_collection` so distributed WCC runs
//! over exactly the same `(collection, edge_label)` subgraph as single-node
//! `GRAPH ALGO WCC ON <collection>`. Only nodes whose `VShardId::from_key(name)`
//! is in `owned_vshards` are "owned" by this shard. For each owned node `u` and
//! each out-edge `u -> v`: if `v` is owned, `union(u, v)` in the local
//! union-find; else record a boundary edge `(name(u), name(v))`. Each owned
//! node's LOCAL label is the lexicographically-minimum owned node NAME in its
//! local component. `VShardId::from_key` is a pure hash, so no routing table is
//! needed on the Data Plane.
//!
//! Every node reads the graph as of the round's read cut (the plan's
//! `system_as_of`), so a write during the round reaches no node's view.

use std::collections::HashMap;
use std::collections::HashSet;

use nodedb_graph::CsrIndex;
use tracing::debug;

use crate::types::VShardId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::WccSuperstepResult;

use super::graph_algo::build_csr_for_collection;

/// The pure WCC contraction-round core: given an already-built `CsrIndex` and the
/// owned-vShard set, builds the owned-node set, runs local union-find over
/// owned→owned edges, records owned→ghost boundary edges, and computes each
/// owned node's lexicographically-minimum-owned-name component label.
///
/// Both [`CoreLoop::execute_wcc_superstep`] (after calling
/// `build_csr_for_collection`) and the unit tests call this function, so the
/// tests exercise the real handler logic rather than a re-implementation.
pub(super) fn run_wcc_superstep_core(csr: &CsrIndex, owned_vshards: &[u32]) -> WccSuperstepResult {
    // Build a HashSet of owned vShards for O(1) membership checks in the
    // per-edge hot path.
    let owned_set: HashSet<u32> = owned_vshards.iter().copied().collect();
    let is_owned =
        |name: &str| -> bool { owned_set.contains(&VShardId::from_key(name.as_bytes()).as_u32()) };

    // Build the owned-node set: CSR raw u32 id → dense owned index, plus the
    // parallel name vector and reverse map (dense → raw). One pass.
    let node_count = csr.node_count();
    let mut raw_to_owned: HashMap<u32, u32> = HashMap::new();
    let mut node_names: Vec<String> = Vec::new();
    let mut owned_to_raw: Vec<u32> = Vec::new();
    for raw in 0..node_count as u32 {
        let name = csr.node_name_raw(raw);
        if is_owned(name) {
            let dense = node_names.len() as u32;
            raw_to_owned.insert(raw, dense);
            node_names.push(name.to_string());
            owned_to_raw.push(raw);
        }
    }
    let vertex_count = node_names.len();

    if vertex_count == 0 {
        return WccSuperstepResult::default();
    }

    // Local union-find over owned dense indices. Classify each owned node's
    // out-edges: owned destination → union; ghost destination → boundary edge.
    let mut uf = UnionFind::new(vertex_count);
    let mut boundary_edges: Vec<(String, String)> = Vec::new();
    for (owned_idx, &raw) in owned_to_raw.iter().enumerate() {
        for (_label, dst_raw) in csr.iter_out_edges_raw(raw) {
            match raw_to_owned.get(&dst_raw) {
                Some(&dst_owned) => uf.union(owned_idx, dst_owned as usize),
                None => {
                    // Ghost destination: owned -> non-owned. Record the boundary
                    // edge by NAME so the coordinator can stitch globally.
                    let ghost_name = csr.node_name_raw(dst_raw).to_string();
                    boundary_edges.push((node_names[owned_idx].clone(), ghost_name));
                }
            }
        }
    }

    // Per-component minimum owned node NAME (the local component root label).
    // First resolve every owned node's local root, then take the min name per root.
    let mut root_min: HashMap<usize, &str> = HashMap::new();
    let roots: Vec<usize> = (0..vertex_count).map(|i| uf.find(i)).collect();
    for (i, &root) in roots.iter().enumerate() {
        let name = node_names[i].as_str();
        root_min
            .entry(root)
            .and_modify(|m| {
                if name < *m {
                    *m = name;
                }
            })
            .or_insert(name);
    }

    let node_labels: Vec<(String, String)> = (0..vertex_count)
        .map(|i| {
            let root = roots[i];
            (node_names[i].clone(), root_min[&root].to_string())
        })
        .collect();

    WccSuperstepResult {
        node_labels,
        boundary_edges,
        vertex_count,
        // The fan that dispatched the round stamps the cut it read at.
        system_as_of: None,
    }
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_wcc_superstep(
        &self,
        task: &ExecutionTask,
        tid: u64,
        params: &nodedb_graph::AlgoParams,
        owned_vshards: &[u32],
        system_as_of: Option<i64>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            collection = %params.collection,
            "wcc superstep dispatch"
        );

        // Fails this one core's round, so a test can check that the all-core
        // gather fails the query instead of merging the other cores.
        #[cfg(feature = "failpoints")]
        if let Some(detail) = crate::fail_point::eval_fail(
            self.fail_scope,
            &format!("graph::wcc_superstep::core{}", self.core_id),
        ) {
            return self.response_error(task, ErrorCode::Internal { detail });
        }

        // Every node of the round reads the graph at the round's read cut,
        // so every node reads the same graph.
        let Some(system_as_of) = system_as_of else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "wcc superstep: the plan carries no read cut".into(),
                },
            );
        };

        let database_id = task.request.database_id.as_u64();
        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            task.request.database_id,
            crate::types::TenantId::new(tid),
            nodedb_mem::EngineId::Graph,
        );

        // Build a collection-scoped CSR from the edge store as of the cut —
        // same call as execute_graph_algo — so distributed WCC runs over
        // exactly the same (collection, edge_label) subgraph as single-node
        // GRAPH ALGO WCC ON <collection>.
        let csr = match build_csr_for_collection(
            &self.edge_store,
            database_id,
            tid,
            &params.collection,
            params.edge_label.as_deref(),
            Some(system_as_of),
            memory,
        ) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        if csr.node_count() == 0 {
            return self.encode_wcc_result(task, WccSuperstepResult::default());
        }

        let result = run_wcc_superstep_core(&csr, owned_vshards);
        self.encode_wcc_result(task, result)
    }

    /// Serialize a `WccSuperstepResult` into a response payload (zerompk).
    fn encode_wcc_result(&self, task: &ExecutionTask, result: WccSuperstepResult) -> Response {
        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("wcc superstep result encode: {e}"),
                },
            ),
        }
    }
}

/// Disjoint-set (Union-Find) with path halving and union-by-rank over dense
/// owned indices. Amortized near-O(1) per find/union.
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

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
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

    /// Build a small CSR with a chain a->b->c and a separate z node/edge z->w.
    fn two_component_csr() -> CsrIndex {
        let memory = nodedb_mem::ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            nodedb_types::DatabaseId::DEFAULT,
            crate::types::TenantId::new(0),
            nodedb_mem::EngineId::Graph,
        );
        let mut csr = CsrIndex::new(memory);
        csr.add_edge("a", "e", "b").unwrap();
        csr.add_edge("b", "e", "c").unwrap();
        csr.add_edge("z", "e", "w").unwrap();
        csr.compact().unwrap();
        csr
    }

    #[test]
    fn all_owned_single_chain_one_local_root() {
        let csr = two_component_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        let res = run_wcc_superstep_core(&csr, &owned);

        assert_eq!(res.vertex_count, 5);
        assert!(res.boundary_edges.is_empty(), "no ghosts when all owned");

        let labels: HashMap<&str, &str> = res
            .node_labels
            .iter()
            .map(|(n, r)| (n.as_str(), r.as_str()))
            .collect();
        // Chain a-b-c shares the min name "a" as its local root.
        assert_eq!(labels["a"], "a");
        assert_eq!(labels["b"], "a");
        assert_eq!(labels["c"], "a");
        // z-w component shares min name "w" (w < z).
        assert_eq!(labels["z"], "w");
        assert_eq!(labels["w"], "w");
    }

    #[test]
    fn ghost_destination_recorded_as_boundary_edge() {
        let csr = two_component_csr();
        // Exclude c's vShard → edge b->c becomes a ghost edge, c is not owned.
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let res = run_wcc_superstep_core(&csr, &owned);

        // c excluded from owned set.
        let names: HashSet<&str> = res.node_labels.iter().map(|(n, _)| n.as_str()).collect();
        assert!(!names.contains("c"));

        // b->c is the only ghost edge from an owned node.
        assert_eq!(res.boundary_edges.len(), 1);
        assert_eq!(res.boundary_edges[0], ("b".to_string(), "c".to_string()));
    }

    #[test]
    fn no_owned_nodes_is_empty() {
        let csr = two_component_csr();
        // Own no vShards → no owned nodes.
        let res = run_wcc_superstep_core(&csr, &[]);
        assert_eq!(res.vertex_count, 0);
        assert!(res.node_labels.is_empty());
        assert!(res.boundary_edges.is_empty());
    }
}
