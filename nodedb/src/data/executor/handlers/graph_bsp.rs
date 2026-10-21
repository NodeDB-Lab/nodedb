// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane handler for `GraphOp::BspSuperstep` — runs ONE distributed
//! PageRank BSP superstep on this shard's local CSR partition.
//!
//! The handler is stateless across supersteps. All per-superstep state rides
//! in the `GraphOp::BspSuperstep` plan (the current rank as `rank_seed`, the
//! contributions routed to this shard's owned nodes) and comes back in
//! [`BspSuperstepResult`]. The Control-Plane coordinator
//! (`graph_dispatch::bsp_pagerank`) owns the superstep loop, the convergence
//! check, and contribution routing.
//!
//! Each superstep is one power iteration of the whole graph:
//!
//! - Superstep 0 sets the initial rank and scatters it.
//! - Every later superstep computes the next rank from the current rank and
//!   the contributions every other shard scattered from that same rank, then
//!   scatters the next rank.
//!
//! A contribution therefore lands in the iteration it belongs to, and no rank
//! mass is in flight when the run halts.
//!
//! Ownership model: each superstep builds a collection-scoped CSR via
//! `CoreLoop::algo_csr` (the same read `execute_graph_algo` uses), so
//! distributed PageRank runs over exactly the `(collection, edge_label)`
//! subgraph single-node `GRAPH ALGO ON <collection>` does. A node whose
//! `VShardId::from_key(name)` is in `owned_vshards` is owned by this shard
//! and carries a rank. Both endpoint homes store an edge, so an owned node's
//! out-edges and its in-edges from other shards are all in this CSR. An edge
//! to a non-owned destination is a ghost edge: its contribution goes out in
//! `outbound`, tagged with the destination's vShard.
//!
//! Count-only sentinel: `global_n == 0` means the coordinator is running its
//! pre-superstep count phase. `run_bsp_superstep_core` then returns only
//! `vertex_count`, `node_names` and `seed_hits`, and runs no superstep.

use std::collections::{HashMap, HashSet};

use nodedb_cluster::distributed_graph::{PageRankUpdate, ShardPageRankState};
use nodedb_graph::{AlgoParams, CsrIndex, GraphAlgorithm};
use tracing::debug;

use crate::types::VShardId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::BspSuperstepResult;

/// Borrowed arguments for [`CoreLoop::execute_bsp_superstep`], destructured
/// from the `GraphOp::BspSuperstep` plan variant by the dispatcher.
pub struct BspSuperstepArgs<'a> {
    pub algorithm: &'a GraphAlgorithm,
    pub params: &'a AlgoParams,
    pub superstep: u32,
    pub global_n: usize,
    pub owned_vshards: &'a [u32],
    pub incoming_contributions: &'a [(String, f64)],
    pub rank_seed: &'a [(String, f64)],
    /// The dangling mass of the rank in `rank_seed` over the whole graph
    /// (see `BspSuperstepPlan::global_dangling`). `0.0` on the count phase
    /// and superstep 0.
    pub global_dangling: f64,
    /// Coordinator-computed GLOBAL `Σ max(w, 0.0)` over the Personalized-PageRank
    /// seed map (see `BspSuperstepPlan::personalization_sum`). `0.0` means standard
    /// uniform PageRank; `> 0.0` activates Personalized PageRank — each owned node's
    /// globally-normalized seed share is `max(seed[name], 0.0) / personalization_sum`.
    pub personalization_sum: f64,
    /// The run's read cut: the system-time ordinal every superstep reads the
    /// graph at (see `BspSuperstepPlan::read_cut_marker`). A superstep
    /// without one is refused.
    pub system_as_of: Option<i64>,
    /// The valid-time instant every edge the run reads must contain, or
    /// `None` for every valid time.
    pub valid_at_ms: Option<i64>,
}

/// The pure BSP-superstep core: given an already-built `CsrIndex` and the
/// per-superstep arguments, builds the owned-node set, runs one superstep,
/// and returns the complete [`BspSuperstepResult`].
///
/// Both [`CoreLoop::execute_bsp_superstep`] (after calling
/// `CoreLoop::algo_csr`) and the unit tests call this function, so the
/// tests exercise the real handler math rather than a re-implementation.
///
/// A superstep that cannot be one step of the whole graph is an error:
/// superstep 0 with a routed contribution, an owned node with no current
/// rank, or a contribution to a node this shard does not own.
pub(super) fn run_bsp_superstep_core(
    csr: &CsrIndex,
    args: &BspSuperstepArgs<'_>,
) -> Result<BspSuperstepResult, crate::Error> {
    // Build a HashSet of owned vShards for O(1) membership checks in the
    // per-edge hot path (avoids O(n) slice scan per edge).
    let owned_set: HashSet<u32> = args.owned_vshards.iter().copied().collect();
    let is_owned =
        |name: &str| -> bool { owned_set.contains(&VShardId::from_key(name.as_bytes()).as_u32()) };

    // Build the owned-node set: CSR raw u32 id → dense owned index, plus the
    // parallel name vector. `rank_vec`/`node_names` index by dense owned id.
    let node_count = csr.node_count();
    let mut raw_to_owned: HashMap<u32, u32> = HashMap::new();
    let mut node_names: Vec<String> = Vec::new();
    // Reverse map: dense owned index → CSR raw id (for edge iteration).
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

    // `global_n == 0` is the COUNT-ONLY sentinel: `global_n` is not known yet,
    // so no superstep runs. It returns the owned count and names, and the
    // owned nodes that are positively-weighted seed keys, so the coordinator
    // can decide whether personalization is active (mirroring single-node
    // `build_personalization` returning `None` for unknown seeds).
    if args.global_n == 0 {
        let seed_hits = match args.params.personalization_vector() {
            Some(seed) => node_names
                .iter()
                .filter(|name| seed.get(name.as_str()).copied().unwrap_or(0.0) > 0.0)
                .count(),
            None => 0,
        };
        return Ok(BspSuperstepResult {
            local_delta: 0.0,
            outbound: Vec::new(),
            rank_vec: Vec::new(),
            vertex_count,
            node_names,
            dangling_sum: 0.0,
            seed_hits,
            system_as_of: args.system_as_of,
        });
    }

    // Out-degree per owned node, counted over ALL out-edges (owned + ghost)
    // so dangling classification and contribution division match the
    // single-node PageRank semantics (a node with only ghost edges is NOT
    // dangling).
    let mut out_degrees: Vec<usize> = vec![0; vertex_count];
    for (raw, &owned) in &raw_to_owned {
        out_degrees[owned as usize] = csr.out_degree_raw(*raw);
    }

    // Dense owned index → out-edges as (dst_name, is_ghost, target_vshard).
    // Ghost = destination not owned by this shard.
    let csr_out_edges = |owned_idx: u32| -> Vec<(String, bool, u16)> {
        let raw = owned_to_raw[owned_idx as usize];
        csr.iter_out_edges_raw(raw)
            .map(|(_label, dst_raw)| {
                let dst_name = csr.node_name_raw(dst_raw).to_string();
                let dst_vs = VShardId::from_key(dst_name.as_bytes()).as_u32();
                let ghost = !owned_set.contains(&dst_vs);
                (dst_name, ghost, dst_vs as u16)
            })
            .collect()
    };

    let mut state =
        ShardPageRankState::init(vertex_count, out_degrees, |_name| None, &csr_out_edges);

    // Personalized-PageRank seed share for THIS shard's owned nodes, GLOBALLY
    // normalized by the coordinator-computed cluster-wide sum so `Σ_global p_i ==
    // 1.0`. `personalization_sum > 0.0` activates PPR; `None` (== 0.0) recovers
    // standard uniform PageRank. `p[i] = max(seed[name_i], 0.0) / personalization_sum`,
    // positionally aligned with `state.rank` / `node_names`.
    let personalization: Option<Vec<f64>> = if args.personalization_sum > 0.0 {
        let seed = args.params.personalization_vector();
        let p: Vec<f64> = node_names
            .iter()
            .map(|name| {
                let w = seed
                    .and_then(|m| m.get(name.as_str()).copied())
                    .unwrap_or(0.0)
                    .max(0.0);
                w / args.personalization_sum
            })
            .collect();
        Some(p)
    } else {
        None
    };

    let damping = args.params.damping_factor();
    let local_delta = if args.superstep == 0 {
        // The initial rank is the seed share under personalization (as
        // single-node `rank[i] = p[i]`), else `1/global_n`. Nothing has been
        // scattered yet, so nothing can be routed here.
        if let Some((vertex, _)) = args.incoming_contributions.first() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "bsp pagerank: superstep 0 received a contribution to '{vertex}' \
                     before any rank was scattered"
                ),
            });
        }
        match &personalization {
            Some(p) => state.rank.copy_from_slice(p),
            None => state.rank.fill(1.0 / args.global_n as f64),
        }
        0.0
    } else {
        let seed: HashMap<&str, f64> = args
            .rank_seed
            .iter()
            .map(|(name, rank)| (name.as_str(), *rank))
            .collect();
        for (slot, name) in state.rank.iter_mut().zip(&node_names) {
            let Some(&rank) = seed.get(name.as_str()) else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "bsp pagerank: owned node '{name}' has no rank from superstep {}; \
                         the graph changed during the run",
                        args.superstep - 1
                    ),
                });
            };
            *slot = rank;
        }
        for (dst_name, value) in args.incoming_contributions {
            state.add_remote_contribution(dst_name.clone(), *value);
        }

        // Dense owned index → owned destination dense indices. Ghost
        // destinations are scattered through `outbound` instead.
        let local_edge_iter = |owned_idx: u32| -> Vec<u32> {
            let raw = owned_to_raw[owned_idx as usize];
            csr.iter_out_edges_raw(raw)
                .filter_map(|(_label, dst_raw)| raw_to_owned.get(&dst_raw).copied())
                .collect()
        };
        let node_id_to_local = |name: &str| -> Option<u32> {
            csr.node_id_raw(name)
                .and_then(|raw| raw_to_owned.get(&raw).copied())
        };
        state
            .update(PageRankUpdate {
                damping,
                global_n: args.global_n,
                global_dangling_sum: args.global_dangling,
                personalization: personalization.as_deref(),
                local_edge_iter: &local_edge_iter,
                node_id_to_local: &node_id_to_local,
            })
            .map_err(|e| crate::Error::Internal {
                detail: format!("bsp pagerank: {e}"),
            })?
    };

    let (dangling_sum, outbound_map) = state.scatter(damping);
    let mut outbound: Vec<(u32, String, f64)> = Vec::new();
    for (target_shard, contribs) in outbound_map {
        for (dst_name, contrib) in contribs {
            outbound.push((target_shard as u32, dst_name, contrib));
        }
    }

    Ok(BspSuperstepResult {
        local_delta,
        outbound,
        rank_vec: state.rank,
        vertex_count,
        node_names,
        dangling_sum,
        // Only the count phase reports seed hits.
        seed_hits: 0,
        system_as_of: args.system_as_of,
    })
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_bsp_superstep(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: BspSuperstepArgs<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = args.algorithm.name(),
            collection = %args.params.collection,
            superstep = args.superstep,
            global_n = args.global_n,
            "bsp superstep dispatch"
        );

        // Only PageRank has a BSP form. Other algorithms answer `Unsupported`.
        if *args.algorithm != GraphAlgorithm::PageRank {
            return self.response_error(
                task,
                ErrorCode::Unsupported {
                    detail: format!(
                        "distributed BSP superstep is only implemented for PageRank, got {}",
                        args.algorithm.name()
                    ),
                },
            );
        }

        // Every superstep of a run reads the graph at the run's read cut, so
        // a write during the run changes no superstep's graph.
        let Some(system_as_of) = args.system_as_of else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "bsp superstep: the plan carries no read cut".into(),
                },
            );
        };

        // The collection-scoped CSR at the cut — the same read as
        // execute_graph_algo — so distributed PageRank runs over exactly the
        // same (collection, edge_label) subgraph as single-node
        // GRAPH ALGO ON <collection>. Every superstep of a run reads the one
        // CSR the historical cache holds for the cut.
        let scope = nodedb_physical::physical_plan::GraphReadScope::at_installed_cut(
            system_as_of,
            args.valid_at_ms,
        );
        let csr = match self.algo_csr(task.request.database_id, tid, args.params, &scope) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        // An empty partition still runs the core: a contribution routed to it
        // is an error there, never dropped.
        match run_bsp_superstep_core(&csr, &args) {
            Ok(result) => self.encode_result(task, result),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Serialize a `BspSuperstepResult` into a response payload (zerompk).
    fn encode_result(&self, task: &ExecutionTask, result: BspSuperstepResult) -> Response {
        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("bsp superstep result encode: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small CSR with a known triangle topology: a→b, b→c, c→a.
    fn triangle_csr() -> CsrIndex {
        let memory = nodedb_mem::ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            nodedb_types::DatabaseId::DEFAULT,
            crate::types::TenantId::new(0),
            nodedb_mem::EngineId::Graph,
        );
        let mut csr = CsrIndex::new(memory);
        for n in ["a", "b", "c"] {
            csr.add_node(n).unwrap();
        }
        csr.add_edge("a", "e", "b").unwrap();
        csr.add_edge("b", "e", "c").unwrap();
        csr.add_edge("c", "e", "a").unwrap();
        csr.compact().unwrap();
        csr
    }

    /// Minimal [`AlgoParams`] carrying only the fields `run_bsp_superstep_core` reads.
    fn dummy_params(damping: f64) -> AlgoParams {
        AlgoParams {
            collection: "test_coll".into(),
            damping: Some(damping),
            ..AlgoParams::default()
        }
    }

    fn args<'a>(
        params: &'a AlgoParams,
        owned: &'a [u32],
        superstep: u32,
        global_n: usize,
    ) -> BspSuperstepArgs<'a> {
        BspSuperstepArgs {
            algorithm: &GraphAlgorithm::PageRank,
            params,
            superstep,
            global_n,
            owned_vshards: owned,
            incoming_contributions: &[],
            rank_seed: &[],
            global_dangling: 0.0,
            personalization_sum: 0.0,
            system_as_of: Some(i64::MAX),
            valid_at_ms: None,
        }
    }

    #[test]
    fn all_owned_no_ghosts_matches_single_node_superstep() {
        let csr = triangle_csr();
        // Own every vShard → no node is a ghost.
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        let params = dummy_params(0.85);
        let seed: Vec<(String, f64)> = [("a", 0.5), ("b", 0.25), ("c", 0.25)]
            .map(|(n, r)| (n.to_string(), r))
            .to_vec();
        let mut step = args(&params, &owned, 1, 3);
        step.rank_seed = &seed;

        let res = run_bsp_superstep_core(&csr, &step).unwrap();

        // No ghost edges → nothing escapes the shard.
        assert!(res.outbound.is_empty(), "no edge should be cross-shard");
        assert_eq!(res.vertex_count, 3);
        assert_eq!(res.rank_vec.len(), 3);
        let sum: f64 = res.rank_vec.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "rank mass conserved, got {sum}");

        // One power iteration of the ring: each node gets the base plus its
        // predecessor's damped rank.
        let rank_of = |name: &str| {
            let i = res.node_names.iter().position(|n| n == name).unwrap();
            res.rank_vec[i]
        };
        let base = 0.15 / 3.0;
        assert!((rank_of("b") - (base + 0.85 * 0.5)).abs() < 1e-12);
        assert!((rank_of("a") - (base + 0.85 * 0.25)).abs() < 1e-12);
    }

    #[test]
    fn global_n_zero_is_count_only_no_superstep() {
        let csr = triangle_csr();
        // Exclude c's vShard so only a and b are owned — proves the count-only
        // path reports the OWNED vertex count (not the whole CSR) and still
        // emits zero rank/outbound.
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let params = dummy_params(0.85);
        let res = run_bsp_superstep_core(&csr, &args(&params, &owned, 0, 0)).unwrap();

        assert_eq!(res.vertex_count, 2, "owned node count (a, b) reported");
        assert_eq!(res.node_names, vec!["a".to_string(), "b".to_string()]);
        assert!(res.rank_vec.is_empty(), "no ranks computed in count phase");
        assert!(res.outbound.is_empty(), "no contributions in count phase");
        assert_eq!(res.local_delta, 0.0, "no convergence delta in count phase");
    }

    /// Superstep 0 sets the initial rank and scatters it. Its ghost edge
    /// carries the initial rank's damped share.
    #[test]
    fn superstep_zero_scatters_the_initial_rank() {
        let csr = triangle_csr();
        // Exclude c's vShard → edge b→c is a ghost edge and c is not owned.
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let params = dummy_params(0.85);
        let res = run_bsp_superstep_core(&csr, &args(&params, &owned, 0, 3)).unwrap();

        assert_eq!(res.node_names, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(res.rank_vec, vec![1.0 / 3.0, 1.0 / 3.0], "initial rank");
        assert_eq!(res.local_delta, 0.0, "superstep 0 changes no rank");
        assert_eq!(res.outbound.len(), 1, "exactly one cross-shard edge");
        let (target_vs, dst_name, contrib) = &res.outbound[0];
        assert_eq!(*target_vs, c_vs, "outbound tagged with destination vShard");
        assert_eq!(dst_name, "c");
        assert!((contrib - 0.85 / 3.0).abs() < 1e-12);
    }

    /// A contribution routed to a node this shard does not own is an error.
    #[test]
    fn an_unowned_contribution_is_refused() {
        let csr = triangle_csr();
        let c_vs = VShardId::from_key(b"c").as_u32();
        let owned: Vec<u32> = (0..VShardId::COUNT).filter(|&v| v != c_vs).collect();
        let params = dummy_params(0.85);
        let seed: Vec<(String, f64)> = vec![("a".into(), 0.5), ("b".into(), 0.5)];
        let incoming: Vec<(String, f64)> = vec![("c".into(), 0.1)];
        let mut step = args(&params, &owned, 1, 3);
        step.rank_seed = &seed;
        step.incoming_contributions = &incoming;
        assert!(run_bsp_superstep_core(&csr, &step).is_err());
    }

    /// An owned node with no current rank is an error, never a fresh rank.
    #[test]
    fn an_owned_node_without_a_rank_is_refused() {
        let csr = triangle_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        let params = dummy_params(0.85);
        let seed: Vec<(String, f64)> = vec![("a".into(), 0.5), ("b".into(), 0.5)];
        let mut step = args(&params, &owned, 1, 3);
        step.rank_seed = &seed;
        assert!(run_bsp_superstep_core(&csr, &step).is_err());
    }

    #[test]
    fn count_phase_reports_seed_hits() {
        let csr = triangle_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        // Seed on "a" (weight 1.0) and "ghost" (absent) → exactly one owned hit.
        let mut seed = HashMap::new();
        seed.insert("a".to_string(), 1.0);
        seed.insert("ghost".to_string(), 1.0);
        let params = AlgoParams {
            collection: "test_coll".into(),
            damping: Some(0.85),
            personalization_vector: Some(seed),
            ..AlgoParams::default()
        };
        let res = run_bsp_superstep_core(&csr, &args(&params, &owned, 0, 0)).unwrap();
        assert_eq!(
            res.seed_hits, 1,
            "only 'a' is an owned positively-weighted seed"
        );
        assert_eq!(res.vertex_count, 3);
    }

    #[test]
    fn personalized_run_starts_from_p_and_steps_like_a_single_node() {
        let csr = triangle_csr();
        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        // All seed mass on "a"; global sum (computed by coordinator) is 1.0.
        let mut seed_map = HashMap::new();
        seed_map.insert("a".to_string(), 1.0);
        let params = AlgoParams {
            collection: "test_coll".into(),
            damping: Some(0.85),
            personalization_vector: Some(seed_map),
            ..AlgoParams::default()
        };
        let mut prime = args(&params, &owned, 0, 3);
        prime.personalization_sum = 1.0;
        let initial = run_bsp_superstep_core(&csr, &prime).unwrap();
        let seed: Vec<(String, f64)> = initial
            .node_names
            .iter()
            .cloned()
            .zip(initial.rank_vec.iter().copied())
            .collect();

        let mut step = args(&params, &owned, 1, 3);
        step.personalization_sum = 1.0;
        step.rank_seed = &seed;
        let res = run_bsp_superstep_core(&csr, &step).unwrap();

        // Superstep 0 sets rank = p = [a:1, b:0, c:0]. One step on the ring
        // a→b→c→a with d = 0.85: only `a` gets teleport (0.15), and `a`'s
        // damped mass flows to `b`. So a = 0.15, b = 0.85, c = 0.
        let rank_of = |name: &str| -> f64 {
            let i = res
                .node_names
                .iter()
                .position(|n| n == name)
                .expect("node present");
            res.rank_vec[i]
        };
        let d = 0.85;
        assert!((rank_of("a") - (1.0 - d)).abs() < 1e-9);
        assert!((rank_of("b") - d).abs() < 1e-9);
        assert!(rank_of("c").abs() < 1e-9);
        let sum: f64 = res.rank_vec.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "mass conserved, got {sum}");
    }

    /// A write that lands during a run is above the run's read cut. Every
    /// superstep reads the graph as of the cut, so the run succeeds and its
    /// result is the one it gives with no write.
    #[test]
    fn a_write_during_a_run_changes_no_superstep() {
        use crate::engine::graph::edge_store::{EdgeRef, EdgeStore, VersionStamp};
        use nodedb_types::{DatabaseId, TenantId};

        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("edge store");
        let put = |src: &str, dst: &str, ordinal: i64| {
            store
                .put_edge_version_recorded(
                    EdgeRef::new(
                        DatabaseId::DEFAULT,
                        TenantId::new(0),
                        "test_coll",
                        src,
                        "e",
                        dst,
                    ),
                    b"",
                    VersionStamp::at(ordinal),
                    0,
                    i64::MAX,
                    true,
                )
                .expect("put edge version");
        };
        for (src, dst) in [("a", "b"), ("b", "c"), ("c", "a")] {
            put(src, dst, 10);
        }
        let cut = 15;
        let csr_at_cut = || {
            let memory = nodedb_mem::ScopedMemory::new(
                crate::data::executor::core_loop::test_governor(),
                DatabaseId::DEFAULT,
                TenantId::new(0),
                nodedb_mem::EngineId::Graph,
            );
            super::super::graph_algo::build_csr_for_collection(
                &store,
                DatabaseId::DEFAULT.as_u64(),
                0,
                "test_coll",
                None,
                &nodedb_physical::physical_plan::GraphReadScope::at_installed_cut(cut, None),
                memory,
            )
            .expect("csr as of the cut")
        };

        let owned: Vec<u32> = (0..VShardId::COUNT).collect();
        let params = dummy_params(0.85);
        let mut prime = args(&params, &owned, 0, 3);
        prime.system_as_of = Some(cut);
        let initial = run_bsp_superstep_core(&csr_at_cut(), &prime).unwrap();
        let seed: Vec<(String, f64)> = initial
            .node_names
            .iter()
            .cloned()
            .zip(initial.rank_vec.iter().copied())
            .collect();
        let mut step = args(&params, &owned, 1, 3);
        step.system_as_of = Some(cut);
        step.rank_seed = &seed;
        let before = run_bsp_superstep_core(&csr_at_cut(), &step).unwrap();

        // A write lands between supersteps, sequenced after the cut. It adds
        // a node the run's rank set does not hold.
        put("a", "z", 20);
        let after = run_bsp_superstep_core(&csr_at_cut(), &step).expect("the run succeeds");
        // The edge scan's order is unspecified, so ranks compare by name.
        let ranks = |res: &BspSuperstepResult| -> std::collections::BTreeMap<String, f64> {
            res.node_names
                .iter()
                .cloned()
                .zip(res.rank_vec.iter().copied())
                .collect()
        };
        assert_eq!(
            ranks(&after),
            ranks(&before),
            "the superstep reads the graph as of the cut"
        );
        assert!(after.outbound.is_empty());
        let sum: f64 = after.rank_vec.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "mass conserved, got {sum}");
    }
}
