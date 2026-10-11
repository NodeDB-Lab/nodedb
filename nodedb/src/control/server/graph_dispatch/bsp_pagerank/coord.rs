// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed BSP PageRank.
//!
//! Drives the superstep loop with one dispatch per DISTINCT OWNER NODE (each
//! carrying that node's full owned-vShard set), NOT one dispatch per vShard,
//! using the `GraphOp::BspSuperstep` primitive; each node's dispatch is then
//! fanned across that node's cores. The coordinator OWNS all durable state —
//! the per-node rank vectors and the routed cross-shard contributions;
//! [`BspCoordinator`] is used ONLY for convergence bookkeeping (`record_ack` /
//! `advance`).
//!
//! Each shard is one distinct owner node (the local node + each distinct
//! non-local data-group leader), carrying that node's FULL set of owned vShards.
//! This mirrors `match_scatter`'s per-owner-node scatter; the handler ranks
//! every node homed on that owner in a single CSR pass (see `enumerate.rs`).
//!
//! Steps:
//!
//! 1. **Count.** Dispatch one `BspSuperstep` with `global_n == 0` (the
//!    count-only sentinel) to every node. `global_n = Σ vertex_count`: each
//!    graph node is homed on exactly one owner node. The count phase carries
//!    a fresh cut marker. Every node resolves the run's read cut from it and
//!    answers it, and every later superstep reads at that one cut (see
//!    `graph_dispatch::run_cut`). A write during the run is above the cut, so
//!    the rank set stays the nodes that existed at the cut.
//! 2. **Initial scatter.** Superstep 0 sets every node's initial rank and
//!    returns the contributions it scatters. It changes no rank, so it is not
//!    a convergence step.
//! 3. **Iterate.** Superstep `s >= 1` sends each node its current rank, the
//!    dangling mass of that rank, and the contributions every node scattered
//!    from it. Each node returns the next rank and its scatter. One superstep
//!    is one power iteration of single-node PageRank, so the run halts under
//!    the same tolerance and iteration budget, with no rank mass in flight.
//! 4. **Route.** Every outbound `(target_vshard, dst_name, contrib)` goes to
//!    the node that owns `target_vshard` in the enumeration. The handlers use
//!    that same enumeration as their owned sets, so a contribution always
//!    reaches the node that ranks its destination.
//! 5. **Assemble.** On halt, zip each node's final `rank_vec` with its
//!    `node_names`, concatenate across nodes (each owns a disjoint graph-node
//!    set), and build an `AlgoResultBatch` serialized exactly like the
//!    single-node path.

use std::collections::HashMap;

use nodedb_cluster::distributed_graph::{BspCoordinator, SuperstepAck};
use nodedb_graph::{AlgoParams, GraphAlgorithm};

use crate::bridge::envelope::Payload;
use crate::control::state::SharedState;
use crate::engine::graph::algo::result::AlgoResultBatch;
use crate::types::{DatabaseId, TenantId};

use super::enumerate::enumerate_shards;
use super::scatter::{ScatterSuperstepParams, ShardDispatch, ShardResult, scatter_superstep};
use crate::control::server::graph_dispatch::run_cut::{agreed_cut, new_cut_marker};
use crate::control::server::graph_dispatch::shard_reads::{ShardReadLog, qualified};

/// Default max supersteps when the query carries no explicit `ITERATIONS`.
/// Mirrors the single-node PageRank default iteration budget.
const DEFAULT_MAX_ITERATIONS: u32 = 20;

/// Per-node rank state the coordinator owns across supersteps: the owned vShard
/// set (passed to the handler each superstep), the node names (positionally
/// aligned with `rank_vec`), and the current rank vector.
struct ShardRankState {
    is_local: bool,
    owned_vshards: Vec<u32>,
    route_vshard: u32,
    node_names: Vec<String>,
    rank_vec: Vec<f64>,
}

/// The state one superstep hands to the next: the contributions routed to
/// each owner node, and the dangling mass of the rank they came from.
#[derive(Default)]
struct Carry {
    incoming: HashMap<u64, Vec<(String, f64)>>,
    global_dangling: f64,
}

/// Run distributed BSP PageRank and return the bare `AlgoResultBatch` payload
/// (the exact shape `algo_payload_to_query_response` consumes — identical to the
/// single-node path).
///
/// Caller guarantees cluster mode (`cluster_routing.is_some()`) and
/// `algorithm == PageRank`; single-node / other algorithms never enter here.
pub async fn run_bsp_pagerank(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    params: AlgoParams,
    deadline_ms: u64,
    linearizable: bool,
) -> crate::Result<Payload> {
    let algorithm = GraphAlgorithm::PageRank;

    // ── Enumerate shards (one per distinct owner node, local + remote). ──
    let enumeration = enumerate_shards(state)?;
    let targets = enumeration.targets;
    // `vShard → owner node` map: routes each outbound contribution's
    // `target_vshard` to the node-shard that owns it.
    let vshard_owner = enumeration.vshard_owner;
    if targets.is_empty() {
        return empty_payload();
    }

    // Personalized-PageRank global seed sum: `Σ max(w, 0.0)` over the seed map.
    // Personalization is active only when this is positive AND at least one
    // seed name exists somewhere in the cluster graph (the count phase's
    // `seed_hits`), matching single-node `build_personalization` returning
    // `None` for unknown seeds.
    let global_seed_sum: f64 = params
        .personalization_vector()
        .map(|seed| seed.values().map(|&w| w.max(0.0)).sum())
        .unwrap_or(0.0);

    let max_iterations = params
        .max_iterations
        .map(|m| m.clamp(1, u32::MAX as usize) as u32)
        .unwrap_or(DEFAULT_MAX_ITERATIONS);
    let tolerance = params.convergence_tolerance();
    // `BspCoordinator` needs stable shard ids: node ids (cast to u32) serve.
    let shard_ids: Vec<u32> = targets.iter().map(|t| t.node_id as u32).collect();
    let mut bsp = BspCoordinator::new(
        algorithm.name().to_string(),
        max_iterations,
        tolerance,
        shard_ids,
    );

    // ── Count. global_n = 0 sentinel → handler returns owned counts. ──
    // The count phase also pins the run's read cut: every node resolves it
    // from one Calvin cut marker and reads every superstep at it.
    let read_cut_marker = new_cut_marker(state);
    let count_dispatches: Vec<ShardDispatch> = targets
        .iter()
        .map(|t| ShardDispatch {
            node_id: t.node_id,
            is_local: t.is_local,
            owned_vshards: t.owned_vshards.clone(),
            route_vshard: t.route_vshard(),
            incoming_contributions: Vec::new(),
            rank_seed: Vec::new(),
            global_dangling: 0.0,
            personalization_sum: 0.0,
            read_cut_marker,
            system_as_of: None,
        })
        .collect();
    let counts = scatter_superstep(
        state,
        ScatterSuperstepParams {
            tenant_id,
            database_id,
            algorithm,
            params: &params,
            superstep: 0,
            global_n: 0, // count-only sentinel
            dispatches: count_dispatches,
            deadline_ms,
            linearizable,
        },
    )
    .await?;

    // The count phase reads every owner's partition first. A write after it
    // on any owned vShard changes what later supersteps read, so the count
    // phase's versions are the ones the transaction read-set keeps.
    let mut reads = ShardReadLog::new();
    for count in &counts {
        if let Some(target) = targets.iter().find(|t| t.node_id == count.node_id) {
            reads.note(target.owned_vshards.iter().copied(), &count.read_versions);
        }
    }
    reads.publish(
        tenant_id,
        database_id,
        Some(qualified(database_id, &params.collection)),
    );

    let system_as_of = agreed_cut(counts.iter().map(|c| (c.node_id, c.result.system_as_of)))?;
    let global_n: usize = counts.iter().map(|c| c.result.vertex_count).sum();
    if global_n == 0 {
        return empty_payload();
    }

    let global_seed_hits: usize = counts.iter().map(|c| c.result.seed_hits).sum();
    let personalization_sum = if global_seed_sum > 0.0 && global_seed_hits > 0 {
        global_seed_sum
    } else {
        0.0
    };

    let mut shard_state: HashMap<u64, ShardRankState> = HashMap::with_capacity(targets.len());
    for (target, count) in targets.iter().zip(counts) {
        shard_state.insert(
            target.node_id,
            ShardRankState {
                is_local: target.is_local,
                owned_vshards: target.owned_vshards.clone(),
                route_vshard: target.route_vshard(),
                node_names: count.result.node_names,
                rank_vec: Vec::new(),
            },
        );
    }

    // ── Initial scatter (superstep 0) and iterations. ──
    let mut carry = Carry::default();
    let mut superstep: u32 = 0;
    loop {
        let mut ordered_nodes: Vec<u64> = shard_state.keys().copied().collect();
        ordered_nodes.sort_unstable();

        let dispatches: Vec<ShardDispatch> = ordered_nodes
            .iter()
            .map(|&node_id| {
                let st = &shard_state[&node_id];
                ShardDispatch {
                    node_id,
                    is_local: st.is_local,
                    owned_vshards: st.owned_vshards.clone(),
                    route_vshard: st.route_vshard,
                    incoming_contributions: carry.incoming.remove(&node_id).unwrap_or_default(),
                    rank_seed: st
                        .node_names
                        .iter()
                        .cloned()
                        .zip(st.rank_vec.iter().copied())
                        .collect(),
                    global_dangling: carry.global_dangling,
                    personalization_sum,
                    read_cut_marker: 0,
                    system_as_of: Some(system_as_of),
                }
            })
            .collect();

        let results = scatter_superstep(
            state,
            ScatterSuperstepParams {
                tenant_id,
                database_id,
                algorithm,
                params: &params,
                superstep,
                global_n,
                dispatches,
                deadline_ms,
                linearizable,
            },
        )
        .await?;

        carry = Carry::default();
        for sr in results {
            // Superstep 0 changes no rank: it is not a convergence step.
            if superstep > 0 {
                bsp.record_ack(SuperstepAck {
                    shard_id: sr.node_id as u32,
                    iteration: superstep,
                    local_delta: sr.result.local_delta,
                    vertex_count: sr.result.vertex_count,
                    contributions_sent: sr.result.outbound.len(),
                });
            }
            absorb(sr, &vshard_owner, &mut shard_state, &mut carry)?;
        }

        if superstep > 0 {
            // Every shard ACKs exactly once per dispatch; `advance` refuses a
            // partial barrier rather than summing a subset.
            let keep_going = bsp.advance().map_err(|e| crate::Error::Internal {
                detail: format!(
                    "bsp pagerank: not all shards acked after superstep dispatch ({e})"
                ),
            })?;
            if !keep_going {
                break;
            }
        }
        superstep += 1;
    }

    assemble_result(&shard_state)
}

/// Fold one node's superstep result into the coordinator state: store its
/// rank, add its dangling mass, and route its contributions to the nodes that
/// own their destinations. A contribution to a vShard no node owns is an
/// error: its mass will leave the graph.
fn absorb(
    result: ShardResult,
    vshard_owner: &HashMap<u32, u64>,
    shard_state: &mut HashMap<u64, ShardRankState>,
    carry: &mut Carry,
) -> crate::Result<()> {
    let ShardResult {
        node_id, result, ..
    } = result;
    carry.global_dangling += result.dangling_sum;
    for (target_vshard, dst_name, contrib) in result.outbound {
        let Some(&owner) = vshard_owner.get(&target_vshard) else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "bsp pagerank: outbound contribution to unmapped target \
                     vshard={target_vshard} (dst={dst_name})"
                ),
            });
        };
        if !shard_state.contains_key(&owner) {
            return Err(crate::Error::Internal {
                detail: format!(
                    "bsp pagerank: outbound contribution to unknown owner \
                     node={owner} for vshard={target_vshard} (dst={dst_name})"
                ),
            });
        }
        carry
            .incoming
            .entry(owner)
            .or_default()
            .push((dst_name, contrib));
    }
    let Some(st) = shard_state.get_mut(&node_id) else {
        return Err(crate::Error::Internal {
            detail: format!("bsp pagerank: result from node {node_id}, which ranks no shard"),
        });
    };
    st.node_names = result.node_names;
    st.rank_vec = result.rank_vec;
    Ok(())
}

/// Concatenate every node's `(node_name, rank)` into an `AlgoResultBatch` using
/// the same `push_node_f64` + `to_msgpack` seam as single-node PageRank, so
/// `algo_payload_to_query_response` produces byte-identical client output. Each
/// owner node holds a disjoint graph-node set, so no dedup is required.
fn assemble_result(shard_state: &HashMap<u64, ShardRankState>) -> crate::Result<Payload> {
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::PageRank);
    // Deterministic node order for a stable row order across runs.
    let mut ordered: Vec<u64> = shard_state.keys().copied().collect();
    ordered.sort_unstable();
    for node_id in ordered {
        let st = &shard_state[&node_id];
        for (name, rank) in st.node_names.iter().zip(st.rank_vec.iter()) {
            batch.push_node_f64(name.clone(), *rank);
        }
    }
    let bytes = batch.to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}

/// An empty PageRank result encoded the same way the single-node empty-CSR path
/// encodes it (`AlgoResultBatch::new(...).to_msgpack()`).
fn empty_payload() -> crate::Result<Payload> {
    let bytes = AlgoResultBatch::new(GraphAlgorithm::PageRank).to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}
