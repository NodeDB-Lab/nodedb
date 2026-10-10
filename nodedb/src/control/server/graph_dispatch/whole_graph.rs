// SPDX-License-Identifier: BUSL-1.1

//! Graph reads that need every partition of a graph: `SHOW GRAPH STATS` and
//! `GRAPH ALGO`.
//!
//! A graph edge lives on the key vShard of each endpoint
//! (`types/record_home.rs`), so a collection's edges spread over every data
//! group. Each of these reads goes to one node per data group: the group's
//! leader, carrying every group it leads, exactly as the BSP coordinators
//! enumerate shards (`bsp_pagerank::enumerate`). Each node answers from every
//! one of its cores, and every node must answer, or the read fails.
//!
//! How an algorithm uses the partitions:
//!
//! - PageRank and WCC run as BSP supersteps across the owners in a cluster.
//!   Both are vertex-centric and converge to the single-node answer from
//!   per-shard message passing.
//! - Every other algorithm gathers the collection's edges from every owner
//!   and runs once, on one core of this node, over their union.
//!   Label propagation and Louvain depend on update order, so a synchronous
//!   BSP round will diverge from the single-node answer. LCC and triangles
//!   need two-hop neighbourhoods. Betweenness, closeness, harmonic, diameter
//!   and SSSP need shortest paths over the whole graph. K-core peels the
//!   whole graph. Degree needs both endpoints of each edge. The gathered edges
//!   are sorted, so the CSR, and the answer, match a single node's exactly.
//! - On a single node every algorithm gathers, so an algorithm sees the edges
//!   of every core, not one core's share.
//! - A historical run (`AS OF SYSTEM TIME`) gathers for every algorithm: each
//!   owner exports the edges live at that time.
//! - A gathered run needs the whole graph in one CSR on one core, as a single
//!   node's run does. It is bounded by `GraphTuning::max_gathered_algo_edges`:
//!   past the cap it is refused with the edge count, never answered from part
//!   of the graph.

use std::collections::BTreeMap;

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::statement_deadline_ms;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::server::exchange::execute_plan_all_local_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_graph::{AlgoParams, GraphAlgorithm};
use nodedb_physical::physical_plan::{AlgoEdge, AlgoStage, GraphOp};

use super::bsp_pagerank::enumerate::enumerate_shards;
use super::cluster_resolve::{DispatchSuperstepParams, dispatch_superstep_to_node, gateway_shared};
use super::shard_reads::{ShardReadLog, qualified};

/// Run `plan` on one node per data group and return each node's payload.
///
/// On a single node the plan fans across this node's cores and yields one
/// payload. A linearizable read is confirmed on each node that serves it.
/// Every vShard read joins the transaction read-set, scoped to `collection`
/// (database-qualified), or to every collection when `None`.
pub async fn scatter_to_graph_owners(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    linearizable: bool,
    collection: Option<String>,
) -> crate::Result<Vec<Payload>> {
    if state.cluster_routing.is_none() {
        let node =
            execute_plan_all_local_cores(state, tenant_id, database_id, plan, TraceId::ZERO, None)
                .await?;
        // This node holds every vShard, so the read observed all of them.
        let mut reads = ShardReadLog::new();
        reads.note(0..VShardId::COUNT, &node.read_versions);
        reads.publish(tenant_id, database_id, collection);
        return Ok(vec![Payload::from_vec(node.payload)]);
    }
    let targets = enumerate_shards(state)?.targets;
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());
    let deadline_ms = statement_deadline_ms(state);
    let legs = targets.into_iter().map(|target| {
        let plan = plan.clone();
        let shared_arc = shared_arc.clone();
        let version_set = version_set.clone();
        async move {
            let read = dispatch_superstep_to_node(
                &shared_arc,
                DispatchSuperstepParams {
                    tenant_id,
                    database_id,
                    deadline_ms,
                    node_id: target.node_id,
                    is_local: target.is_local,
                    route_vshard: target.route_vshard(),
                    plan,
                    version_set: &version_set,
                    linearizable,
                },
            )
            .await?;
            Ok::<_, crate::Error>((target.owned_vshards, read))
        }
    });
    let mut reads = ShardReadLog::new();
    let mut payloads = Vec::new();
    for leg in join_all(legs).await {
        let (owned_vshards, read) = leg?;
        reads.note(owned_vshards, &read.read_versions);
        payloads.push(read.payload);
    }
    reads.publish(tenant_id, database_id, collection);
    Ok(payloads)
}

/// Run `algorithm` over the whole graph of `params.collection` and return its
/// `AlgoResultBatch` payload. `system_as_of_ms` runs it over the edges live at
/// that system time; `None` runs it over the current edges.
pub async fn run_graph_algo(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    algorithm: GraphAlgorithm,
    params: AlgoParams,
    system_as_of_ms: Option<i64>,
    linearizable: bool,
) -> crate::Result<Payload> {
    let deadline_ms = statement_deadline_ms(state);
    // A current-state BSP run pins its own read cut from a Calvin cut marker
    // (`run_cut`). A historical run gathers the edges live at its system time.
    if state.cluster_routing.is_some() && system_as_of_ms.is_none() {
        match algorithm {
            GraphAlgorithm::PageRank => {
                return super::run_bsp_pagerank(
                    state,
                    tenant_id,
                    database_id,
                    params,
                    deadline_ms,
                    linearizable,
                )
                .await;
            }
            GraphAlgorithm::Wcc => {
                return super::run_bsp_wcc(
                    state,
                    tenant_id,
                    database_id,
                    params,
                    deadline_ms,
                    linearizable,
                )
                .await;
            }
            _ => {}
        }
    }
    let edges = gather_algo_edges(
        state,
        tenant_id,
        database_id,
        algorithm,
        &params,
        system_as_of_ms,
        linearizable,
    )
    .await?;
    let plan = PhysicalPlan::Graph(GraphOp::Algo {
        algorithm,
        params,
        stage: AlgoStage::Gathered { edges },
    });
    let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        VShardId::new(0),
        plan,
        TraceId::ZERO,
    )
    .await?;
    crate::control::local_dispatch::reject_data_plane_error(&response)?;
    Ok(response.payload)
}

/// The union of the collection's edges on every owner, sorted by
/// `(src, label, dst)`. An edge held by more than one node (both endpoint
/// homes, or a replica) appears once.
async fn gather_algo_edges(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
    system_as_of_ms: Option<i64>,
    linearizable: bool,
) -> crate::Result<Vec<AlgoEdge>> {
    let plan = PhysicalPlan::Graph(GraphOp::Algo {
        algorithm,
        params: params.clone(),
        stage: AlgoStage::ExportEdges { system_as_of_ms },
    });
    let payloads = scatter_to_graph_owners(
        state,
        tenant_id,
        database_id,
        plan,
        linearizable,
        Some(qualified(database_id, &params.collection)),
    )
    .await?;
    merge_edge_parts(payloads, state.tuning.graph.max_gathered_algo_edges)
}

/// Union each owner's exported edges, sorted by `(src, label, dst)`. An edge
/// two owners hold (both endpoint homes, or a replica) appears once. The run
/// needs every edge in one CSR on one core, so past `cap` distinct edges it is
/// refused with the count gathered so far, never answered from part of the
/// graph.
fn merge_edge_parts(payloads: Vec<Payload>, cap: usize) -> crate::Result<Vec<AlgoEdge>> {
    let mut union: BTreeMap<(String, String, String), f64> = BTreeMap::new();
    for payload in payloads {
        if payload.is_empty() {
            continue;
        }
        let part: Vec<AlgoEdge> =
            zerompk::from_msgpack(payload.as_ref()).map_err(|e| crate::Error::Codec {
                detail: format!("graph algorithm edge export decode: {e}"),
            })?;
        for edge in part {
            union.insert((edge.src, edge.label, edge.dst), edge.weight);
        }
        if union.len() > cap {
            return Err(crate::Error::LimitExceeded {
                limit_name: "graph.max_gathered_algo_edges",
                value: union.len() as u64,
                max: cap as u64,
            });
        }
    }
    Ok(union
        .into_iter()
        .map(|((src, label, dst), weight)| AlgoEdge {
            src,
            label,
            dst,
            weight,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(src: &str, dst: &str) -> AlgoEdge {
        AlgoEdge {
            src: src.into(),
            label: "l".into(),
            dst: dst.into(),
            weight: 1.0,
        }
    }

    fn part(edges: &[AlgoEdge]) -> Payload {
        Payload::from_vec(zerompk::to_msgpack_vec(&edges.to_vec()).expect("encode part"))
    }

    #[test]
    fn owners_union_into_one_sorted_edge_set() {
        let merged = merge_edge_parts(
            vec![
                part(&[edge("b", "c"), edge("a", "b")]),
                Payload::from_vec(Vec::new()),
                part(&[edge("a", "b"), edge("c", "a")]),
            ],
            10,
        )
        .expect("merge");
        let keys: Vec<(String, String)> = merged.into_iter().map(|e| (e.src, e.dst)).collect();
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "b".to_string()),
                ("b".to_string(), "c".to_string()),
                ("c".to_string(), "a".to_string()),
            ]
        );
    }

    #[test]
    fn a_graph_past_the_cap_is_refused_with_its_edge_count() {
        let error = merge_edge_parts(
            vec![part(&[edge("a", "b"), edge("b", "c"), edge("c", "d")])],
            2,
        )
        .expect_err("three edges exceed a cap of two");
        assert!(matches!(
            error,
            crate::Error::LimitExceeded {
                limit_name: "graph.max_gathered_algo_edges",
                value: 3,
                max: 2,
            }
        ));
    }
}
