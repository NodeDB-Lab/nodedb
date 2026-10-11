// SPDX-License-Identifier: BUSL-1.1

//! Per-node `BspSuperstep` dispatch: one plan per distinct owner node, all
//! issued concurrently via `join_all`, decoding each node's
//! `BspSuperstepResult`.
//!
//! Each dispatch carries the owner node's FULL `owned_vshards` set so the
//! handler ranks every node homed on that owner in a single CSR pass. A remote
//! node gets one `RouteDecision::Remote` dispatch; the local node fans across
//! ALL local cores via `execute_plan_all_local_cores` (which merges the
//! per-core disjoint results into one `BspSuperstepResult` before returning).
//! At 1 core/node the fan is over a single core and behaviour is identical to
//! the prior single-core dispatch.

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::server::graph_dispatch::cluster_resolve::{
    DispatchSuperstepParams, dispatch_superstep_to_node, gateway_shared,
};
use crate::types::{DatabaseId, TenantId};
use nodedb_graph::{AlgoParams, GraphAlgorithm};
use nodedb_physical::physical_plan::{BspSuperstepPlan, BspSuperstepResult, GraphOp};

/// Inputs for one node's superstep, paired with its target.
pub(super) struct ShardDispatch {
    /// Owner node id — the stable per-shard key returned in [`ShardResult`].
    pub(super) node_id: u64,
    /// `true` if this node is the coordinating node (dispatch local, single core).
    pub(super) is_local: bool,
    /// The FULL set of vShards this node owns — passed verbatim as the plan's
    /// `owned_vshards` so the handler ranks every node homed on this owner.
    pub(super) owned_vshards: Vec<u32>,
    /// One of this node's vShards, used as the remote route's `vshard_id` (any
    /// one of the node's vShards selects the same node).
    pub(super) route_vshard: u32,
    /// Cross-shard contributions routed to THIS node's owned nodes this
    /// superstep (empty on the count phase and superstep 0).
    pub(super) incoming_contributions: Vec<(String, f64)>,
    /// Name-keyed `(node_name, rank)` seed for this node's owned nodes (empty on
    /// the count phase and on superstep 0 — the handler seeds `1/global_n`).
    pub(super) rank_seed: Vec<(String, f64)>,
    /// Global dangling-node rank mass to seed this superstep's teleport base.
    /// `0.0` on the count phase and on superstep 0.
    pub(super) global_dangling: f64,
    /// Coordinator-computed GLOBAL `Σ max(w, 0.0)` over the Personalized-PageRank
    /// seed map. `0.0` = uniform PageRank; `> 0.0` activates PPR on every shard.
    /// Constant across all dispatches within a run (it is a cluster-wide scalar).
    pub(super) personalization_sum: f64,
    /// The cut marker the node resolves the run's read cut from: set on the
    /// count phase only, `0` after (see `BspSuperstepPlan::read_cut_marker`).
    pub(super) read_cut_marker: u64,
    /// The run's read cut, once the count phase resolved it.
    pub(super) system_as_of: Option<i64>,
}

/// One node's decoded superstep result, tagged with its owner node id.
pub(super) struct ShardResult {
    pub(super) node_id: u64,
    pub(super) result: BspSuperstepResult,
    /// The versions the node's cores reported for the superstep.
    pub(super) read_versions: crate::types::ReadVersions,
}

/// Parameters for [`scatter_superstep`].
pub(super) struct ScatterSuperstepParams<'a> {
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) algorithm: GraphAlgorithm,
    pub(super) params: &'a AlgoParams,
    pub(super) superstep: u32,
    pub(super) global_n: usize,
    pub(super) dispatches: Vec<ShardDispatch>,
    pub(super) deadline_ms: u64,
    pub(super) linearizable: bool,
}

/// Dispatch one `BspSuperstep` to every owner node concurrently and decode each
/// node's [`BspSuperstepResult`]. `global_n == 0` is the count-only phase
/// (handler short-circuits after counting owned nodes).
pub(super) async fn scatter_superstep(
    state: &crate::control::state::SharedState,
    args: ScatterSuperstepParams<'_>,
) -> crate::Result<Vec<ShardResult>> {
    let ScatterSuperstepParams {
        tenant_id,
        database_id,
        algorithm,
        params,
        superstep,
        global_n,
        dispatches,
        deadline_ms,
        linearizable,
    } = args;
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    let futs = dispatches.into_iter().map(|d| {
        let plan = PhysicalPlan::Graph(GraphOp::BspSuperstep(Box::new(BspSuperstepPlan {
            algorithm,
            params: params.clone(),
            superstep,
            global_n,
            // FULL owned set for this node — the handler ranks every node homed
            // here in one pass and emits ghosts only for dsts on OTHER nodes.
            owned_vshards: d.owned_vshards.clone(),
            incoming_contributions: d.incoming_contributions,
            rank_seed: d.rank_seed,
            global_dangling: d.global_dangling,
            personalization_sum: d.personalization_sum,
            read_cut_marker: d.read_cut_marker,
            system_as_of: d.system_as_of,
        })));
        let version_set = version_set.clone();
        let node_id = d.node_id;
        let is_local = d.is_local;
        let route_vshard = d.route_vshard;
        let shared_arc = shared_arc.clone();

        Box::pin(async move {
            let read = dispatch_superstep_to_node(
                &shared_arc,
                DispatchSuperstepParams {
                    tenant_id,
                    database_id,
                    deadline_ms,
                    node_id,
                    is_local,
                    route_vshard,
                    plan,
                    version_set: &version_set,
                    linearizable,
                },
            )
            .await?;
            let result = decode_single_result_from_payload(node_id, read.payload)?;
            Ok::<ShardResult, crate::Error>(ShardResult {
                node_id,
                result,
                read_versions: read.read_versions,
            })
        })
    });

    let results = join_all(futs).await;
    let mut out = Vec::with_capacity(results.len());
    for res in results {
        out.push(res?);
    }
    Ok(out)
}

/// Decode a single node's `BspSuperstepResult` from an already-wrapped
/// [`Payload`].  Empty payload → `BspSuperstepResult::default()` (zero-vertex
/// shard, contributes nothing). Used by both the remote (first) payload path
/// and the local all-cores merged payload path via `dispatch_superstep_to_node`.
fn decode_single_result_from_payload(
    node_id: u64,
    payload: Payload,
) -> crate::Result<BspSuperstepResult> {
    if payload.is_empty() {
        return Ok(BspSuperstepResult::default());
    }
    zerompk::from_msgpack::<BspSuperstepResult>(payload.as_ref()).map_err(|e| crate::Error::Codec {
        detail: format!("bsp pagerank: node={node_id} result decode: {e}"),
    })
}
