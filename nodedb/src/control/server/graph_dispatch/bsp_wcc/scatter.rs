// SPDX-License-Identifier: BUSL-1.1

//! Per-node `WccSuperstep` dispatch: one plan per distinct owner node, all
//! issued concurrently via `join_all`, decoding each node's
//! [`WccSuperstepResult`].
//!
//! Each dispatch carries the owner node's FULL `owned_vshards` set so the
//! handler contracts every node homed on that owner in a single CSR pass. A
//! remote node gets one `RouteDecision::Remote` dispatch; the local node fans
//! across ALL local cores via `execute_plan_all_local_cores` (which merges the
//! per-core disjoint results into one `WccSuperstepResult` before returning).
//! At 1 core/node the fan is over a single core and behaviour is identical to a
//! single-core dispatch.

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::server::graph_dispatch::bsp_pagerank::enumerate::ShardTarget;
use crate::control::server::graph_dispatch::cluster_resolve::{
    DispatchSuperstepParams, dispatch_superstep_to_node, gateway_shared,
};
use crate::types::{DatabaseId, TenantId};
use nodedb_graph::AlgoParams;
use nodedb_physical::physical_plan::{GraphOp, WccSuperstepPlan, WccSuperstepResult};

/// One owner node's decoded WCC result.
pub(super) struct ShardWccResult {
    pub(super) node_id: u64,
    pub(super) result: WccSuperstepResult,
    /// The versions the node's cores reported for the round.
    pub(super) read_versions: crate::types::ReadVersions,
}

/// The inputs of one WCC round.
pub(super) struct WccRound<'a> {
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) params: &'a AlgoParams,
    pub(super) targets: &'a [ShardTarget],
    /// The cut marker every node resolves the round's read cut from.
    pub(super) read_cut_marker: u64,
    pub(super) deadline_ms: u64,
    pub(super) linearizable: bool,
}

/// Dispatch one `WccSuperstep` to every owner node concurrently and decode each
/// node's [`WccSuperstepResult`]. Single round — there is no count phase or
/// loop; the coordinator stitches the returned results globally.
pub(super) async fn scatter_wcc_round(
    state: &crate::control::state::SharedState,
    round: WccRound<'_>,
) -> crate::Result<Vec<ShardWccResult>> {
    let WccRound {
        tenant_id,
        database_id,
        params,
        targets,
        read_cut_marker,
        deadline_ms,
        linearizable,
    } = round;
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    let futs = targets.iter().map(|t| {
        let plan = PhysicalPlan::Graph(GraphOp::WccSuperstep(Box::new(WccSuperstepPlan {
            params: params.clone(),
            // FULL owned set for this node — the handler contracts every node
            // homed here in one pass and emits boundary edges only for dsts on
            // OTHER nodes.
            owned_vshards: t.owned_vshards.clone(),
            // Every node resolves the same read cut from the marker.
            read_cut_marker,
            system_as_of: None,
        })));
        let version_set = version_set.clone();
        let node_id = t.node_id;
        let is_local = t.is_local;
        let route_vshard = t.route_vshard();
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
            let result = decode_wcc_from_payload(node_id, read.payload)?;
            Ok::<ShardWccResult, crate::Error>(ShardWccResult {
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

/// Decode a single node's `WccSuperstepResult` from an already-wrapped
/// [`Payload`]. Empty payload → `WccSuperstepResult::default()` (zero-vertex
/// shard, contributes no labels or boundary edges).
fn decode_wcc_from_payload(node_id: u64, payload: Payload) -> crate::Result<WccSuperstepResult> {
    if payload.is_empty() {
        return Ok(WccSuperstepResult::default());
    }
    zerompk::from_msgpack::<WccSuperstepResult>(payload.as_ref()).map_err(|e| crate::Error::Codec {
        detail: format!("wcc: node={node_id} result decode: {e}"),
    })
}
