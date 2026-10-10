// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed WCC (single-round contraction).
//!
//! Unlike distributed PageRank, WCC is NOT iterative. The coordinator:
//!
//! 1. **Enumerate.** One shard per distinct owner node (local + each distinct
//!    non-local data-group leader), each carrying that node's FULL owned-vShard
//!    set. Reuses `bsp_pagerank::enumerate::enumerate_shards`.
//! 2. **Contract.** Dispatch ONE `GraphOp::WccSuperstep` per owner node, with
//!    one fresh cut marker. Every node resolves the same read cut from it and
//!    reads the graph at it (see `graph_dispatch::run_cut`). Each
//!    shard contracts its OWNED nodes into local components and returns
//!    `node_labels` (`(name, local_component_root_name)`) plus `boundary_edges`
//!    (`(owned_name, ghost_name)` for every owned→ghost out-edge).
//! 3. **Stitch.** Concatenate every shard's `node_labels` + `boundary_edges` and
//!    call `wcc::stitch_components` to build ONE global union-find over node
//!    names, assigning dense `component_id: i64` ids ordered by each component's
//!    minimum node name.
//! 4. **Assemble.** Emit one `(node_name, component_id)` row per node via
//!    `AlgoResultBatch::push_node_i64`, serialized exactly like the single-node
//!    path so the client output is byte-identical.

use nodedb_cluster::distributed_graph::stitch_components;
use nodedb_graph::{AlgoParams, GraphAlgorithm};

use crate::bridge::envelope::Payload;
use crate::control::server::graph_dispatch::bsp_pagerank::enumerate::enumerate_shards;
use crate::control::state::SharedState;
use crate::engine::graph::algo::result::AlgoResultBatch;
use crate::types::{DatabaseId, TenantId};

use super::scatter::{WccRound, scatter_wcc_round};
use crate::control::server::graph_dispatch::run_cut::{agreed_cut, new_cut_marker};
use crate::control::server::graph_dispatch::shard_reads::{ShardReadLog, qualified};

/// Run distributed WCC and return the bare `AlgoResultBatch` payload (the exact
/// shape `algo_payload_to_query_response` consumes — identical to the
/// single-node path).
///
/// Caller guarantees cluster mode (`cluster_routing.is_some()`) and
/// `algorithm == Wcc`; single-node / other algorithms never enter here.
pub async fn run_bsp_wcc(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    params: AlgoParams,
    deadline_ms: u64,
    linearizable: bool,
) -> crate::Result<Payload> {
    // ── Enumerate shards (one per distinct owner node, local + remote). ──
    let enumeration = enumerate_shards(state)?;
    let targets = enumeration.targets;
    if targets.is_empty() {
        // No data shards — empty result (same as single-node empty CSR).
        return empty_payload();
    }

    // ── Single contraction round across every owner node. ──
    // Every node reads at one read cut, so every node reads the same graph.
    let results = scatter_wcc_round(
        state,
        WccRound {
            tenant_id,
            database_id,
            params: &params,
            targets: &targets,
            read_cut_marker: new_cut_marker(state),
            deadline_ms,
            linearizable,
        },
    )
    .await?;
    agreed_cut(
        results
            .iter()
            .map(|sr| (sr.node_id, sr.result.system_as_of)),
    )?;

    // Every owner's partition was read once, at the versions it reported.
    let mut reads = ShardReadLog::new();
    for sr in &results {
        if let Some(target) = targets.iter().find(|t| t.node_id == sr.node_id) {
            reads.note(target.owned_vshards.iter().copied(), &sr.read_versions);
        }
    }
    reads.publish(
        tenant_id,
        database_id,
        Some(qualified(database_id, &params.collection)),
    );

    // Concatenate every shard's local labels + boundary edges. Each owner node
    // holds a disjoint owned-node set, so the union of labels has one entry per
    // graph node; boundary edges stitch components across shard boundaries.
    let mut node_labels: Vec<(String, String)> = Vec::new();
    let mut boundary_edges: Vec<(String, String)> = Vec::new();
    for sr in results {
        node_labels.extend(sr.result.node_labels);
        boundary_edges.extend(sr.result.boundary_edges);
    }

    if node_labels.is_empty() {
        // No nodes anywhere.
        return empty_payload();
    }

    // ── Stitch into global components with dense ids ordered by min name. ──
    let rows = stitch_components(node_labels, boundary_edges);

    // ── Assemble final AlgoResultBatch (single-node-identical shape). ──
    let mut batch = AlgoResultBatch::new(GraphAlgorithm::Wcc);
    for (name, component_id) in rows {
        batch.push_node_i64(name, component_id);
    }
    let bytes = batch.to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}

/// An empty WCC result encoded the same way the single-node empty-CSR path
/// encodes it (`AlgoResultBatch::new(...).to_msgpack()`).
fn empty_payload() -> crate::Result<Payload> {
    let bytes = AlgoResultBatch::new(GraphAlgorithm::Wcc).to_msgpack()?;
    Ok(Payload::from_vec(bytes))
}
