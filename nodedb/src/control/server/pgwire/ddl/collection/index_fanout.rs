//! Cluster fan-out for `CREATE INDEX` backfill.
//!
//! Single-node CREATE INDEX runs `DocumentOp::BackfillIndex` against
//! the local Data Plane and is sufficient. In cluster mode the
//! collection's documents are spread across vShards hosted on
//! multiple nodes, and the local dispatch only populates the
//! coordinator's vShards. Non-coordinator nodes host rows the
//! coordinator never sees, so the Ready commit would pass with an
//! incomplete index — a silent-miss class.
//!
//! This module drives the fan-out: after the coordinator's local
//! backfill succeeds, it sends the same `DocumentOp::BackfillIndex`
//! plan to every other node via `RaftRpc::ExecuteRequest` over the
//! existing cluster transport. Failure on any node leaves the index
//! in the `Building` state and surfaces the remote error verbatim;
//! the caller commits the Ready flip only on unanimous success.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::DocumentOp;
use crate::control::state::SharedState;
use crate::types::TenantId;

use crate::bridge::physical_plan::wire as plan_wire;
use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc};
use nodedb_cluster::topology::NodeState;

use super::super::super::types::sqlstate_error;

/// Remaining budget for per-peer RPCs. Chosen to cover backfill on
/// collections with up to ~1M rows at the Data Plane's current
/// throughput; large production collections will need a streaming
/// backfill that reports progress, tracked separately.
const PEER_BACKFILL_DEADLINE: Duration = Duration::from_secs(120);

/// Run `DocumentOp::BackfillIndex` on every cluster node other than
/// this coordinator. Returns `Ok(())` only when every peer reports
/// success; any peer failure is returned as a pgwire error with
/// SQLSTATE 23505 for duplicates and XX000 otherwise, matching the
/// single-node path.
///
/// Single-node clusters (no peers) return `Ok(())` immediately — the
/// coordinator's local dispatch already covered everything.
/// Inputs to [`backfill_on_peers`]. Mirrors the fields of
/// `DocumentOp::BackfillIndex` plus the owning tenant — grouped as a
/// struct because the handler forwards every field unchanged and the
/// positional form at the call site obscured which boolean meant
/// which (clippy's `too_many_arguments` rightly flagged it).
pub(super) struct PeerBackfill<'a> {
    pub tenant_id: TenantId,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
}

pub(super) async fn backfill_on_peers(
    state: &SharedState,
    args: PeerBackfill<'_>,
) -> Result<(), pgwire::error::PgWireError> {
    let Some(transport) = state.cluster_transport.as_ref() else {
        // Non-cluster build / single-node without cluster transport:
        // the local dispatch is the only required step.
        return Ok(());
    };
    let Some(topology_lock) = state.cluster_topology.as_ref() else {
        return Ok(());
    };

    // Snapshot peer ids under the topology read lock — do not hold the
    // lock across `.await`. `all_nodes` includes learners and paused
    // peers; restrict to `Active` to avoid sending backfill RPCs to
    // nodes that are mid-join or draining (they'll pick up the index
    // when their applier observes the Ready commit and dual-writes
    // catch up the rows they host).
    let peer_ids: Vec<u64> = {
        let topology = topology_lock
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        topology
            .all_nodes()
            .filter(|n| n.state == NodeState::Active && n.node_id != state.node_id)
            .map(|n| n.node_id)
            .collect()
    };

    if peer_ids.is_empty() {
        return Ok(());
    }

    let plan = PhysicalPlan::Document(DocumentOp::BackfillIndex {
        collection: args.collection.to_string(),
        path: args.path.to_string(),
        is_array: args.is_array,
        unique: args.unique,
        case_insensitive: args.case_insensitive,
        predicate: args.predicate.map(str::to_string),
    });
    let plan_bytes = plan_wire::encode(&plan)
        .map_err(|e| sqlstate_error("XX000", &format!("backfill plan encode: {e}")))?;

    // Fan out in parallel; collect per-peer outcomes. Any failure
    // aborts the commit — we do NOT compensate by dropping the index
    // here because the coordinator still holds the Building entry in
    // the catalog and the caller's error return leaves it that way
    // for a retry (DROP + CREATE) to clear.
    let deadline_ms = PEER_BACKFILL_DEADLINE.as_millis() as u64;
    let trace_id = rand::random::<u64>();
    let mut joins = Vec::with_capacity(peer_ids.len());
    for node_id in peer_ids {
        let transport = transport.clone();
        let plan_bytes = plan_bytes.clone();
        let req = RaftRpc::ExecuteRequest(ExecuteRequest {
            plan_bytes,
            tenant_id: args.tenant_id.as_u32(),
            deadline_remaining_ms: deadline_ms,
            trace_id,
            descriptor_versions: Vec::new(),
        });
        joins.push(tokio::spawn(async move {
            let outcome = transport.send_rpc(node_id, req).await;
            (node_id, outcome)
        }));
    }

    for join in joins {
        let (node_id, outcome) = join
            .await
            .map_err(|e| sqlstate_error("XX000", &format!("peer backfill join: {e}")))?;
        let resp = outcome.map_err(|e| {
            sqlstate_error(
                "XX000",
                &format!("peer backfill transport to node {node_id}: {e}"),
            )
        })?;
        let RaftRpc::ExecuteResponse(resp) = resp else {
            return Err(sqlstate_error(
                "XX000",
                &format!("peer backfill on node {node_id}: unexpected RPC variant {resp:?}"),
            ));
        };
        if let Some(err) = resp.error {
            let detail = format!("peer backfill on node {node_id}: {err:?}");
            let code = if detail.to_lowercase().contains("unique") {
                "23505"
            } else {
                "XX000"
            };
            return Err(sqlstate_error(code, &detail));
        }
    }

    Ok(())
}
