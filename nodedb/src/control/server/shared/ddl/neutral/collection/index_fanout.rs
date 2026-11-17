// SPDX-License-Identifier: BUSL-1.1

//! Cluster fan-out for `CREATE INDEX` backfill.
//!
//! Single-node CREATE INDEX runs `DocumentOp::BackfillIndex` against
//! the local Data Plane and is sufficient. In cluster mode the
//! collection's documents are spread across vShards hosted on
//! multiple nodes, and the local dispatch only populates the
//! coordinator's vShards. Non-coordinator nodes host rows the
//! coordinator never sees, so the Ready commit will pass with an
//! incomplete index — a silent-miss class.
//!
//! This module drives the fan-out: after the coordinator's local
//! backfill succeeds, it sends the same `DocumentOp::BackfillIndex`
//! plan to every other node via `RaftRpc::ExecuteRequest` over the
//! existing cluster transport. Failure on any node leaves the index
//! in the `Building` state and surfaces the remote error verbatim;
//! the caller commits the Ready flip only on unanimous success.
//!
//! A full-text index build sends `TextOp::RebuildIndex` to each peer
//! through [`execute_on_peer`], once per poll: its rebuild runs off the
//! peer's core, and each call answers at once with the rebuild's progress.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::DocumentOp;

use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc};
use nodedb_cluster::topology::NodeState;
use nodedb_physical::physical_plan::wire as plan_wire;

use super::super::super::result::DdlError;

/// Remaining budget for per-peer RPCs. Chosen to cover backfill on
/// collections with up to ~1M rows at the Data Plane's current
/// throughput; large production collections will need a streaming
/// backfill that reports progress, tracked separately.
const PEER_BACKFILL_DEADLINE: Duration = Duration::from_secs(120);

/// Inputs to [`backfill_on_peers`]. Mirrors the fields of
/// `DocumentOp::BackfillIndex` plus the owning tenant — grouped as a
/// struct because the handler forwards every field unchanged and the
/// positional form at the call site obscured which boolean meant
/// which (clippy's `too_many_arguments` rightly flagged it).
pub(super) struct PeerBackfill<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
}

/// Run `DocumentOp::BackfillIndex` on every cluster node other than
/// this coordinator. Returns `Ok(())` only when every peer reports
/// success. A peer's typed refusal keeps its SQLSTATE, so a duplicate
/// key is `23505` as on the single-node path. A transport fault is
/// `XX000`.
///
/// A cluster with no peers returns `Ok(())` immediately — the
/// coordinator's local dispatch already covered everything.
pub(super) async fn backfill_on_peers(
    state: &SharedState,
    args: PeerBackfill<'_>,
) -> Result<(), DdlError> {
    let plan = PhysicalPlan::Document(DocumentOp::BackfillIndex {
        collection: nodedb_types::QualifiedCollection::new(args.database_id, args.collection),
        path: args.path.to_string(),
        is_array: args.is_array,
        unique: args.unique,
        case_insensitive: args.case_insensitive,
        predicate: args.predicate.map(str::to_string),
    });
    execute_on_peers(state, args.tenant_id, args.database_id, &plan, "backfill").await
}

/// Run `plan` on every active cluster node other than this coordinator,
/// for an index build step this node already ran locally.
///
/// Returns `Ok(())` only when every peer reports success. A peer's typed
/// refusal keeps its SQLSTATE. A transport fault is `XX000`. `what` names
/// the step in every error message. A node with no transport or no peer
/// returns `Ok(())` at once.
async fn execute_on_peers(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    what: &str,
) -> Result<(), DdlError> {
    let peers = active_peers(state);
    if peers.is_empty() {
        return Ok(());
    }
    let call = PeerCall::new(state, tenant_id, database_id, plan, what)?;

    // Fan out in parallel; collect per-peer outcomes. Any failure
    // aborts the commit — we do NOT compensate by dropping the index
    // here because the coordinator still holds the Building entry in
    // the catalog and the caller's error return leaves it that way
    // for a retry (DROP + CREATE) to clear.
    let mut joins = Vec::with_capacity(peers.len());
    for node_id in peers {
        let call = call.clone();
        joins.push(tokio::spawn(async move {
            call.execute(node_id, PEER_BACKFILL_DEADLINE).await
        }));
    }
    for join in joins {
        join.await
            .map_err(|e| DdlError::internal(format!("peer {what} join: {e}")))??;
    }
    Ok(())
}

/// Every active cluster node other than this one. Empty with no transport
/// or no topology: no peer is reachable, and the local dispatch is the only
/// step.
///
/// `all_nodes` includes learners and paused peers. Only `Active` nodes get
/// an index build step: a node mid-join or draining picks the index up
/// when its applier observes the Ready commit and dual-writes catch up the
/// rows it hosts.
pub(super) fn active_peers(state: &SharedState) -> Vec<u64> {
    if state.cluster_transport.is_none() {
        return Vec::new();
    }
    let Some(topology_lock) = state.cluster_topology.as_ref() else {
        return Vec::new();
    };
    // Snapshot under the read lock: the lock is never held across `.await`.
    let topology = topology_lock
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    topology
        .all_nodes()
        .filter(|n| n.state == NodeState::Active && n.node_id != state.node_id)
        .map(|n| n.node_id)
        .collect()
}

/// One index build plan, encoded once, ready to send to any peer.
#[derive(Clone)]
pub(super) struct PeerCall {
    transport: std::sync::Arc<nodedb_cluster::NexarTransport>,
    plan_bytes: Vec<u8>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    what: String,
}

impl PeerCall {
    /// Encode `plan` for every peer. `what` names the step in every error.
    pub(super) fn new(
        state: &SharedState,
        tenant_id: TenantId,
        database_id: DatabaseId,
        plan: &PhysicalPlan,
        what: &str,
    ) -> Result<Self, DdlError> {
        let transport = state.cluster_transport.as_ref().ok_or_else(|| {
            DdlError::internal(format!("{what}: no cluster transport reaches a peer"))
        })?;
        let plan_bytes = plan_wire::encode(plan)
            .map_err(|e| DdlError::internal(format!("{what} plan encode: {e}")))?;
        Ok(Self {
            transport: std::sync::Arc::clone(transport),
            plan_bytes,
            tenant_id,
            database_id,
            trace_id: TraceId::generate(),
            what: what.to_string(),
        })
    }

    /// Run the plan on peer `node_id` with `deadline` for its answer.
    /// Returns the answer's payloads. A peer's typed refusal keeps its
    /// SQLSTATE. A transport fault is `XX000`. A call past `deadline` is
    /// `57014`.
    pub(super) async fn execute(
        &self,
        node_id: u64,
        deadline: Duration,
    ) -> Result<Vec<Vec<u8>>, DdlError> {
        let what = &self.what;
        let req = RaftRpc::ExecuteRequest(ExecuteRequest {
            plan_bytes: self.plan_bytes.clone(),
            tenant_id: self.tenant_id.as_u64(),
            database_id: self.database_id.as_u64(),
            deadline_remaining_ms: u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
            trace_id: self.trace_id.0,
            descriptor_versions: Vec::new(),
            // Index build fan-out is not session-transaction-scoped.
            txn_id: None,
            vshard_id: None,
            read_groups: Vec::new(),
            user_roles: Vec::new(),
        });
        // `deadline` bounds each attempt's response read and the whole call,
        // retries included. The transport's own `rpc_timeout` does not
        // apply.
        let call = self
            .transport
            .send_rpc_with_read_timeout(node_id, req, deadline);
        let resp = tokio::time::timeout(deadline, call)
            .await
            .map_err(|_| {
                DdlError::new(
                    "57014",
                    format!(
                        "peer {what} on node {node_id}: no answer within {}ms",
                        deadline.as_millis()
                    ),
                )
            })?
            .map_err(|e| {
                DdlError::internal(format!("peer {what} transport to node {node_id}: {e}"))
            })?;
        let RaftRpc::ExecuteResponse(resp) = resp else {
            return Err(DdlError::internal(format!(
                "peer {what} on node {node_id}: unexpected RPC variant {resp:?}"
            )));
        };
        if let Some(e) = resp.error {
            return Err(DdlError::from_error_in_context(
                &format!("peer {what} on node {node_id}"),
                &crate::Error::from(e),
            ));
        }
        Ok(resp.payloads)
    }
}
