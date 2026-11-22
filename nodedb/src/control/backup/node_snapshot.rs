// SPDX-License-Identifier: BUSL-1.1

//! Take one database's tenant snapshot on one source node.
//!
//! This node snapshots over the local SPSC bridge. A remote node receives the
//! plan in a `RaftRpc::ExecuteRequest`. Either way the request names the
//! database, and the Data Plane snapshot covers that database only.

use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::snapshot_tenant_on_local_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::wire as plan_wire;

/// Default per-node snapshot dispatch timeout.
const NODE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Whether `node_id` names this node.
pub(super) fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0
}

/// Snapshot `tenant_id` in `database_id` on every core of this node, with
/// every array cell version when `arrays` is set.
///
/// This node already took the backup's cut, so the local snapshot carries no
/// cut request.
pub(super) async fn snapshot_self(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    arrays: bool,
) -> Result<Vec<u8>, Error> {
    snapshot_tenant_on_local_cores(
        state,
        TenantId::new(tenant_id),
        database_id,
        NODE_SNAPSHOT_TIMEOUT,
        arrays,
    )
    .await
}

/// Snapshot `tenant_id` in `database_id` on the remote node `node_id`.
pub(super) async fn snapshot_remote(
    state: &SharedState,
    node_id: u64,
    tenant_id: u64,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> Result<Vec<u8>, Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("backup: cluster_transport unavailable but node {node_id} is remote"),
        })?;

    let plan_bytes = plan_wire::encode(plan).map_err(|e| Error::Internal {
        detail: format!("backup: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: database_id.as_u64(),
        deadline_remaining_ms: NODE_SNAPSHOT_TIMEOUT.as_millis() as u64,
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
        // Backup snapshot dispatch is not session-transaction-scoped.
        txn_id: None,
        vshard_id: None,
        read_groups: Vec::new(),
        user_roles: Vec::new(),
        stream: crate::control::server::shared::session::forwarded_stream::current_on_wire(),
    });

    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("backup: snapshot RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            mut payloads,
            ..
        }) => {
            // CreateTenantSnapshot returns exactly one payload.
            if payloads.len() != 1 {
                return Err(Error::Internal {
                    detail: format!(
                        "backup: expected 1 payload from node {node_id}, got {}",
                        payloads.len()
                    ),
                });
            }
            Ok(payloads.remove(0))
        }
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("backup: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "backup: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("backup node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("backup node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("backup node {node_id}: snapshot RPC routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "backup node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
        // Keep the shard's verdict typed: a backup snapshot refused by the
        // Data Plane must not read as a generic internal backup fault.
        TypedClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        // A constraint verdict keeps its collection and kind, so the client
        // reads the SQLSTATE the refusing shard meant.
        TypedClusterError::RejectedConstraint {
            collection,
            constraint,
            detail,
        } => Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        },
        // A Calvin abort keeps the error a local submit returns.
        aborted @ TypedClusterError::CalvinAborted { .. } => Error::from(aborted),
    }
}
