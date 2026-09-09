// SPDX-License-Identifier: BUSL-1.1

//! Remote-node dispatch helpers for RESTORE TENANT.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::wire as plan_wire;

pub(super) const NODE_RESTORE_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) async fn dispatch_remote(
    state: &Arc<SharedState>,
    node_id: u64,
    tenant_id: u64,
    plan: PhysicalPlan,
) -> Result<(), Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("restore: cluster_transport unavailable but node {node_id} is remote"),
        })?;
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("restore: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: nodedb_types::id::DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: NODE_RESTORE_TIMEOUT.as_millis() as u64,
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
        // Restore dispatch is not session-transaction-scoped.
        txn_id: None,
    });
    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("restore RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse { success: true, .. }) => Ok(()),
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("restore: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "restore: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

pub(super) fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("restore node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("restore node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("restore node {node_id}: routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "restore node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
        // Keep the shard's verdict typed: a restore refused by the Data Plane
        // must not read as a generic internal restore fault.
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
    }
}
