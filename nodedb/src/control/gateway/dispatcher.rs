//! Per-route dispatch: local SPSC or remote `ExecuteRequest` RPC.
//!
//! The dispatcher takes a single [`TaskRoute`] and executes it:
//!
//! - `RouteDecision::Local` → dispatch through the SPSC bridge via
//!   [`dispatch_to_data_plane`].
//! - `RouteDecision::Remote { node_id, .. }` → encode the plan as
//!   [`ExecuteRequest`] bytes and send via [`NexarTransport::send_rpc`].
//! - `RouteDecision::Broadcast { .. }` → each individual route in the
//!   broadcast list is already split into Local/Remote routes by the router,
//!   so by the time dispatch runs, each element is a concrete Local or Remote.
//!
//! Returns `Vec<u8>` payloads — raw Data Plane response bytes that the fuser
//! can merge.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc, TypedClusterError};
use tracing::debug;

use crate::Error;
use crate::bridge::physical_plan::wire as plan_wire;
use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

use super::route::{RouteDecision, TaskRoute};
use super::version_set::GatewayVersionSet;

/// Dispatch a single route and return the raw payload bytes.
///
/// `tenant_id` — the authenticated tenant for this query.
/// `trace_id` — distributed trace ID propagated from the client request.
/// `deadline_ms` — remaining deadline in milliseconds.
/// `version_set` — descriptor versions for the collections touched by the plan.
pub async fn dispatch_route(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    trace_id: u64,
    deadline_ms: u64,
    version_set: &GatewayVersionSet,
) -> Result<Vec<Vec<u8>>, Error> {
    match route.decision {
        RouteDecision::Local => dispatch_local(route, shared, tenant_id, trace_id).await,
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                trace_id,
                deadline_ms,
                version_set,
            })
            .await
        }
        RouteDecision::Broadcast { .. } => {
            // Broadcast routes are split into individual Local/Remote routes
            // by the router before dispatch. This arm should not be reached.
            Err(Error::Internal {
                detail: "dispatcher: Broadcast route reached dispatch — should have been split"
                    .into(),
            })
        }
    }
}

/// Local dispatch via SPSC bridge.
async fn dispatch_local(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    trace_id: u64,
) -> Result<Vec<Vec<u8>>, Error> {
    let vshard_id = VShardId::new(route.vshard_id);
    let resp = dispatch_to_data_plane(shared, tenant_id, vshard_id, route.plan, trace_id).await?;
    Ok(vec![resp.payload.to_vec()])
}

/// Arguments for a remote dispatch call (bundles the 8 parameters to stay
/// within clippy's `too_many_arguments` limit).
struct RemoteDispatchArgs<'a> {
    plan: crate::bridge::physical_plan::PhysicalPlan,
    shared: &'a Arc<SharedState>,
    node_id: u64,
    vshard_id: u64,
    tenant_id: TenantId,
    trace_id: u64,
    deadline_ms: u64,
    version_set: &'a GatewayVersionSet,
}

/// Remote dispatch via `ExecuteRequest` RPC.
async fn dispatch_remote(args: RemoteDispatchArgs<'_>) -> Result<Vec<Vec<u8>>, Error> {
    let RemoteDispatchArgs {
        plan,
        shared,
        node_id,
        vshard_id,
        tenant_id,
        trace_id,
        deadline_ms,
        version_set,
    } = args;
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote dispatch".into(),
    })?;

    // Encode the plan.
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("gateway: plan encode failed: {e}"),
    })?;

    // Build descriptor version entries.
    let descriptor_versions: Vec<nodedb_cluster::rpc_codec::DescriptorVersionEntry> = version_set
        .iter()
        .map(
            |(name, version)| nodedb_cluster::rpc_codec::DescriptorVersionEntry {
                collection: name.clone(),
                version: *version,
            },
        )
        .collect();

    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u32(),
        deadline_remaining_ms: deadline_ms,
        trace_id,
        descriptor_versions,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u32(),
        "gateway: dispatching ExecuteRequest to remote node"
    );

    let resp_rpc = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::NotLeader {
            vshard_id: VShardId::new(vshard_id.min(u16::MAX as u64) as u16),
            leader_node: node_id,
            leader_addr: format!("node-{node_id} (transport error: {e})"),
        })?;

    match resp_rpc {
        RaftRpc::ExecuteResponse(resp) => {
            if let Some(err) = resp.error {
                Err(map_typed_cluster_error(err, vshard_id))
            } else {
                Ok(resp.payloads)
            }
        }
        other => Err(Error::Internal {
            detail: format!("gateway: unexpected RPC response variant: {other:?}"),
        }),
    }
}

/// Map a [`TypedClusterError`] to an internal [`Error`].
///
/// `NotLeader` is mapped such that the gateway retry loop can extract the
/// hinted leader from `Error::NotLeader.leader_node` and update the routing
/// table before the next attempt.
fn map_typed_cluster_error(err: TypedClusterError, vshard_id: u64) -> Error {
    match err {
        TypedClusterError::NotLeader {
            leader_node_id,
            leader_addr,
            ..
        } => Error::NotLeader {
            vshard_id: VShardId::new(vshard_id.min(u16::MAX as u64) as u16),
            leader_node: leader_node_id.unwrap_or(0),
            leader_addr: leader_addr.unwrap_or_default(),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::RetryableSchemaChanged {
            descriptor: collection,
        },
        TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        },
        TypedClusterError::Internal { message, .. } => Error::Internal { detail: message },
    }
}

/// Build the deadline_remaining_ms value from the server's default.
pub fn default_deadline_ms(shared: &SharedState) -> u64 {
    Duration::from_secs(shared.tuning.network.default_deadline_secs).as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::rpc_codec::TypedClusterError;

    #[test]
    fn map_not_leader() {
        let err = TypedClusterError::NotLeader {
            group_id: 0,
            leader_node_id: Some(5),
            leader_addr: Some("10.0.0.5:9400".into()),
            term: 3,
        };
        match map_typed_cluster_error(err, 7) {
            Error::NotLeader { leader_node, .. } => assert_eq!(leader_node, 5),
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn map_descriptor_mismatch() {
        let err = TypedClusterError::DescriptorMismatch {
            collection: "orders".into(),
            expected_version: 1,
            actual_version: 2,
        };
        match map_typed_cluster_error(err, 0) {
            Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "orders"),
            other => panic!("expected RetryableSchemaChanged, got {other:?}"),
        }
    }

    #[test]
    fn map_deadline_exceeded() {
        let err = TypedClusterError::DeadlineExceeded { elapsed_ms: 100 };
        assert!(matches!(
            map_typed_cluster_error(err, 0),
            Error::DeadlineExceeded { .. }
        ));
    }
}
