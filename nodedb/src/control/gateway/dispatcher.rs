// SPDX-License-Identifier: BUSL-1.1

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

use nodedb_cluster::rpc_codec::TypedClusterError;

use crate::Error;
use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::server::result_stream::ResultStream;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

use super::dispatch_remote::{RemoteDispatchArgs, dispatch_remote, dispatch_remote_stream};
use super::route::{RouteDecision, TaskRoute};
use super::version_set::GatewayVersionSet;

/// Result of dispatching a single route: the raw payload bytes plus the
/// per-shard read watermarks observed while producing them.
///
/// `shard_watermarks` carries one `(vshard, watermark_lsn)` entry per shard that
/// contributed to `payloads` — the local SPSC response's own watermark, or the
/// remote `ExecuteResponse.watermark_lsn` keyed to the collection's owning
/// vShard. The gateway accumulates these across routes so an in-transaction read
/// records one read-set entry per participating shard at its true committed LSN
/// (rather than the former hardcoded `Lsn::ZERO`).
pub struct DispatchOutcome {
    pub payloads: Vec<Vec<u8>>,
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
}

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
    database_id: DatabaseId,
    trace_id: TraceId,
    deadline_ms: u64,
    version_set: &GatewayVersionSet,
) -> Result<DispatchOutcome, Error> {
    match route.decision {
        RouteDecision::Local => {
            dispatch_local(route, shared, tenant_id, database_id, trace_id).await
        }
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                database_id,
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
        RouteDecision::LeaderUnknown { vshard_id } => {
            // Cluster mode with no leader currently known for this vShard.
            // Surface as NotLeader so the gateway retry loop sleeps and
            // re-resolves the routing table on the next attempt — never
            // silently serve from a possibly-stale local replica.
            Err(Error::NotLeader {
                vshard_id: VShardId::new(vshard_id as u32),
                leader_node: 0,
                leader_addr: String::new(),
            })
        }
    }
}

/// Parameters for [`dispatch_route_stream`].
pub struct DispatchRouteStreamParams<'a> {
    pub route: TaskRoute,
    pub shared: &'a Arc<SharedState>,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
}

/// Streaming sibling of [`dispatch_route`]: dispatch a single route and return
/// a [`ResultStream`] of row batches.
///
/// - `Local` → [`gather_all_cores_stream`] over the route's plan (the route is
///   a single-vShard-homed scan answered on this node; fanning to all local
///   cores mirrors the local degenerate of `gather_all_vshards`).
/// - `Remote` → [`dispatch_remote_stream`] (eager first frame + retry split).
/// - `Broadcast` → unreachable (router splits broadcasts before dispatch).
/// - `LeaderUnknown` → `NotLeader` so the gateway retry loop re-resolves.
pub async fn dispatch_route_stream(
    args: DispatchRouteStreamParams<'_>,
) -> Result<ResultStream, Error> {
    let DispatchRouteStreamParams {
        route,
        shared,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
    } = args;
    match route.decision {
        // Cluster gateway route dispatch: no session-transaction context
        // crosses this boundary yet, so `None`. TRACKED: cross-node
        // in-transaction reads are a known gap (see resolve/exchange.rs).
        RouteDecision::Local => crate::control::server::exchange::gather::gather_all_cores_stream(
            shared,
            tenant_id,
            database_id,
            route.plan,
            trace_id,
            None,
        ),
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote_stream(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                database_id,
                trace_id,
                deadline_ms,
                version_set,
            })
            .await
        }
        RouteDecision::Broadcast { .. } => Err(Error::Internal {
            detail: "dispatcher: Broadcast route reached stream dispatch — should have been split"
                .into(),
        }),
        RouteDecision::LeaderUnknown { vshard_id } => Err(Error::NotLeader {
            vshard_id: VShardId::new(vshard_id as u32),
            leader_node: 0,
            leader_addr: String::new(),
        }),
    }
}

/// Local dispatch via SPSC bridge.
async fn dispatch_local(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> Result<DispatchOutcome, Error> {
    let vshard_id = VShardId::new(route.vshard_id);
    let resp = dispatch_to_data_plane(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        route.plan,
        trace_id,
    )
    .await?;
    Ok(DispatchOutcome {
        payloads: vec![resp.payload.to_vec()],
        shard_watermarks: vec![(vshard_id, resp.watermark_lsn)],
    })
}

/// Map a [`TypedClusterError`] to an internal [`Error`].
///
/// `NotLeader` is mapped such that the gateway retry loop can extract the
/// hinted leader from `Error::NotLeader.leader_node` and update the routing
/// table before the next attempt.
pub(super) fn map_typed_cluster_error(err: TypedClusterError, vshard_id: u64) -> Error {
    match err {
        TypedClusterError::NotLeader {
            leader_node_id,
            leader_addr,
            ..
        } => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
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
