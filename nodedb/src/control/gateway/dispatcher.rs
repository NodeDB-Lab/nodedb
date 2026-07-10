// SPDX-License-Identifier: BUSL-1.1

//! Per-route dispatch: local SPSC or remote `ExecuteRequest` RPC.
//!
//! The dispatcher takes a single [`TaskRoute`] and executes it:
//!
//! - `RouteDecision::Local` → dispatch through the SPSC bridge via
//!   [`dispatch_to_data_plane_with_txn`] (threading the owning transaction so a
//!   local-leader in-transaction read resolves its staging overlay).
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

use futures::StreamExt;
use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc, TypedClusterError};
use tracing::debug;

use crate::Error;
use crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn;
use crate::control::server::result_stream::{ResultStream, RowBatch};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::wire as plan_wire;

use super::route::{RouteDecision, TaskRoute};
use super::version_set::GatewayVersionSet;

/// Parameters for [`dispatch_route`] (bundled to stay within clippy's
/// `too_many_arguments` limit once the `txn_id` thread was added).
///
/// `tenant_id` — the authenticated tenant for this query.
/// `trace_id` — distributed trace ID propagated from the client request.
/// `txn_id` — owning interactive transaction, threaded to the **local** hop so
///   the leaseholder resolves this transaction's staging overlay
///   (read-your-own-writes). The remote `ExecuteRequest` path cannot yet carry
///   it (tracked cross-node in-transaction gap), so a remote route drops it.
/// `deadline_ms` — remaining deadline in milliseconds.
/// `version_set` — descriptor versions for the collections touched by the plan.
pub struct DispatchRouteParams<'a> {
    pub route: TaskRoute,
    pub shared: &'a Arc<SharedState>,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub txn_id: Option<TxnId>,
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
}

/// Dispatch a single route and return the raw payload bytes.
pub async fn dispatch_route(params: DispatchRouteParams<'_>) -> Result<Vec<Vec<u8>>, Error> {
    let DispatchRouteParams {
        route,
        shared,
        tenant_id,
        database_id,
        trace_id,
        txn_id,
        deadline_ms,
        version_set,
    } = params;
    match route.decision {
        RouteDecision::Local => {
            dispatch_local(route, shared, tenant_id, database_id, trace_id, txn_id).await
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
    txn_id: Option<TxnId>,
) -> Result<Vec<Vec<u8>>, Error> {
    let vshard_id = VShardId::new(route.vshard_id);
    let resp = dispatch_to_data_plane_with_txn(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        route.plan,
        trace_id,
        txn_id,
    )
    .await?;
    Ok(vec![resp.payload.to_vec()])
}

/// Arguments for a remote dispatch call (bundles the 8 parameters to stay
/// within clippy's `too_many_arguments` limit).
struct RemoteDispatchArgs<'a> {
    plan: nodedb_physical::physical_plan::PhysicalPlan,
    shared: &'a Arc<SharedState>,
    node_id: u64,
    vshard_id: u64,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
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
        database_id,
        trace_id,
        deadline_ms,
        version_set,
    } = args;
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote dispatch".into(),
    })?;

    // Resolve any Exchange data-movement nodes BEFORE shipping to the remote
    // node. A Data-Plane core rejects any plan still containing an Exchange, so
    // the coordinator must gather/embed cross-node data here — symmetric with
    // the local path (`dispatch_local` → `dispatch_to_data_plane_with_source`,
    // which already resolves). A self-contained plan (no Exchange) is a no-op.
    // `resolve_exchange_in_plan` is identity-free; catalog materialization is
    // already done upstream on the pgwire/native paths that own the identity.
    // (`Box::pin` breaks the async-recursion cycle: resolving a Broadcast build
    // side calls `gather_all_vshards` → `gateway.execute` → routing → here.)
    // Cluster remote-dispatch: no session-transaction context crosses this
    // boundary yet, so `None`. TRACKED: cross-node in-transaction reads are a
    // known gap (see resolve/exchange.rs).
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
        None,
    ))
    .await?
    {
        // A root-level Gather resolved entirely at the coordinator — its merged
        // response is ready; return it instead of shipping anything.
        crate::control::server::exchange::Resolved::Gathered(resp, _shard_watermarks) => {
            return Ok(vec![resp.payload.to_vec()]);
        }
        crate::control::server::exchange::Resolved::Plan(p) => p,
        // Gateway path returns collected bytes: materialize the stream into one
        // merged-array payload. (Single-node streaming never reaches the gateway
        // — `state.gateway.is_none()` gates the Stream branch — but handle it
        // exhaustively and behaviour-preservingly regardless.)
        crate::control::server::exchange::Resolved::Stream(s) => {
            let (merged, _lsn) = crate::control::server::result_stream::materialize(s).await?;
            return Ok(vec![merged]);
        }
    };

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
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteRequest to remote node"
    );

    let resp_rpc = transport.send_rpc(node_id, req).await.map_err(|e| {
        // Transport failure means the target node is unreachable —
        // we do NOT know who the new leader is. Use leader_node = 0
        // so the retry loop does NOT re-entrench the unreachable node
        // as leader in the routing table. The next retry will route
        // locally (leader == 0 → local) and let the local Raft state
        // resolve to the actual leader.
        Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("node-{node_id} (transport error: {e})"),
        }
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

/// Remote streaming dispatch via the multi-frame `ExecuteStreamRequest` RPC.
///
/// Returns a [`ResultStream`] that yields the remote shard's row batches as
/// they arrive over QUIC, interleaved by the caller's `select_all` with any
/// local routes.
///
/// ## Retry-vs-stream split (critical)
///
/// Leader resolution and the FIRST frame are obtained EAGERLY here: the bidi
/// stream is opened and the first stream item is pulled inside this function.
/// A terminal error that arrives BEFORE any row (`NotLeader`,
/// `DescriptorMismatch`, transport failure on open) is mapped via
/// [`map_typed_cluster_error`] to a retryable [`Error`] and propagated to the
/// gateway's existing not-leader retry loop. Once at least one chunk has been
/// observed, any subsequent error is TERMINAL — it is surfaced as a stream
/// `Err` and never retried (re-running the plan would duplicate the rows
/// already streamed to the client).
///
/// The returned stream re-emits the buffered first batch followed by the rest.
async fn dispatch_remote_stream(args: RemoteDispatchArgs<'_>) -> Result<ResultStream, Error> {
    let RemoteDispatchArgs {
        plan,
        shared,
        node_id,
        vshard_id,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
    } = args;
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote stream dispatch".into(),
    })?;

    // Resolve Exchange nodes before shipping (symmetric with `dispatch_remote`).
    // No session-transaction context crosses this boundary yet, so `None`.
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
        None,
    ))
    .await?
    {
        crate::control::server::exchange::Resolved::Plan(p) => p,
        // A streamable child whose Exchange resolved at the coordinator into a
        // ready response/stream — re-emit it as a single-batch / forwarded
        // stream. These do not occur for the streamable-scan plans routed here,
        // but handle exhaustively and behaviour-preservingly.
        crate::control::server::exchange::Resolved::Gathered(resp, _shard_watermarks) => {
            let batch = RowBatch {
                payload: resp.payload.to_vec(),
                watermark_lsn: resp.watermark_lsn,
            };
            return Ok(Box::pin(futures::stream::once(async move { Ok(batch) })));
        }
        crate::control::server::exchange::Resolved::Stream(s) => return Ok(s),
    };

    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("gateway: plan encode failed: {e}"),
    })?;

    let descriptor_versions: Vec<nodedb_cluster::rpc_codec::DescriptorVersionEntry> = version_set
        .iter()
        .map(
            |(name, version)| nodedb_cluster::rpc_codec::DescriptorVersionEntry {
                collection: name.clone(),
                version: *version,
            },
        )
        .collect();

    let req = RaftRpc::ExecuteStreamRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteStreamRequest to remote node"
    );

    // Open the stream eagerly. A failure to even open / send the request is a
    // pre-row condition: map it like a transport failure in `dispatch_remote`
    // so the retry loop routes elsewhere on the next attempt.
    let stream = transport
        .send_rpc_stream(node_id, req)
        .await
        .map_err(|e| Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("node-{node_id} (stream open error: {e})"),
        })?;
    // The `async_stream` body is `!Unpin`; pin it on the heap so we can pull
    // the eager first frame and then keep the tail around for `.chain`.
    let mut stream = Box::pin(stream);

    // Eagerly pull the FIRST frame so a pre-row terminal error is catchable and
    // retryable. Any error here is a pre-row error.
    let first = match stream.next().await {
        Some(Ok((payload, lsn))) => RowBatch {
            payload,
            watermark_lsn: Lsn::new(lsn),
        },
        Some(Err(e)) => return Err(map_stream_cluster_error(e, vshard_id)),
        // Clean EOF with zero rows: a valid empty result. Return an empty stream.
        None => return Ok(Box::pin(futures::stream::empty())),
    };

    // Build the result stream: re-emit the buffered first batch, then forward
    // the rest. Errors past the first frame are TERMINAL — surfaced as stream
    // `Err`, never retried.
    let rest = stream.map(move |item| match item {
        Ok((payload, lsn)) => Ok(RowBatch {
            payload,
            watermark_lsn: Lsn::new(lsn),
        }),
        Err(e) => Err(Error::Dispatch {
            detail: format!("remote stream terminal error: {e}"),
        }),
    });

    let head = futures::stream::once(async move { Ok(first) });
    Ok(Box::pin(head.chain(rest)))
}

/// Map a pre-row [`nodedb_cluster::ClusterError`] from a streaming dispatch to a
/// retryable internal [`Error`].
///
/// A `StreamTerminal` carrying a typed `NotLeader` / `DescriptorMismatch` maps
/// through the same [`map_typed_cluster_error`] used by the one-shot path so the
/// gateway retry loop handles it identically. Any other cluster error becomes a
/// transport-style `NotLeader` (leader_node = 0) so the next attempt re-resolves
/// routing rather than re-entrenching an unreachable node.
fn map_stream_cluster_error(err: nodedb_cluster::ClusterError, vshard_id: u64) -> Error {
    match err {
        nodedb_cluster::ClusterError::StreamTerminal { error, .. } => {
            map_typed_cluster_error(error, vshard_id)
        }
        other => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("stream dispatch error: {other}"),
        },
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
