// SPDX-License-Identifier: BUSL-1.1

//! Remote dispatch: send a plan to a remote node via `ExecuteRequest` RPC.
//!
//! Split out of `dispatcher.rs` to keep that file under the project's
//! 500-line limit. Holds the one-shot (`dispatch_remote`) and streaming
//! (`dispatch_remote_stream`) remote dispatch paths used by
//! [`super::dispatcher::dispatch_route`] and
//! [`super::dispatcher::dispatch_route_stream`] respectively.

use std::sync::Arc;

use futures::StreamExt;
use nodedb_cluster::ClusterError;
use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc};
use tracing::debug;

use crate::Error;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::result_stream::{ResultStream, RowBatch};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::wire as plan_wire;

use super::cluster_error::map_typed_cluster_error;
use super::dispatcher::DispatchOutcome;
use super::version_set::GatewayVersionSet;

/// Arguments for a remote dispatch call (bundles the parameters to stay
/// within clippy's `too_many_arguments` limit).
pub(super) struct RemoteDispatchArgs<'a> {
    pub plan: nodedb_physical::physical_plan::PhysicalPlan,
    pub shared: &'a Arc<SharedState>,
    pub node_id: u64,
    pub vshard_id: u64,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
    /// Session-transaction context forwarded to the remote executor, or `None`
    /// for non-transactional dispatch.
    pub txn_id: Option<TxnId>,
    /// The route is a leg of a linearizable read. Exchange nodes resolved here
    /// before the plan ships inherit it.
    pub linearizable: bool,
    /// Groups the remote node confirms before it reads. Empty for a write or
    /// a weaker read.
    pub read_groups: Vec<u64>,
}

/// Remote dispatch via `ExecuteRequest` RPC.
pub(super) async fn dispatch_remote(
    args: RemoteDispatchArgs<'_>,
) -> Result<DispatchOutcome, Error> {
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
        txn_id,
        linearizable,
        read_groups,
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
    let scope = crate::control::server::exchange::ReadScope {
        database_id,
        tenant_id,
        trace_id,
        txn_id: None,
        linearizable,
    };
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared, plan, scope,
    ))
    .await?
    {
        // A root-level Gather resolved entirely at the coordinator — its merged
        // response is ready; return it instead of shipping anything.
        crate::control::server::exchange::Resolved::Gathered(
            resp,
            shard_watermarks,
            _shuffle_reads,
        ) => {
            return Ok(DispatchOutcome {
                payloads: vec![resp.payload.to_vec()],
                shard_watermarks,
                // Coordinator-gathered at the exchange root: the gather folded
                // the read versions across the responding shards and stamped
                // them on this response, so carry them through. This route did
                // not observe a version of its own to report.
                read_versions: resp.read_versions,
                not_found: false,
            });
        }
        crate::control::server::exchange::Resolved::Plan(p) => *p,
        // Gateway path returns collected bytes: materialize the stream into one
        // merged-array payload. Key the collected watermark to the collection's
        // owning vShard this route dispatched to.
        crate::control::server::exchange::Resolved::Stream(s) => {
            let (merged, lsn) = crate::control::server::result_stream::materialize(s).await?;
            return Ok(DispatchOutcome {
                payloads: vec![merged],
                shard_watermarks: vec![(VShardId::new(vshard_id as u32), lsn)],
                // A materialized stream reports no read versions: its frames
                // carry per-batch watermarks only. Empty is honest here rather
                // than lossy: the streaming branch is gated on
                // `txn_id.is_none()` (`resolve/exchange.rs`), so a stream never
                // serves an in-transaction read and no read-set entry consumes
                // this value.
                read_versions: crate::types::ReadVersions::new(),
                not_found: false,
            });
        }
    };

    // A read can be sent again after a lost answer. Any other plan can have
    // run on the target, so a lost answer is an unknown outcome.
    let resend_safe = required_permission(&plan) == Permission::Read;

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

    let scoped_vshard = scoped_vshard(&plan, vshard_id)?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
        txn_id,
        vshard_id: scoped_vshard,
        read_groups,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteRequest to remote node"
    );

    // The remote handler works within `deadline_ms`: a write can wait at the
    // leader's gate or in a Calvin queue for that long. The reply wait follows
    // that budget, never the transport's fixed RPC timeout.
    let reply_wait = nodedb_cluster::rpc_codec::reply_wait(deadline_ms);
    let resp_rpc = match tokio::time::timeout(
        reply_wait,
        transport.send_rpc_with_read_timeout(node_id, req, reply_wait),
    )
    .await
    {
        Ok(answer) => answer.map_err(|e| send_error(e, resend_safe, node_id, vshard_id))?,
        Err(_) => {
            return Err(send_error(
                ClusterError::ShardTimeout {
                    vshard_id: (vshard_id % VShardId::COUNT as u64) as u32,
                    elapsed_ms: u64::try_from(reply_wait.as_millis()).unwrap_or(u64::MAX),
                },
                resend_safe,
                node_id,
                vshard_id,
            ));
        }
    };

    match resp_rpc {
        RaftRpc::ExecuteResponse(resp) => {
            // A read that found no row reads back like a local miss: an
            // empty answer that keeps the `NotFound` verdict and the versions
            // the read observed. Every other error crosses as its typed error.
            let not_found = match resp.error {
                None => false,
                Some(err) => match map_typed_cluster_error(err, vshard_id) {
                    Error::DataPlane(crate::bridge::envelope::ErrorCode::NotFound) => true,
                    error => return Err(error),
                },
            };
            // Key the remote's read watermark to the collection's owning
            // vShard this route routed to (mirroring `dispatch_local`), NOT
            // the `vshard_id % COUNT` retry hint — the read-set validator
            // expects the true owning vShard as the entry key.
            Ok(DispatchOutcome {
                shard_watermarks: vec![(
                    VShardId::new(vshard_id as u32),
                    Lsn::new(resp.watermark_lsn),
                )],
                payloads: resp.payloads,
                read_versions: crate::types::ReadVersions::from_wire(&resp.read_versions),
                not_found,
            })
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
/// `Err` and never retried (re-running the plan duplicates the rows
/// already streamed to the client).
///
/// The returned stream re-emits the buffered first batch followed by the rest.
pub(super) async fn dispatch_remote_stream(
    args: RemoteDispatchArgs<'_>,
) -> Result<ResultStream, Error> {
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
        txn_id,
        linearizable,
        read_groups,
    } = args;
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote stream dispatch".into(),
    })?;

    // Resolve Exchange nodes before shipping (symmetric with `dispatch_remote`).
    // No session-transaction context crosses this boundary yet, so `None`.
    let scope = crate::control::server::exchange::ReadScope {
        database_id,
        tenant_id,
        trace_id,
        txn_id: None,
        linearizable,
    };
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared, plan, scope,
    ))
    .await?
    {
        crate::control::server::exchange::Resolved::Plan(p) => *p,
        // A streamable child whose Exchange resolved at the coordinator into a
        // ready response/stream — re-emit it as a single-batch / forwarded
        // stream. These do not occur for the streamable-scan plans routed here,
        // but handle exhaustively and behaviour-preservingly.
        crate::control::server::exchange::Resolved::Gathered(
            resp,
            _shard_watermarks,
            _shuffle_reads,
        ) => {
            let batch = RowBatch {
                payload: resp.payload.to_vec(),
                watermark_lsn: resp.watermark_lsn,
                read_versions: resp.read_versions,
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

    let scoped_vshard = scoped_vshard(&plan, vshard_id)?;
    let req = RaftRpc::ExecuteStreamRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
        txn_id,
        vshard_id: scoped_vshard,
        read_groups,
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
            leader_term: 0,
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
            // The `ExecuteStream` wire chunk carries only the watermark; no
            // read version is threaded on this remote path.
            read_versions: crate::types::ReadVersions::new(),
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
            read_versions: crate::types::ReadVersions::new(),
        }),
        Err(e) => Err(Error::Dispatch {
            detail: format!("remote stream terminal error: {e}"),
        }),
    });

    let head = futures::stream::once(async move { Ok(first) });
    Ok(Box::pin(head.chain(rest)))
}

/// Map a pre-row [`nodedb_cluster::ClusterError`] from a streaming dispatch to
/// an [`Error`].
///
/// A typed error (`StreamTerminal`, `ShardExecution`) maps through the same
/// [`map_typed_cluster_error`] used by the one-shot path, so the gateway retry
/// loop handles it identically. A Data-Plane verdict keeps its code. Any other
/// cluster error becomes a transport-style `NotLeader` (leader_node = 0), so
/// the next attempt re-resolves routing rather than re-entrenching an
/// unreachable node.
fn map_stream_cluster_error(err: ClusterError, vshard_id: u64) -> Error {
    match err {
        ClusterError::StreamTerminal { error, .. } | ClusterError::ShardExecution { error, .. } => {
            map_typed_cluster_error(*error, vshard_id)
        }
        // A verdict from a shard that answered. Retrying it on another route
        // repeats it, so it keeps its SQLSTATE.
        ClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        other @ (ClusterError::Raft(_)
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationInProgress { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Transport { .. }
        | ClusterError::ShardTimeout { .. }
        | ClusterError::Unanswered { .. }
        | ClusterError::Storage { .. }
        | ClusterError::Codec { .. }
        | ClusterError::UnsupportedWireVersion { .. }
        | ClusterError::CircuitOpen { .. }
        | ClusterError::JoinGroupDisappeared { .. }
        | ClusterError::JoinCommitTimeout { .. }
        | ClusterError::ReadIndexNotLeader { .. }
        | ClusterError::ReadIndexTimeout { .. }
        | ClusterError::Config { .. }
        | ClusterError::MigrationCheckpoint(_)
        | ClusterError::MigrationRecovery(_)
        | ClusterError::WrongOwner { .. }
        | ClusterError::Calvin(_)
        | ClusterError::SnapshotCrcMismatch { .. }
        | ClusterError::SnapshotOffsetRegression { .. }
        | ClusterError::PartialSnapshotCorrupt { .. }
        | ClusterError::PartialSnapshotCleanupFailed { .. }
        | ClusterError::SnapshotApplyFailed { .. }
        | ClusterError::Mirror(_)
        | ClusterError::BspBarrier(_)
        | ClusterError::VectorGather(_)
        | ClusterError::SpatialGather(_)
        | ClusterError::Bm25Gather(_)
        | ClusterError::TsGather(_)
        | ClusterError::ShufflePush(_)
        | ClusterError::RemoteUntyped { .. }) => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("stream dispatch error: {other}"),
            leader_term: 0,
        },
    }
}

/// The vShard a remote receiver must run `plan` on, or `None` when the plan
/// fans across the receiver's local cores.
///
/// A transaction meta-op names no collection, so only the route's own vShard
/// can pick the core that holds the transaction's staging overlay.
fn scoped_vshard(
    plan: &nodedb_physical::physical_plan::PhysicalPlan,
    vshard_id: u64,
) -> Result<Option<VShardId>, Error> {
    if !super::router::is_task_vshard_scoped(plan) {
        return Ok(None);
    }
    let raw = u32::try_from(vshard_id)
        .ok()
        .filter(|raw| *raw < VShardId::COUNT)
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "gateway: vShard-scoped plan routed to out-of-range vShard {vshard_id}"
            ),
        })?;
    Ok(Some(VShardId::new(raw)))
}

/// The error of a failed `ExecuteRequest` send.
///
/// A request the target never received, and a lost answer to a read, retry as
/// `NotLeader` with `leader_node = 0`. The unreachable node is then not kept as
/// leader, and the next try routes locally to the real leader. A lost answer
/// to any other plan is an unknown outcome: the plan can have run, and a
/// second send would run it again.
fn send_error(e: ClusterError, resend_safe: bool, node_id: u64, vshard_id: u64) -> Error {
    let outcome_unknown = matches!(
        e,
        ClusterError::Unanswered { .. } | ClusterError::ShardTimeout { .. }
    );
    if outcome_unknown && !resend_safe {
        tracing::warn!(
            node_id,
            vshard_id,
            error = %e,
            "gateway: a remote execute reached its deadline with no answer"
        );
        return Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        };
    }
    Error::NotLeader {
        vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
        leader_node: 0,
        leader_addr: format!("node-{node_id} (transport error: {e})"),
        leader_term: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

    fn unanswered() -> ClusterError {
        ClusterError::Unanswered {
            node_id: 2,
            detail: "stream reset".into(),
        }
    }

    /// A write whose answer was lost can have applied: it is never sent again.
    #[test]
    fn a_lost_answer_to_a_write_is_an_unknown_outcome() {
        assert!(matches!(
            send_error(unanswered(), false, 2, 7),
            Error::DeadlineExceeded { .. }
        ));
    }

    /// A read whose answer was lost retries through the leader lookup.
    #[test]
    fn a_lost_answer_to_a_read_retries() {
        assert!(matches!(
            send_error(unanswered(), true, 2, 7),
            Error::NotLeader { leader_node: 0, .. }
        ));
    }

    /// A request that never reached the target retries, read or write.
    #[test]
    fn an_unsent_request_retries() {
        let unsent = ClusterError::Transport {
            detail: "connect refused".into(),
        };
        assert!(matches!(
            send_error(unsent, false, 2, 7),
            Error::NotLeader { leader_node: 0, .. }
        ));
    }

    #[test]
    fn a_transaction_meta_op_carries_its_route_vshard() {
        let plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
            txn_id: TxnId::new(4),
        });
        assert_eq!(scoped_vshard(&plan, 77).unwrap(), Some(VShardId::new(77)));
    }

    #[test]
    fn a_fanned_plan_carries_no_vshard() {
        let plan = PhysicalPlan::Meta(MetaOp::Checkpoint);
        assert_eq!(scoped_vshard(&plan, 77).unwrap(), None);
    }

    #[test]
    fn an_out_of_range_vshard_is_refused() {
        let plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
            txn_id: TxnId::new(4),
        });
        assert!(scoped_vshard(&plan, u64::from(VShardId::COUNT)).is_err());
    }
}
