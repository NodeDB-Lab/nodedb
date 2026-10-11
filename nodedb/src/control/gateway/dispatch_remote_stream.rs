// SPDX-License-Identifier: BUSL-1.1

//! Remote streaming dispatch: run a plan on a remote node via the
//! multi-frame `ExecuteStreamRequest` RPC. Used by
//! [`super::dispatcher::dispatch_route_stream`].

use futures::StreamExt;
use nodedb_cluster::ClusterError;
use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc};
use nodedb_physical::physical_plan::wire as plan_wire;
use tracing::debug;

use crate::Error;
use crate::control::server::result_stream::{ResultStream, RowBatch};
use crate::types::{Lsn, VShardId};

use super::cluster_error::map_typed_cluster_error;
use super::dispatch_remote::{RemoteDispatchArgs, scoped_vshard};

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
