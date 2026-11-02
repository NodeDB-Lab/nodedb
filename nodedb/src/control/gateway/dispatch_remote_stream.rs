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
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::result_stream::{ResultStream, RowBatch};
use crate::types::{Lsn, VShardId};

use super::cluster_error::{RemoteSend, map_typed_cluster_error, reply_wait_expired, send_error};
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
/// The remote runs the stream within the statement's whole remaining budget.
/// The open and the first frame wait at most `reply_wait` of that budget. A
/// dead node fails the open at the QUIC idle timeout, and a read retries.
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
        user_roles,
    } = args;
    // The statement's deadline, fixed before any wait of this attempt.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(deadline_ms);
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
                read_vshards: resp.read_vshards,
            };
            return Ok(Box::pin(futures::stream::once(async move { Ok(batch) })));
        }
        crate::control::server::exchange::Resolved::Stream(s) => return Ok(s),
    };

    // A read can be sent again after a lost answer. Any other plan can have
    // run on the target, so a lost answer is an unknown outcome.
    let send = RemoteSend {
        node_id,
        vshard_id,
        resend_safe: required_permission(&plan) == Permission::Read,
        deadline,
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
        // The remote bounds the whole stream with the statement's remainder.
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
        txn_id,
        vshard_id: scoped_vshard,
        read_groups,
        user_roles,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteStreamRequest to remote node"
    );

    // Open the stream and pull the FIRST frame eagerly, within the reply
    // wait, so a pre-row error is catchable and retryable. Any error here is
    // a pre-row error.
    let reply_wait = nodedb_cluster::rpc_codec::reply_wait(deadline_ms);
    let opened = tokio::time::timeout(reply_wait, async move {
        let stream = transport
            .send_rpc_stream(node_id, req)
            .await
            .map_err(|e| send_error(e, &send))?;
        // The `async_stream` body is `!Unpin`; pin it on the heap so the tail
        // stays usable for `.chain` after the first frame.
        let mut stream = Box::pin(stream);
        let first = stream.next().await;
        Ok::<_, Error>((stream, first))
    })
    .await;
    let (stream, first) = match opened {
        Ok(opened) => opened?,
        // The wait was the statement's whole budget: no retry has time left.
        Err(_) => return Err(reply_wait_expired(&send, reply_wait)),
    };
    let first = match first {
        Some(Ok((payload, lsn))) => RowBatch {
            payload,
            watermark_lsn: Lsn::new(lsn),
            // The `ExecuteStream` wire chunk carries only the watermark; no
            // read vShard is threaded on this remote path.
            read_vshards: crate::types::ReadShards::new(),
        },
        Some(Err(e)) => {
            return Err(map_stream_cluster_error(e, &send));
        }
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
            read_vshards: crate::types::ReadShards::new(),
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
/// loop handles it identically. A Data-Plane verdict keeps its code. A lost
/// answer goes through [`send_error`]: a read retries, any other plan is an
/// unknown outcome. Any other cluster error becomes a transport-style
/// `NotLeader` (leader_node = 0), so the next attempt re-resolves routing
/// rather than re-entrenching an unreachable node.
fn map_stream_cluster_error(err: ClusterError, send: &RemoteSend) -> Error {
    let vshard_id = send.vshard_id;
    match err {
        ClusterError::StreamTerminal { error, .. } | ClusterError::ShardExecution { error, .. } => {
            map_typed_cluster_error(*error, vshard_id)
        }
        // A verdict from a shard that answered. Retrying it on another route
        // repeats it, so it keeps its SQLSTATE.
        ClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        // The request can have run on the remote node.
        lost if lost.is_lost_answer() => send_error(lost, send),
        other @ (ClusterError::Raft(_)
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationInProgress { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Transport { .. }
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
        | ClusterError::RemoteUntyped { .. }
        // The guards above take these. They stay named for exhaustiveness.
        | ClusterError::ShardTimeout { .. }
        | ClusterError::Unanswered { .. }) => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: 0,
            leader_addr: format!("stream dispatch error: {other}"),
            leader_term: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send(resend_safe: bool) -> RemoteSend {
        RemoteSend {
            node_id: 2,
            vshard_id: 7,
            resend_safe,
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        }
    }

    fn unanswered() -> ClusterError {
        ClusterError::Unanswered {
            node_id: 2,
            detail: "stream reset".into(),
        }
    }

    /// A read whose stream answer was lost retries through the leader lookup.
    #[test]
    fn a_lost_stream_answer_to_a_read_retries() {
        assert!(matches!(
            map_stream_cluster_error(unanswered(), &send(true)),
            Error::NotLeader { leader_node: 0, .. }
        ));
    }

    /// Any other plan can have run when its stream answer was lost, so it
    /// is never sent again.
    #[test]
    fn a_lost_stream_answer_to_a_non_read_is_an_unknown_outcome() {
        assert!(matches!(
            map_stream_cluster_error(unanswered(), &send(false)),
            Error::DeadlineExceeded { .. }
        ));
        let timed_out = ClusterError::ShardTimeout {
            vshard_id: 7,
            elapsed_ms: 1_000,
        };
        assert!(matches!(
            map_stream_cluster_error(timed_out, &send(false)),
            Error::DeadlineExceeded { .. }
        ));
    }

    /// A transport error before any row retries, read or not.
    #[test]
    fn a_pre_row_transport_error_retries() {
        let unsent = ClusterError::Transport {
            detail: "connect refused".into(),
        };
        assert!(matches!(
            map_stream_cluster_error(unsent, &send(false)),
            Error::NotLeader { leader_node: 0, .. }
        ));
    }
}
