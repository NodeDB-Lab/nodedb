// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-leader routing for a static (non-dependent) Calvin submit.
//!
//! Resolves the sequencer-group leader from this node's live Raft status and
//! either runs the local submit-and-await or forwards the `TxClass` to the
//! leader over one `SubmitCalvinTxn` RPC.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::{
    ClusterError, RaftRpc, SubmitCalvinTxnRequest, SubmitCalvinTxnResponse, TypedClusterError,
};

use crate::Error;
use crate::bridge::envelope::Response;
use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::state::SharedState;

use super::local::{submit_prepared_and_await, synthetic_returning_response};
use super::stream::{StreamTarget, stream_parts};

/// Backoff schedule (milliseconds) for waiting on the sequencer-group leader
/// election before a cross-shard submit. Covers the brief post-startup window
/// (a fresh single-node cluster elects in a couple of seconds) and short
/// re-election gaps. Bounded: once the schedule is exhausted a genuinely
/// leaderless cluster surfaces a typed error rather than hanging.
const SEQUENCER_LEADER_WAIT_BACKOFF_MS: &[u64] = &[50, 100, 200, 400, 800, 1000, 1000, 1000];

/// Submit a Calvin write whose tx class holds a write, routed as
/// [`submit_calvin_routed`], and return its answer.
///
/// A committed write always answers. Its primary participant deposits the
/// answer on its own replicas and reports it in its completion ack, which
/// every coordinator reads (see `with_reported_results`). So the affected
/// count reaches the coordinator wherever it runs, even on a node that hosts
/// none of the participants.
pub async fn submit_calvin_routed_write(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<Response> {
    submit_calvin_routed(state, tx_class)
        .await?
        .ok_or_else(|| Error::Internal {
            detail: "a committed Calvin write reported no answer; its primary participant \
                     reports one in its completion ack"
                .to_owned(),
        })
}

/// Submit a cross-shard Calvin `tx_class`, routing it to the sequencer-group
/// leader so it is actually sequenced and acked.
///
/// Routing logic (mirrors `assign_surrogate_routed`):
/// - **No `cluster_transport`**: this `SharedState` never ran `start_raft`,
///   so no sequencer runs here. Return `SequencerUnavailable`.
/// - **Leader is self**: submit-and-await locally.
/// - **Leader is a remote node**: register the leader's address from the live
///   topology, then send one `SubmitCalvinTxnRequest` (carrying the
///   msgpack-encoded `TxClass`); the leader runs the submit-and-await and
///   replies. Map transport / leader errors to a typed `crate::Error`.
/// - **No leader elected (0 / none)**: wait through
///   [`SEQUENCER_LEADER_WAIT_BACKOFF_MS`] for an election, then return a typed
///   error — never submit on a non-leader, since that submit is silently
///   discarded.
pub async fn submit_calvin_routed(
    state: &SharedState,
    mut tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    super::local::raise_metadata_floor(state, &mut tx_class);
    super::local::stamp_incarnations(state, &mut tx_class)?;
    super::unique_claims::stamp_unique_claims(state, &mut tx_class)?;
    let stream = super::parts::split_into_parts(state, &mut tx_class)?;
    // Every running server has a cluster transport, the synthesized one-node
    // cluster included. Without one, `start_raft` never ran here.
    let Some(transport) = state.cluster_transport.as_ref() else {
        return Err(Error::SequencerUnavailable);
    };

    // Resolve the sequencer-group leader from THIS node's live Raft status. The
    // `raft_status_fn` snapshot includes every group hosted on this node,
    // including `SEQUENCER_GROUP_ID`; its `leader_id` is the leader this node
    // currently believes.
    let status_fn = state.raft_status_fn.get().ok_or_else(|| Error::Internal {
        detail: "calvin-submit: raft status fn not installed (cluster not started)".to_owned(),
    })?;

    // `leader_id == 0` means no sequencer leader is elected YET — the brief
    // window right after startup (the client gateway can open before the
    // sequencer group finishes its first election) or during a re-election.
    // Submitting on a non-leader is drained and discarded, so we must not; but
    // `leader == 0` also guarantees NOTHING has been submitted, so waiting for
    // the election to resolve and re-reading is safe and idempotent. Poll with
    // bounded backoff (mirroring the gateway's NotLeader retry) rather than
    // failing the client's very first write on a freshly-ready node; only a
    // genuinely leaderless cluster exhausts the schedule and surfaces the error.
    let mut leader = 0;
    for (attempt, &backoff_ms) in SEQUENCER_LEADER_WAIT_BACKOFF_MS.iter().enumerate() {
        leader = status_fn()
            .into_iter()
            .find(|g| g.group_id == SEQUENCER_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0);
        if leader != 0 {
            break;
        }
        if attempt + 1 < SEQUENCER_LEADER_WAIT_BACKOFF_MS.len() {
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }
    }
    if leader == 0 {
        return Err(Error::Internal {
            detail: "calvin-submit: no sequencer leader elected yet; cannot submit cross-shard \
                     transaction"
                .to_owned(),
        });
    }

    // Leader is self: submit-and-await locally (a self-RPC will be a pointless
    // extra hop and the local registry is the one that completes).
    if leader == state.node_id {
        let timeout = super::budget::statement_budget(state)?;
        return submit_prepared_and_await(state, tx_class, stream, timeout).await;
    }

    // Remote leader: ensure its address is registered before dispatch, then send
    // the one-shot RPC carrying the msgpack-encoded TxClass.
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let tx_class_bytes = zerompk::to_msgpack_vec(&tx_class).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode TxClass for routed Calvin submit: {e}"),
    })?;

    // The leader works on the transaction only as long as the statement waits.
    let deadline_remaining_ms = super::budget::statement_budget_ms(state)?;
    let req = SubmitCalvinTxnRequest {
        tx_class_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };

    // The leader-side handler holds this RPC open until the transaction is
    // sequenced AND completion-acked (up to `deadline_remaining_ms`). The generic
    // short `rpc_timeout` (a normal request/response round-trip budget) will
    // abort the call long before that, so bound the response read by the
    // forwarded deadline plus a margin for the round-trip itself.
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));
    let header = transport.send_rpc_with_read_timeout(
        leader,
        RaftRpc::SubmitCalvinTxnRequest(req),
        read_timeout,
    );
    // The header's reply comes only at completion, so the parts stream
    // beside it. The leader opens the stream when it proposes the header;
    // until then it answers `Unknown`, and the stream waits.
    let reply = match &stream {
        None => header.await,
        Some(stream) => {
            tokio::pin!(header);
            let streaming = stream_parts(state, StreamTarget::Remote(leader), stream, false);
            tokio::pin!(streaming);
            tokio::select! {
                reply = &mut header => reply,
                end = &mut streaming => {
                    end.into_result()?;
                    header.await
                }
            }
        }
    };
    match reply {
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: None,
            payload_bytes,
        })) => {
            // The leader drained ITS local sidecar and forwarded the RETURNING
            // payload bytes over this non-Raft RPC response. Reconstruct a
            // minimal Control-Plane Response carrying only that payload so the
            // coordinator emits DATA-ROW output; `None` for plain writes.
            Ok(payload_bytes.map(synthetic_returning_response))
        }
        // An error with no class keeps the leader in its message.
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: Some(TypedClusterError::Internal { code: 0, message }),
            ..
        })) => Err(Error::Internal {
            detail: format!("calvin-submit failed on sequencer leader node {leader}: {message}"),
        }),
        // Every other error from the sequencer leader is rebuilt as the error
        // a local submit returns: a Calvin abort stays a serialization
        // conflict, a superseded collection stays retryable, and a Data-Plane
        // or constraint verdict keeps its SQLSTATE.
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: Some(e), ..
        })) => Err(Error::from(e)),
        Ok(other) => Err(Error::Internal {
            detail: format!("calvin-submit: unexpected reply from node {leader}: {other:?}"),
        }),
        // The submit went out and its answer was lost. The leader can have
        // sequenced it, so its outcome is unknown.
        Err(error @ ClusterError::Unanswered { .. }) => {
            tracing::warn!(
                leader,
                %error,
                "calvin-submit reached its deadline with no answer from the sequencer leader"
            );
            Err(Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            })
        }
        Err(e) => Err(Error::Internal {
            detail: format!("calvin-submit RPC to sequencer leader node {leader} failed: {e}"),
        }),
    }
}
