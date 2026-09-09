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
use nodedb_cluster::{RaftRpc, SubmitCalvinTxnRequest, SubmitCalvinTxnResponse, TypedClusterError};

use crate::Error;
use crate::bridge::envelope::Response;
use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::SharedState;

use super::local::{submit_and_await_calvin, synthetic_returning_response};

/// Backoff schedule (milliseconds) for waiting on the sequencer-group leader
/// election before a cross-shard submit. Covers the brief post-startup window
/// (a fresh single-node cluster elects in a couple of seconds) and short
/// re-election gaps. Bounded: once the schedule is exhausted a genuinely
/// leaderless cluster surfaces a typed error rather than hanging.
const SEQUENCER_LEADER_WAIT_BACKOFF_MS: &[u64] = &[50, 100, 200, 400, 800, 1000, 1000, 1000];

/// Submit a cross-shard Calvin `tx_class`, routing it to the sequencer-group
/// leader so it is actually sequenced and acked.
///
/// Routing logic (mirrors `assign_surrogate_routed`):
/// - **Not cluster mode** (no `cluster_transport` / `cluster_routing`): submit
///   locally — single-node IS the sequencer leader.
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
    tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    // Not cluster mode — single-node is the only sequencer member, hence the
    // leader. Submit-and-await locally.
    let (Some(transport), Some(_routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return submit_and_await_calvin(state, tx_class).await;
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

    // Leader is self: submit-and-await locally (a self-RPC would be a pointless
    // extra hop and the local registry is the one that completes).
    if leader == state.node_id {
        return submit_and_await_calvin(state, tx_class).await;
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

    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    let req = SubmitCalvinTxnRequest {
        tx_class_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };

    // The leader-side handler holds this RPC open until the transaction is
    // sequenced AND completion-acked (up to `deadline_remaining_ms`). The generic
    // short `rpc_timeout` (a normal request/response round-trip budget) would
    // abort the call long before that, so bound the response read by the
    // forwarded deadline plus a margin for the round-trip itself.
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));
    match transport
        .send_rpc_with_read_timeout(leader, RaftRpc::SubmitCalvinTxnRequest(req), read_timeout)
        .await
    {
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: None,
            payload_bytes,
        })) => {
            // The leader drained ITS local sidecar and forwarded the RETURNING
            // payload bytes over this non-Raft RPC response. Reconstruct a
            // minimal Control-Plane Response carrying just that payload so the
            // coordinator emits DATA-ROW output; `None` for plain writes.
            Ok(payload_bytes.map(synthetic_returning_response))
        }
        // A Data-Plane verdict from the sequencer leader keeps its code, so a
        // constraint violation on a routed write reaches the client as its own
        // SQLSTATE instead of a generic internal error.
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: Some(TypedClusterError::DataPlane { code }),
            ..
        })) => Err(Error::DataPlane(code.into())),
        // A constraint refusal on the sequencer leader keeps its kind, so a
        // NOT NULL refusal on a routed write reaches the client as 23502
        // instead of collapsing into a generic internal error.
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error:
                Some(TypedClusterError::RejectedConstraint {
                    collection,
                    constraint,
                    detail,
                }),
            ..
        })) => Err(Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        }),
        Ok(RaftRpc::SubmitCalvinTxnResponse(SubmitCalvinTxnResponse {
            error: Some(e), ..
        })) => Err(Error::Internal {
            detail: format!("calvin-submit failed on sequencer leader node {leader}: {e:?}"),
        }),
        Ok(other) => Err(Error::Internal {
            detail: format!("calvin-submit: unexpected reply from node {leader}: {other:?}"),
        }),
        Err(e) => Err(Error::Internal {
            detail: format!("calvin-submit RPC to sequencer leader node {leader} failed: {e}"),
        }),
    }
}
