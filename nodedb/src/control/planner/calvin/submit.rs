// SPDX-License-Identifier: BUSL-1.1

//! Calvin submit-and-await primitive and sequencer-leader routing (Cv1).
//!
//! NodeDB's Calvin cross-shard write path only completes when the transaction is
//! submitted on the SEQUENCER-GROUP leader:
//!
//! - the sequencer SERVICE assigns transactions (`note_assigned`) ONLY on the
//!   `SEQUENCER_GROUP_ID` leader — a non-leader's sequencer service drains and
//!   DISCARDS its inbox;
//! - the replicated `CompletionAck` is applied on ALL sequencer-group members,
//!   so every member's `CalvinCompletionRegistry` receives `note_completion_ack`.
//!
//! The consequence: a submit-and-await is correct ONLY on the leader, whose
//! local registry receives BOTH the assignment and the completion ack. A submit
//! on a non-leader is silently lost and the caller times out at the ASSIGNMENT
//! phase.
//!
//! [`submit_and_await_calvin`] is the local primitive — it MUST run on the
//! sequencer leader. [`submit_calvin_routed`] is the entry point every
//! coordinator calls: it resolves the sequencer leader and either runs the
//! submit-and-await locally (this node IS the leader) or forwards the `TxClass`
//! to the leader via a one-shot RPC (`SubmitCalvinTxn`), mirroring the routed
//! surrogate-exchange path exactly.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's / leader's Control Plane (Tokio). The QUIC
//! `send_rpc` call is Control-Plane I/O, allowed here. The actual transaction
//! execution happens on the Data Plane via the sequencer service / per-vshard
//! schedulers; this module never does storage I/O or io_uring directly.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::calvin::{AttemptOutcome, SEQUENCER_GROUP_ID, TxnId};
use nodedb_cluster::{
    RaftRpc, SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse,
};

use crate::Error;
use crate::bridge::envelope::Response;
use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::{CalvinApplyResult, SharedState};

/// Build a minimal Control-Plane [`Response`] carrying only the RETURNING
/// `payload` bytes forwarded over the cross-node routed-submit RPC.
///
/// The coordinator only reads `.payload` (and derives the plan kind from the
/// task) when shaping RETURNING rows, so the other fields are placeholders: the
/// authoritative status/LSN already lived on the leader that applied the txn.
fn synthetic_returning_response(payload_bytes: Vec<u8>) -> Response {
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};

    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload_bytes),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        write_set: Vec::new(),
    }
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await completion.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns;
/// its registry receives the replicated completion ack). Callers that are not
/// the leader MUST route via [`submit_calvin_routed`].
///
/// The assignment + completion waits are bounded by
/// `state.tuning.network.default_deadline_secs`.
pub async fn submit_and_await_calvin(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    submit_and_await_calvin_with_timeout(state, tx_class, timeout).await
}

/// [`submit_and_await_calvin`] with an explicit deadline budget.
///
/// Used by the leader-side RPC handler so the forwarded submit-and-await is
/// bounded by the coordinator's remaining deadline rather than this node's full
/// default deadline.
pub async fn submit_and_await_calvin_with_timeout(
    state: &SharedState,
    tx_class: TxClass,
    timeout: Duration,
) -> crate::Result<Option<Response>> {
    let inbox = state
        .sequencer_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
        detail: format!("Calvin sequencer rejected transaction: {e}"),
    })?;

    let assignment_rx = registry.register_submission(inbox_seq);
    let (epoch, position, participants) = tokio::time::timeout(timeout, assignment_rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for Calvin sequencer assignment".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin sequencer assignment channel closed".to_owned(),
        })?;

    let completion_rx = registry.register_completion(TxnId::new(epoch, position), participants);
    let outcome = tokio::time::timeout(timeout, completion_rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for Calvin transaction completion".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin completion channel closed".to_owned(),
        })?;
    // Terminal, NON-retryable: the scheduler rejected the transaction's local
    // plan routing and broadcast `TxnRoutingFailed`. Surface it immediately —
    // falling through to the RETURNING-drain below would silently report
    // `Ok(None)` for a transaction that never applied.
    if let AttemptOutcome::Failed { detail } = &outcome {
        return Err(Error::Internal {
            detail: format!("calvin transaction routing failed: {detail}"),
        });
    }
    // Terminal, NON-retryable: the global cross-shard OCC verdict was ABORT
    // (read-set validation failed) and the writes were dropped. This is a
    // fall-through chain, NOT a match — without this explicit check `Aborted`
    // would fall through to the RETURNING drain below and silently return
    // `Ok(None)`, reporting COMMIT SUCCESS for a transaction that never applied.
    // Surface it as a serialization failure (SQLSTATE 40001) so the client
    // retries the whole transaction.
    if outcome == AttemptOutcome::Aborted {
        return Err(Error::CalvinSerializationConflict);
    }
    // The static (non-dependent) Calvin path never produces an OLLP mismatch —
    // `note_ollp_mismatch` only fires on the dependent-predicate retry path — so
    // this branch is unreachable at runtime today. It is kept as a typed error
    // (never a panic) so any future mismatch signal on this channel surfaces
    // deterministically instead of crashing.
    if outcome == AttemptOutcome::Mismatch {
        return Err(Error::Internal {
            detail: "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
        });
    }

    // Completion fired: the scheduler deposited the applied Response (with any
    // RETURNING rows) into the sidecar BEFORE proposing the ack that woke this
    // waiter, so the entry is present now if this write carried RETURNING.
    // Drain it (removing the entry) and hand it back so the coordinator can emit
    // DATA-ROW output instead of a bare command tag. `None` for plain writes; a
    // `Conflict` (>1 RETURNING participant) fails loudly rather than returning a
    // partial cross-shard union.
    let drained = state
        .calvin_apply_results
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&TxnId::new(epoch, position));
    match drained {
        Some(CalvinApplyResult::Single { response, .. }) => Ok(Some(response)),
        Some(CalvinApplyResult::Conflict) => Err(Error::Internal {
            detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
        }),
        None => Ok(None),
    }
}

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
/// - **No leader elected (0 / none)**: return a typed error — never submit
///   locally, since a non-leader submit is silently discarded.
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
    let leader = status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);

    // `0` = no sequencer leader elected yet. We must NOT submit locally: a
    // non-leader submit is drained and discarded by the local sequencer service,
    // so the caller would time out at the assignment phase. Surface a typed error
    // (same divergence-safety contract as the routed-surrogate leader==0 path) so
    // the caller retries once an election resolves.
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

/// The sequencer ASSIGNMENT for a submitted dependent (OLLP) `TxClass`.
///
/// Returned by [`submit_calvin_routed_assign`] AS SOON AS the sequencer assigns
/// the transaction — completion is NOT awaited (the OLLP coordinator loop drives
/// the dependent transaction to completion itself in a later unit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutedAssignment {
    pub inbox_seq: u64,
    pub epoch: u64,
    pub position: u32,
    pub participants: usize,
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await only its
/// ASSIGNMENT (NOT completion), bounded by `timeout`.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns).
/// Callers that are not the leader MUST route via
/// [`submit_calvin_routed_assign`]. The local primitive for the OLLP dependent
/// path — the sibling of [`submit_and_await_calvin_with_timeout`] that stops at
/// the assignment phase.
///
/// `pub(crate)` so [`crate::control::server::calvin_submit::inbox_hook`] can
/// call it after decoding the wire bytes, mirroring how `hook.rs` delegates to
/// `submit_and_await_calvin_with_timeout`.
pub(crate) async fn submit_local_assign(
    state: &SharedState,
    tx_class: TxClass,
    timeout: Duration,
) -> crate::Result<RoutedAssignment> {
    let inbox = state
        .sequencer_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
        detail: format!("Calvin sequencer rejected transaction: {e}"),
    })?;

    let assignment_rx = registry.register_submission(inbox_seq);
    let (epoch, position, participants) = tokio::time::timeout(timeout, assignment_rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for Calvin sequencer assignment".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin sequencer assignment channel closed".to_owned(),
        })?;

    Ok(RoutedAssignment {
        inbox_seq,
        epoch,
        position,
        participants,
    })
}

/// Submit a cross-shard dependent (OLLP) Calvin `tx_class`, routing it to the
/// sequencer-group leader, and return its ASSIGNMENT immediately — WITHOUT
/// awaiting completion.
///
/// The OLLP dependent sibling of [`submit_calvin_routed`]. Routing logic mirrors
/// it exactly:
/// - **Not cluster mode** (no `cluster_transport` / `cluster_routing`) OR
///   **leader is self**: submit-and-assign locally — single-node / this node IS
///   the sequencer leader.
/// - **No leader elected (0 / none)**: return a typed error — never submit
///   locally, since a non-leader submit is silently discarded.
/// - **Leader is a remote node**: register the leader's address from the live
///   topology, then send one `SubmitCalvinInboxRequest` (carrying the
///   msgpack-encoded `TxClass`); the leader runs the submit-and-assign and
///   replies with the assignment. Map transport / leader errors to a typed
///   `crate::Error`.
pub async fn submit_calvin_routed_assign(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<RoutedAssignment> {
    let local_timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);

    // Not cluster mode — single-node is the only sequencer member, hence the
    // leader. Submit-and-assign locally.
    let (Some(transport), Some(_routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return submit_local_assign(state, tx_class, local_timeout).await;
    };

    // Resolve the sequencer-group leader from THIS node's live Raft status.
    let status_fn = state.raft_status_fn.get().ok_or_else(|| Error::Internal {
        detail: "calvin-inbox: raft status fn not installed (cluster not started)".to_owned(),
    })?;
    let leader = status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);

    // `0` = no sequencer leader elected yet. We must NOT submit locally: a
    // non-leader submit is drained and discarded by the local sequencer service.
    if leader == 0 {
        return Err(Error::Internal {
            detail: "calvin-inbox: no sequencer leader elected yet; cannot submit cross-shard \
                     transaction"
                .to_owned(),
        });
    }

    // Leader is self: submit-and-assign locally (a self-RPC would be a pointless
    // extra hop and the local registry is the one that gets the assignment).
    if leader == state.node_id {
        return submit_local_assign(state, tx_class, local_timeout).await;
    }

    // Remote leader: ensure its address is registered before dispatch, then send
    // the one-shot RPC carrying the msgpack-encoded TxClass.
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let tx_class_bytes = zerompk::to_msgpack_vec(&tx_class).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode TxClass for routed Calvin inbox submit: {e}"),
    })?;

    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    let req = SubmitCalvinInboxRequest {
        tx_class_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };

    // The leader-side handler holds this RPC open until the transaction is
    // assigned (up to `deadline_remaining_ms`). The generic short `rpc_timeout`
    // would abort the call long before that, so bound the response read by the
    // forwarded deadline plus a margin for the round-trip itself.
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));
    match transport
        .send_rpc_with_read_timeout(leader, RaftRpc::SubmitCalvinInboxRequest(req), read_timeout)
        .await
    {
        Ok(RaftRpc::SubmitCalvinInboxResponse(SubmitCalvinInboxResponse {
            inbox_seq,
            epoch,
            position,
            participants,
            error: None,
        })) => Ok(RoutedAssignment {
            inbox_seq,
            epoch,
            position,
            participants: participants as usize,
        }),
        Ok(RaftRpc::SubmitCalvinInboxResponse(SubmitCalvinInboxResponse {
            error: Some(e),
            ..
        })) => Err(Error::Internal {
            detail: format!("calvin-inbox failed on sequencer leader node {leader}: {e:?}"),
        }),
        Ok(other) => Err(Error::Internal {
            detail: format!("calvin-inbox: unexpected reply from node {leader}: {other:?}"),
        }),
        Err(e) => Err(Error::Internal {
            detail: format!("calvin-inbox RPC to sequencer leader node {leader} failed: {e}"),
        }),
    }
}
