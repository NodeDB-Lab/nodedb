// SPDX-License-Identifier: BUSL-1.1

//! Reservation submit/release primitives and sequencer-leader routing.
//!
//! Mirrors [`super::submit::submit_local_assign`] /
//! [`super::submit::submit_calvin_routed_assign`] exactly: reservation requests
//! (reserve-read admission, release-on-commit/abort) only complete correctly on
//! the sequencer-group leader, since the reservation service only assigns on
//! the leader and drains-and-discards elsewhere. The local primitives here
//! MUST run on the leader; the routed entry points resolve the leader and
//! either run locally (this node IS the leader) or forward via a one-shot RPC.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's / leader's Control Plane (Tokio). The QUIC
//! `send_rpc` call is Control-Plane I/O, allowed here. This module never does
//! storage I/O or io_uring directly.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::types::{LockKeyWire, ReleaseReason, TxnIdWire};
use nodedb_cluster::{
    ClusterError, RaftRpc, ReleaseReservationRequest, ReleaseReservationResponse,
    ReserveReadRequest, ReserveReadResponse,
};

use crate::Error;
use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::state::SharedState;

/// Submit a reserve-read to THIS node's reservation inbox and await the
/// assigned owner, bounded by `timeout`.
///
/// PRECONDITION: this node is the sequencer-group leader (its service
/// assigns). Callers that are not the leader MUST route via
/// [`submit_reserve_read`].
///
/// `pub(crate)` so [`crate::control::server::reservation::hooks::RegistryReserveRead`]
/// can call it after decoding the wire bytes.
pub(crate) async fn submit_local_reserve_read(
    state: &SharedState,
    key: LockKeyWire,
    vshard: u32,
    owner: Option<TxnIdWire>,
    timeout: Duration,
) -> crate::Result<TxnIdWire> {
    let inbox = state
        .reservation_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    let rx = inbox
        .submit_reserve(key, vshard, owner)
        .map_err(|e| Error::BadRequest {
            detail: format!("reservation service rejected reserve-read: {e}"),
        })?;

    tokio::time::timeout(timeout, rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for reservation assignment".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "reservation assignment channel closed".to_owned(),
        })
}

/// Submit a reserve-read, routing it to the sequencer-group leader.
///
/// Routing logic mirrors [`super::submit::submit_calvin_routed_assign`]
/// exactly:
/// - **No `cluster_transport`**: this `SharedState` never ran `start_raft`,
///   so no sequencer runs here. Return `SequencerUnavailable`.
/// - **Leader is self**: submit locally.
/// - **No leader elected (0 / none)**: return a typed error — never submit
///   locally, since a non-leader submit is silently discarded.
/// - **Leader is a remote node**: register the leader's address from the live
///   topology, then send one `ReserveReadRequest` (carrying the
///   msgpack-encoded key + owner); the leader runs the local submit and
///   replies with the assigned owner.
pub(crate) async fn submit_reserve_read(
    state: &SharedState,
    key: LockKeyWire,
    vshard: u32,
    owner: Option<TxnIdWire>,
) -> crate::Result<TxnIdWire> {
    // Every running server has a cluster transport, the synthesized one-node
    // cluster included. Without one, `start_raft` never ran here.
    let Some(transport) = state.cluster_transport.as_ref() else {
        return Err(Error::SequencerUnavailable);
    };

    // Resolve the sequencer-group leader from THIS node's live Raft status.
    let status_fn = state.raft_status_fn.get().ok_or_else(|| Error::Internal {
        detail: "reserve-read: raft status fn not installed (cluster not started)".to_owned(),
    })?;
    let leader = status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);

    // `0` = no sequencer leader elected yet. We must NOT submit locally: a
    // non-leader submit is drained and discarded by the local reservation
    // service.
    if leader == 0 {
        return Err(Error::Internal {
            detail: "no sequencer leader elected yet; cannot reserve read".to_owned(),
        });
    }

    // Leader is self: submit locally (a self-RPC will be a pointless extra
    // hop and the local inbox is the one that gets the assignment).
    if leader == state.node_id {
        let local_timeout = super::submit::budget::statement_budget(state)?;
        return submit_local_reserve_read(state, key, vshard, owner, local_timeout).await;
    }

    // Remote leader: ensure its address is registered before dispatch, then
    // send the one-shot RPC carrying the msgpack-encoded key + owner.
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let lock_key_bytes = zerompk::to_msgpack_vec(&key).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode LockKeyWire for routed reserve-read: {e}"),
    })?;
    let owner_bytes = owner
        .map(|o| {
            zerompk::to_msgpack_vec(&o).map_err(|e| Error::Serialization {
                format: "msgpack".to_owned(),
                detail: format!("failed to encode TxnIdWire owner for routed reserve-read: {e}"),
            })
        })
        .transpose()?;

    // The leader works on the reservation only as long as the statement waits.
    let deadline_remaining_ms = super::submit::budget::statement_budget_ms(state)?;
    let req = ReserveReadRequest {
        lock_key_bytes,
        vshard,
        owner_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };

    // The leader-side handler holds this RPC open until the reservation is
    // assigned (up to `deadline_remaining_ms`). The generic short `rpc_timeout`
    // will abort the call long before that, so bound the response read by the
    // forwarded deadline plus a margin for the round-trip itself.
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));
    match transport
        .send_rpc_with_read_timeout(leader, RaftRpc::ReserveReadRequest(req), read_timeout)
        .await
    {
        Ok(RaftRpc::ReserveReadResponse(ReserveReadResponse {
            owner_bytes: Some(b),
            error: None,
        })) => zerompk::from_msgpack::<TxnIdWire>(&b).map_err(|e| Error::Serialization {
            format: "msgpack".to_owned(),
            detail: format!("failed to decode TxnIdWire owner from reserve-read reply: {e}"),
        }),
        Ok(RaftRpc::ReserveReadResponse(ReserveReadResponse { error: Some(e), .. })) => {
            Err(Error::Internal {
                detail: format!("reserve-read failed on sequencer leader node {leader}: {e:?}"),
            })
        }
        Ok(other) => Err(Error::Internal {
            detail: format!("reserve-read: unexpected reply from node {leader}: {other:?}"),
        }),
        // The request went out and its answer was lost. The leader can have
        // granted it, so its outcome is unknown. Lease GC reaps a lost grant.
        Err(ClusterError::Unanswered { .. }) => Err(Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        }),
        Err(e) => Err(Error::Internal {
            detail: format!("reserve-read RPC to sequencer leader node {leader} failed: {e}"),
        }),
    }
}

/// Submit a release to THIS node's reservation inbox.
///
/// PRECONDITION: this node is the sequencer-group leader. Callers that are
/// not the leader MUST route via [`release_reservation`].
///
/// Fire-and-forget enqueue: a dropped release is backstopped by lease GC, so
/// this does not await any completion signal.
///
/// `pub(crate)` so [`crate::control::server::reservation::hooks::RegistryReleaseReservation`]
/// can call it after decoding the wire bytes.
pub(crate) async fn submit_local_release(
    state: &SharedState,
    owner: TxnIdWire,
    vshard: u32,
    reason: ReleaseReason,
) -> crate::Result<()> {
    state
        .reservation_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?
        .submit_release(owner, vshard, reason)
        .map_err(|e| Error::BadRequest {
            detail: format!("reservation release rejected: {e}"),
        })?;
    Ok(())
}

/// Submit a release, routing it to the sequencer-group leader.
///
/// Same 3-way skeleton as [`submit_reserve_read`], but a failed release must
/// NEVER propagate as an error to the caller: releases run on the
/// commit/abort path, and lease GC backstops any release that doesn't land
/// (leader not yet elected, RPC failure, etc.) by reaping the lease after it
/// expires. Failures are logged, not returned.
pub(crate) async fn release_reservation(
    state: &SharedState,
    owner: TxnIdWire,
    vshard: u32,
    reason: ReleaseReason,
) -> crate::Result<()> {
    // Without a cluster transport `start_raft` never ran here, so no
    // reservation this release can name was ever granted.
    let Some(transport) = state.cluster_transport.as_ref() else {
        return Err(Error::SequencerUnavailable);
    };

    let status_fn = match state.raft_status_fn.get() {
        Some(f) => f,
        None => {
            tracing::warn!(
                "release-reservation: raft status fn not installed; leaving release to lease GC"
            );
            return Ok(());
        }
    };
    let leader = status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);

    // No leader elected: nothing we can do — lease GC will reap the
    // reservation once it expires. Releases must not fail the commit/abort
    // path, so return Ok rather than an error.
    if leader == 0 {
        tracing::warn!(
            "release-reservation: no sequencer leader elected yet; leaving release to lease GC"
        );
        return Ok(());
    }

    if leader == state.node_id {
        return submit_local_release(state, owner, vshard, reason).await;
    }

    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let owner_bytes = match zerompk::to_msgpack_vec(&owner) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                "release-reservation: failed to encode TxnIdWire owner: {e}; leaving release to \
                 lease GC"
            );
            return Ok(());
        }
    };
    let reason_bytes = match zerompk::to_msgpack_vec(&reason) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                "release-reservation: failed to encode ReleaseReason: {e}; leaving release to \
                 lease GC"
            );
            return Ok(());
        }
    };

    // A release runs on the abort path too, often after the statement's
    // deadline passed. It frees a lock other statements wait on, so it takes
    // the node default, never what is left of the statement.
    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    let req = ReleaseReservationRequest {
        owner_bytes,
        vshard,
        reason_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));

    match transport
        .send_rpc_with_read_timeout(
            leader,
            RaftRpc::ReleaseReservationRequest(req),
            read_timeout,
        )
        .await
    {
        Ok(RaftRpc::ReleaseReservationResponse(ReleaseReservationResponse { error: None })) => {
            Ok(())
        }
        Ok(RaftRpc::ReleaseReservationResponse(ReleaseReservationResponse { error: Some(e) })) => {
            tracing::warn!(
                "release-reservation failed on sequencer leader node {leader}: {e:?}; leaving \
                 release to lease GC"
            );
            Ok(())
        }
        Ok(other) => {
            tracing::warn!(
                "release-reservation: unexpected reply from node {leader}: {other:?}; leaving \
                 release to lease GC"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                "release-reservation RPC to sequencer leader node {leader} failed: {e}; leaving \
                 release to lease GC"
            );
            Ok(())
        }
    }
}
