// SPDX-License-Identifier: BUSL-1.1

//! Host-crate hooks for requests routed to a vShard or sequencer-group leader.
//!
//! Covers surrogate assignment, Calvin submit, Calvin inbox submit, and the
//! reserve-read and release-reservation pair. Re-exported through [`super::hooks`].

/// Hook for routed-surrogate-exchange.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the assign logic
/// for the keys the request carries lives in `nodedb` behind this
/// `Send + Sync` hook.
/// The transport read-loop calls [`on_assign_surrogate`](Self::on_assign_surrogate)
/// when an `AssignSurrogateRequest` arrives at the home vShard's LEADER and writes
/// the returned [`AssignSurrogateResponse`](crate::rpc_codec::AssignSurrogateResponse)
/// back to the coordinator.
///
/// Because the handler runs on the home node (the vShard leader), a LOCAL assign
/// yields the AUTHORITATIVE surrogate: the first call allocates it and every later
/// call for the same key returns the same value (idempotent, first-wins). The
/// coordinator routes here precisely so the value it carries is the one the home
/// node will store under.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; an `AssignSurrogate`
/// request against a node with no assigner installed returns a typed "not
/// configured" error.
///
/// The hook is **async**: the host-crate implementation proposes the batch's
/// unbound keys through the home vShard's Raft log and awaits the apply. It
/// works to the request's `deadline_remaining_ms` and never touches io_uring
/// or the Data Plane directly.
#[async_trait::async_trait]
pub trait AssignRemoteSurrogate: Send + Sync + 'static {
    /// Assign or return the authoritative surrogate of every key `req`
    /// carries. Returns an [`AssignSurrogateResponse`] with one answer per key
    /// on success, or a typed error on failure. Never a silent drop.
    async fn on_assign_surrogate(
        &self,
        req: crate::rpc_codec::AssignSurrogateRequest,
    ) -> crate::rpc_codec::AssignSurrogateResponse;
}

/// Hook for routed Calvin-submit.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the submit logic —
/// decode the `TxClass`, submit it to THIS node's Calvin sequencer inbox, and
/// await assignment + completion through the node-local `CalvinCompletionRegistry`
/// — lives in `nodedb` behind this `Send + Sync` hook. The transport read-loop
/// calls [`on_submit_calvin_txn`](Self::on_submit_calvin_txn) when a
/// `SubmitCalvinTxnRequest` arrives at the SEQUENCER-GROUP leader and writes the
/// returned [`SubmitCalvinTxnResponse`](crate::rpc_codec::SubmitCalvinTxnResponse)
/// back to the coordinator.
///
/// Because the handler runs on the sequencer-group leader, the submit-and-await
/// is correct: only the leader's sequencer service assigns transactions
/// (`note_assigned`), and only the leader's registry receives BOTH the
/// assignment and the replicated completion ack. The coordinator routes here
/// precisely so the submit lands where it will actually be sequenced and acked.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `SubmitCalvinTxn`
/// request against a node with no Calvin-submit hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the submit-and-await blocks on the assignment
/// and completion oneshot channels (bounded by the request deadline) on the
/// Tokio transport reactor. The actual transaction execution happens on the Data
/// Plane via the sequencer service / per-vshard schedulers; this hook never
/// touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait CalvinSubmit: Send + Sync + 'static {
    /// Submit the `TxClass` carried by `req` (msgpack-encoded) to this node's
    /// Calvin sequencer inbox and await its completion. Returns a
    /// [`SubmitCalvinTxnResponse`](crate::rpc_codec::SubmitCalvinTxnResponse)
    /// with `error: None` on commit or a typed error on failure (never a silent
    /// drop).
    async fn on_submit_calvin_txn(
        &self,
        req: crate::rpc_codec::SubmitCalvinTxnRequest,
    ) -> crate::rpc_codec::SubmitCalvinTxnResponse;
}

/// Hook for routed Calvin-INBOX submit.
///
/// OLLP dependent sibling of [`CalvinSubmit`]: `nodedb-cluster` cannot depend on
/// `nodedb` (circular), so the submit logic — decode the `TxClass`, submit it to
/// THIS node's Calvin sequencer inbox, and await only the ASSIGNMENT (NOT
/// completion) through the node-local `CalvinCompletionRegistry` — lives in
/// `nodedb` behind this `Send + Sync` hook. The transport read-loop calls
/// [`on_submit_calvin_inbox`](Self::on_submit_calvin_inbox) when a
/// `SubmitCalvinInboxRequest` arrives at the SEQUENCER-GROUP leader and writes the
/// returned [`SubmitCalvinInboxResponse`](crate::rpc_codec::SubmitCalvinInboxResponse)
/// back to the coordinator.
///
/// Because the handler runs on the sequencer-group leader, the submit-and-assign
/// is correct: only the leader's sequencer service assigns transactions
/// (`note_assigned`). Unlike [`CalvinSubmit`] it returns AS SOON AS the
/// assignment is observed — the OLLP coordinator loop drives the dependent
/// transaction to completion itself, so this hook must NOT block
/// until completion.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `SubmitCalvinInbox`
/// request against a node with no Calvin-inbox hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the submit-and-assign blocks on the assignment
/// oneshot channel (bounded by the request deadline) on the Tokio transport
/// reactor. The actual transaction execution happens on the Data Plane via the
/// sequencer service / per-vshard schedulers; this hook never touches io_uring or
/// storage directly.
#[async_trait::async_trait]
pub trait CalvinSubmitInbox: Send + Sync + 'static {
    /// Submit the `TxClass` carried by `req` (msgpack-encoded) to this node's
    /// Calvin sequencer inbox and await its ASSIGNMENT (not completion). Returns
    /// a [`SubmitCalvinInboxResponse`](crate::rpc_codec::SubmitCalvinInboxResponse)
    /// with `error: None` carrying the assignment on success or a typed error on
    /// failure (never a silent drop).
    async fn on_submit_calvin_inbox(
        &self,
        req: crate::rpc_codec::SubmitCalvinInboxRequest,
    ) -> crate::rpc_codec::SubmitCalvinInboxResponse;

    /// Offer a batch of a multi-part transaction's streamed parts to this
    /// node's sequencer leader queue, and answer how far the stream got.
    async fn on_calvin_parts(
        &self,
        req: crate::rpc_codec::CalvinPartsRequest,
    ) -> crate::rpc_codec::CalvinPartsResponse;
}

/// Hook for routed reserve-read (Calvin OLLP).
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the reserve
/// logic — decode the `LockKey` and assign-only reserve the read lock through
/// THIS node's Calvin sequencer scheduler — lives in `nodedb` behind this
/// `Send + Sync` hook. The transport read-loop calls
/// [`on_reserve_read`](Self::on_reserve_read) when a `ReserveReadRequest`
/// arrives at the SEQUENCER-GROUP leader and writes the returned
/// [`ReserveReadResponse`](crate::rpc_codec::ReserveReadResponse) back to the
/// coordinator.
///
/// Because the handler runs on the sequencer-group leader, the reserve is
/// correct: only the leader's scheduler holds the authoritative lock table for
/// its local sequencer inbox. The coordinator routes here precisely so the
/// reservation lands where it will actually be enforced.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ReserveRead`
/// request against a node with no reserve-read hook installed returns a typed
/// "not configured" error.
///
/// The hook is **async** for signature symmetry with the other one-shot
/// hooks; the reserve itself is bounded by the request deadline on the Tokio
/// transport reactor. It never touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait ReserveRead: Send + Sync + 'static {
    /// Assign-only reserve the read lock for the `LockKey` carried by `req`.
    /// Returns a [`ReserveReadResponse`](crate::rpc_codec::ReserveReadResponse)
    /// with the minted (or confirmed) owner on success or a typed error on
    /// failure (never a silent drop).
    async fn on_reserve_read(
        &self,
        req: crate::rpc_codec::ReserveReadRequest,
    ) -> crate::rpc_codec::ReserveReadResponse;
}

/// Hook for routed release-reservation (Calvin OLLP).
///
/// Ack-only sibling of [`ReserveRead`]: `nodedb-cluster` cannot depend on
/// `nodedb` (circular), so the release logic — decode the owner and release
/// reason, and release the reservation through THIS node's Calvin sequencer
/// scheduler — lives in `nodedb` behind this `Send + Sync` hook. The transport
/// read-loop calls [`on_release_reservation`](Self::on_release_reservation)
/// when a `ReleaseReservationRequest` arrives at the SEQUENCER-GROUP leader
/// and writes the returned
/// [`ReleaseReservationResponse`](crate::rpc_codec::ReleaseReservationResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a
/// `ReleaseReservation` request against a node with no release-reservation
/// hook installed returns a typed "not configured" error.
///
/// The hook is **async** for signature symmetry with the other one-shot
/// hooks; the release itself is bounded by the request deadline on the Tokio
/// transport reactor. It never touches io_uring or storage directly.
#[async_trait::async_trait]
pub trait ReleaseReservation: Send + Sync + 'static {
    /// Release the reservation held by the owner carried by `req`. Returns a
    /// [`ReleaseReservationResponse`](crate::rpc_codec::ReleaseReservationResponse)
    /// with `error: None` on success (ack) or a typed error on failure (never
    /// a silent drop).
    async fn on_release_reservation(
        &self,
        req: crate::rpc_codec::ReleaseReservationRequest,
    ) -> crate::rpc_codec::ReleaseReservationResponse;
}
