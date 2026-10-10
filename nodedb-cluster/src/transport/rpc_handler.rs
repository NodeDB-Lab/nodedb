// SPDX-License-Identifier: BUSL-1.1

//! [`RaftRpcHandler`] — the inbound-RPC dispatch trait the transport server
//! calls for each incoming bidi stream.

use crate::error::Result;
use crate::forward::ChunkSink;
use nodedb_raft::message::TimeoutNowRequest;

use crate::rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, ExecuteRequest, RaftRpc,
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushRequest,
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse, TypedClusterError,
};

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;

    /// Handle a streaming `ExecuteStreamRequest`: execute the plan and push
    /// each result frame to `sink`. Returns `None` on a clean EOF or
    /// `Some(err)` on a terminal failure. The transport writes one
    /// `RPC_EXECUTE_STREAM_END` envelope carrying this outcome after the call
    /// returns. See [`crate::forward::PlanExecutor::execute_plan_streaming`].
    fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> impl std::future::Future<Output = Option<TypedClusterError>> + Send;

    /// Cross-node streaming shuffle (E1) — opening frame of a `ShufflePush`
    /// stream. Lazily creates the receiver inbox for `(shuffle_id, part, side)`
    /// carrying `producer_count` and `num_parts`.
    fn on_shuffle_request(
        &self,
        req: ShufflePushRequest,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Deposit one shuffle chunk payload into the receiver inbox. Bounded —
    /// the implementation blocks while the inbox buffer is full so QUIC flow
    /// control back-pressures the producer.
    fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Terminal frame for one producer of a `ShufflePush` stream: record the
    /// `End` (advancing the per-part build barrier) and capture any terminal
    /// error.
    fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Cross-node shuffle PRODUCER trigger (E4a). Execute the local scan
    /// fragment carried by `req`, hash-partition each output row on `req.keys`,
    /// and fan the rows out to the per-part owners as `ShufflePush` streams
    /// (looping back for parts this node owns). Returns a
    /// [`ShuffleProduceResponse`] whose `error` is `None` on a clean produce or
    /// `Some(err)` if the scan failed (every part has already been `End`ed with
    /// the same error), and whose `read_versions` carry the read versions the
    /// scan observed, one per vShard. The transport writes
    /// exactly one `ShuffleProduceResponse` carrying this outcome back to the
    /// coordinator.
    fn on_shuffle_produce(
        &self,
        req: ShuffleProduceRequest,
    ) -> impl std::future::Future<Output = ShuffleProduceResponse> + Send;

    /// Cross-node shuffle CONSUMER trigger (E4b). Complete the part carried by
    /// `req`: wait for both staged sides of `(shuffle_id, part)` to finalize, run
    /// the node-local grace-hash join over them, and return the joined rows. The
    /// transport writes exactly one `ShuffleConsumeResponse` carrying the rows
    /// (or a typed error) back to the coordinator. Never hangs — the finalize
    /// wait is bounded by `req.deadline_remaining_ms`.
    fn on_shuffle_consume(
        &self,
        req: ShuffleConsumeRequest,
    ) -> impl std::future::Future<Output = ShuffleConsumeResponse> + Send;

    /// Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b). The
    /// single-sided aggregate sibling of [`on_shuffle_consume`](Self::on_shuffle_consume).
    /// Complete the part carried by `req`: wait for the part's single staged
    /// producer side (side 0) of `(shuffle_id, part)` to finalize, merge +
    /// finalize the partial `GroupState`s, and return the aggregate rows. The
    /// transport writes exactly one `ShuffleAggregateConsumeResponse` carrying the
    /// rows (or a typed error) back to the coordinator. Never hangs — the finalize
    /// wait is bounded by `req.deadline_remaining_ms`.
    fn on_shuffle_aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> impl std::future::Future<Output = ShuffleAggregateConsumeResponse> + Send;

    /// Routed-surrogate-exchange (F1b). This node is the LEADER of the home
    /// vShard for the `(collection, pk)` endpoint key carried by `req`:
    /// assign-or-return the AUTHORITATIVE global surrogate (a LOCAL assign on the
    /// leader yields the authoritative, first-wins, idempotent value) and return
    /// it. The transport writes exactly one [`AssignSurrogateResponse`] carrying
    /// the surrogate (or a typed error) back to the coordinator.
    fn on_assign_surrogate(
        &self,
        req: AssignSurrogateRequest,
    ) -> impl std::future::Future<Output = AssignSurrogateResponse> + Send;

    /// Routed Calvin-submit (Cv1). This node is the SEQUENCER-GROUP leader:
    /// decode the `TxClass` carried by `req`, submit it to the local Calvin
    /// sequencer inbox, and await assignment + completion. The transport writes
    /// exactly one [`SubmitCalvinTxnResponse`] carrying success (or a typed
    /// error) back to the coordinator. The await is bounded by
    /// `req.deadline_remaining_ms`.
    fn on_submit_calvin_txn(
        &self,
        req: SubmitCalvinTxnRequest,
    ) -> impl std::future::Future<Output = SubmitCalvinTxnResponse> + Send;

    /// TimeoutNow (leadership transfer). One-way — the receiver immediately
    /// starts an election for the matching group. No reply is written.
    fn on_timeout_now(
        &self,
        req: TimeoutNowRequest,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Routed Calvin-INBOX submit (Cv1). The OLLP dependent sibling of
    /// [`on_submit_calvin_txn`](Self::on_submit_calvin_txn). This node is the
    /// SEQUENCER-GROUP leader: decode the `TxClass` carried by `req`, submit it to
    /// the local Calvin sequencer inbox, and await only the ASSIGNMENT (NOT
    /// completion). The transport writes exactly one [`SubmitCalvinInboxResponse`]
    /// carrying the assignment (or a typed error) back to the coordinator. The
    /// await is bounded by `req.deadline_remaining_ms`.
    fn on_submit_calvin_inbox(
        &self,
        req: SubmitCalvinInboxRequest,
    ) -> impl std::future::Future<Output = SubmitCalvinInboxResponse> + Send;

    /// Routed reserve-read (Calvin OLLP). This node is the SEQUENCER-GROUP
    /// leader: decode the `LockKey` carried by `req` and assign-only reserve
    /// the read lock. The transport writes exactly one [`ReserveReadResponse`]
    /// carrying the minted owner (or a typed error) back to the coordinator.
    /// The reserve is bounded by `req.deadline_remaining_ms`.
    fn on_reserve_read(
        &self,
        req: ReserveReadRequest,
    ) -> impl std::future::Future<Output = ReserveReadResponse> + Send;

    /// Routed release-reservation (Calvin OLLP). The ack-only sibling of
    /// [`on_reserve_read`](Self::on_reserve_read). This node is the
    /// SEQUENCER-GROUP leader: decode the owner and release reason carried by
    /// `req` and release the reservation. The transport writes exactly one
    /// [`ReleaseReservationResponse`] carrying success (or a typed error)
    /// back to the coordinator. The release is bounded by
    /// `req.deadline_remaining_ms`.
    fn on_release_reservation(
        &self,
        req: ReleaseReservationRequest,
    ) -> impl std::future::Future<Output = ReleaseReservationResponse> + Send;
}
