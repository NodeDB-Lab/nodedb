// SPDX-License-Identifier: BUSL-1.1

//! One-shot RPC dispatch arms extracted from [`super::server::handle_stream`].
//!
//! Each arm in [`try_handle_oneshot_rpc`] corresponds to a single
//! request/response exchange that does NOT need access to the inbound
//! [`quinn::RecvStream`] — it reads its single request frame from the
//! already-decoded [`RaftRpc`] value, calls the handler, encodes the response,
//! and finishes the send stream.
//!
//! Arms that read additional frames from `recv` live elsewhere: the
//! `ExecuteStreamRequest` arm in `server.rs` and the `ShufflePushRequest`
//! arm in `shuffle_drain.rs`.

use crate::error::Result;
use crate::rpc_codec::{
    AssignSurrogateResponse, RaftRpc, ReleaseReservationResponse, ReserveReadResponse,
    ShuffleAggregateConsumeResponse, ShuffleConsumeResponse, SubmitCalvinInboxResponse,
    SubmitCalvinTxnResponse,
};
use crate::transport::auth_context::AuthContext;
use crate::transport::rpc_handler::RaftRpcHandler;

use super::frame_io::{finish_stream, reply_and_finish};

/// Attempt to handle a one-shot (single-request / single-response) RPC.
///
/// Checks whether `request` is one of the known one-shot shuffle variants
/// (ShuffleProduce / ShuffleConsume / ShuffleAggregateConsume). If it is,
/// the handler is called, the response is encoded and written on `send`, the
/// stream is finished, and `Ok(None)` is returned — indicating to the caller
/// that it should return immediately.
///
/// If `request` is not one of these variants it is returned as
/// `Ok(Some(request))` so the caller can continue with the normal dispatch
/// path.
pub(super) async fn try_handle_oneshot_rpc<H: RaftRpcHandler>(
    handler: &H,
    request: RaftRpc,
    send: &mut quinn::SendStream,
    auth: &AuthContext,
) -> Result<Option<RaftRpc>> {
    // 4d. Cross-node shuffle PRODUCER trigger: a `ShuffleProduceRequest`
    //     is a ONE-SHOT request/response (NOT a stream from the coordinator).
    //     The producer runs a local scan, fans the hash-partitioned rows out
    //     to the part-owners on its OWN outbound `ShufflePush` streams, then
    //     replies with exactly one `ShuffleProduceResponse` carrying terminal
    //     success or a typed error so the coordinator can await completion.
    //     The reply is written on this same bidi stream's send half (mirroring
    //     the one-shot `handle_rpc` reply below), then `finish()`ed.
    if let RaftRpc::ShuffleProduceRequest(req) = request {
        let resp = handler.on_shuffle_produce(req).await;
        let resp_rpc = RaftRpc::ShuffleProduceResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "shuffle produce response").await?;
        return Ok(None);
    }

    // 4e. Cross-node shuffle CONSUMER trigger: a `ShuffleConsumeRequest`
    //     is a ONE-SHOT request/response. The part-owner waits for both staged
    //     sides of its part to finalize, runs the node-local grace join, and
    //     replies with exactly one `ShuffleConsumeResponse` carrying the join
    //     rows (or a typed error). The reply is written on this same bidi
    //     stream's send half (mirroring the produce arm above), then
    //     `finish()`ed.
    if let RaftRpc::ShuffleConsumeRequest(req) = request {
        let resp: ShuffleConsumeResponse = handler.on_shuffle_consume(req).await;
        let resp_rpc = RaftRpc::ShuffleConsumeResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "shuffle consume response").await?;
        return Ok(None);
    }

    // 4f. Cross-node distributed GROUP BY shuffle CONSUMER trigger: a
    //     `ShuffleAggregateConsumeRequest` is a ONE-SHOT request/response and
    //     the single-sided aggregate sibling of the consume arm above. The
    //     part-owner waits for its part's single staged producer side to
    //     finalize, merges + finalizes the partial states, and replies with
    //     exactly one `ShuffleAggregateConsumeResponse` carrying the aggregate
    //     rows (or a typed error). The reply is written on this same bidi
    //     stream's send half (mirroring the consume arm above), then
    //     `finish()`ed.
    if let RaftRpc::ShuffleAggregateConsumeRequest(req) = request {
        let resp: ShuffleAggregateConsumeResponse = handler.on_shuffle_aggregate(req).await;
        let resp_rpc = RaftRpc::ShuffleAggregateConsumeResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "shuffle aggregate consume response").await?;
        return Ok(None);
    }

    // 4g. Routed-surrogate-exchange: an `AssignSurrogateRequest` is a
    //     ONE-SHOT request/response. This node is the home vShard's leader. It
    //     assigns or returns the authoritative surrogate of every key the
    //     request carries and replies with exactly one
    //     `AssignSurrogateResponse`: one answer per key, or a typed error.
    //     The reply is written on this same bidi stream's send half (mirroring
    //     the consume arms above), then `finish()`ed.
    if let RaftRpc::AssignSurrogateRequest(req) = request {
        let resp: AssignSurrogateResponse = handler.on_assign_surrogate(req).await;
        let resp_rpc = RaftRpc::AssignSurrogateResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "assign surrogate response").await?;
        return Ok(None);
    }

    // 4h. Routed Calvin-submit: a `SubmitCalvinTxnRequest` is a ONE-SHOT
    //     request/response. This node is the SEQUENCER-GROUP leader; it submits
    //     the carried `TxClass` to its local Calvin sequencer inbox, awaits
    //     assignment + completion, and replies with exactly one
    //     `SubmitCalvinTxnResponse` carrying success or a typed error. The reply
    //     is written on this same bidi stream's send half (mirroring the
    //     assign-surrogate arm above), then `finish()`ed.
    if let RaftRpc::SubmitCalvinTxnRequest(req) = request {
        let resp: SubmitCalvinTxnResponse = handler.on_submit_calvin_txn(req).await;
        let resp_rpc = RaftRpc::SubmitCalvinTxnResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "submit calvin txn response").await?;
        return Ok(None);
    }

    // 4i. Routed Calvin-INBOX submit:a `SubmitCalvinInboxRequest` is
    //     a ONE-SHOT request/response and the OLLP dependent sibling of the
    //     submit-calvin-txn arm above. This node is the SEQUENCER-GROUP leader; it
    //     submits the carried `TxClass` to its local Calvin sequencer inbox,
    //     awaits only the ASSIGNMENT (NOT completion), and replies with exactly
    //     one `SubmitCalvinInboxResponse` carrying the assignment or a typed
    //     error. The reply is written on this same bidi stream's send half
    //     (mirroring the submit-calvin-txn arm above), then `finish()`ed.
    if let RaftRpc::SubmitCalvinInboxRequest(req) = request {
        let resp: SubmitCalvinInboxResponse = handler.on_submit_calvin_inbox(req).await;
        let resp_rpc = RaftRpc::SubmitCalvinInboxResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "submit calvin inbox response").await?;
        return Ok(None);
    }

    // 4i2. Routed reserve-read (Calvin OLLP): a `ReserveReadRequest` is a
    //     ONE-SHOT request/response. This node is the SEQUENCER-GROUP leader; it
    //     decodes the carried `LockKey` and assign-only reserves the read lock,
    //     replying with exactly one `ReserveReadResponse` carrying the minted
    //     owner or a typed error. The reply is written on this same bidi
    //     stream's send half (mirroring the submit-calvin-inbox arm above), then
    //     `finish()`ed.
    if let RaftRpc::ReserveReadRequest(req) = request {
        let resp: ReserveReadResponse = handler.on_reserve_read(req).await;
        let resp_rpc = RaftRpc::ReserveReadResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "reserve read response").await?;
        return Ok(None);
    }

    // 4i3. Routed release-reservation (Calvin OLLP): a
    //     `ReleaseReservationRequest` is a ONE-SHOT request/response and the
    //     ack-only sibling of the reserve-read arm above. This node is the
    //     SEQUENCER-GROUP leader; it decodes the carried owner and release
    //     reason and releases the reservation, replying with exactly one
    //     `ReleaseReservationResponse` carrying success or a typed error. The
    //     reply is written on this same bidi stream's send half (mirroring the
    //     reserve-read arm above), then `finish()`ed.
    if let RaftRpc::ReleaseReservationRequest(req) = request {
        let resp: ReleaseReservationResponse = handler.on_release_reservation(req).await;
        let resp_rpc = RaftRpc::ReleaseReservationResponse(resp);
        reply_and_finish(send, auth, &resp_rpc, "release reservation response").await?;
        return Ok(None);
    }

    // 4j. TimeoutNow (leadership transfer): one-way — the receiver dispatches the
    //     trigger to its matching group and finishes the send stream with no
    //     response frame. The sender does not await a reply (it discards recv).
    if let RaftRpc::TimeoutNowRequest(req) = request {
        handler.on_timeout_now(req).await;
        finish_stream(send, "timeout_now (no-response)")?;
        return Ok(None);
    }

    Ok(Some(request))
}
