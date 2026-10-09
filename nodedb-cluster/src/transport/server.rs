// SPDX-License-Identifier: BUSL-1.1

//! Inbound Raft RPC handling.
//!
//! Accepts connections from the QUIC endpoint, dispatches incoming bidi
//! streams to a [`RaftRpcHandler`], and writes back the response frame.
//!
//! # Authenticated wire envelope
//!
//! Every on-wire message is an [`auth_envelope`]-wrapped
//! [`rpc_codec`] frame. The envelope carries `from_node_id`, a per-peer
//! monotonic `seq`, and an HMAC-SHA256 MAC. [`handle_stream`]:
//!
//! 1. reads one envelope from the QUIC stream,
//! 2. verifies the MAC against the cluster MAC key held by
//!    [`AuthContext`],
//! 3. rejects replays via the per-peer sliding window,
//!    3b. verifies the TLS peer certificate identity against the topology pin,
//! 4. decodes the inner frame and dispatches to the handler,
//! 5. wraps the handler's response in its own authenticated envelope
//!    with `from_node_id = local_node_id` and a fresh outbound seq for
//!    the caller's id. A handler error is answered with a typed
//!    `RequestRefused` frame in place of the response.
//!
//! # Cooperative shutdown
//!
//! Every long-lived `.await` is wrapped in a `tokio::select!` over a
//! `watch::Receiver<bool>` shutdown signal that is cloned into every
//! spawned child task, so graceful shutdown promptly releases handler
//! Arcs held by grandchild per-stream tasks.
//!
//! [`auth_envelope`]: crate::rpc_codec::auth_envelope
//! [`rpc_codec`]: crate::rpc_codec

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::error::{ClusterError, Result};
use crate::forward::ChunkSink;
use crate::rpc_codec::{
    self, ExecuteStreamChunk, ExecuteStreamEnd, FrameRefusal, RaftRpc, RequestRefusal,
    auth_envelope,
};
use crate::transport::auth_context::AuthContext;
use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::rpc_handler::RaftRpcHandler;
use crate::wire_version::handshake_io::{local_version_range, perform_version_handshake_server};

use super::frame_io::{finish_stream, read_envelope, reply_and_finish, write_rpc_frame};
use super::reply_stream::ReplyStream;
use super::shuffle_drain::drain_shuffle_push;
use super::stream_dispatch;
use super::stream_identity::{reject_peer_identity, verify_stream_identity};

/// Transport-local [`ChunkSink`] that writes one `RPC_EXECUTE_STREAM_CHUNK`
/// envelope per chunk to a QUIC send stream.
///
/// Each chunk gets a fresh outbound `seq` (mirroring the one-shot response
/// path in [`handle_stream`]). The `write_all` is awaited inline so QUIC flow
/// control throttles the producer — the chunk MUST NOT be detached into a
/// spawned task.
struct QuicChunkSink<'a> {
    send: &'a mut quinn::SendStream,
    auth: &'a AuthContext,
}

impl ChunkSink for QuicChunkSink<'_> {
    async fn send_chunk(
        &mut self,
        payload: Vec<u8>,
        watermark_lsn: u64,
        // The `ExecuteStream` wire chunk only carries `watermark_lsn`; the
        // per-collection read version is surfaced separately on the shuffle
        // produce reply, not on this streaming path.
        _read_version_lsn: u64,
    ) -> Result<()> {
        let rpc = RaftRpc::ExecuteStreamChunk(ExecuteStreamChunk {
            payload,
            watermark_lsn,
        });
        write_rpc_frame(self.send, self.auth, &rpc, "stream chunk").await
    }
}

/// Extract the peer's leaf certificate DER bytes from a QUIC connection.
///
/// Returns `None` if the peer did not present a certificate (insecure
/// transport) or if the runtime-type downcast fails.
fn peer_leaf_cert_der(conn: &quinn::Connection) -> Option<Vec<u8>> {
    let identity = conn.peer_identity()?;
    let certs: &Vec<CertificateDer<'static>> = identity.downcast_ref()?;
    certs.first().map(|c| c.as_ref().to_vec())
}

/// Handle all bidi streams on a single connection.
///
/// Exits cleanly (Ok) on shutdown, on normal connection close,
/// or on unrecoverable transport error.
pub(crate) async fn handle_connection<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized>(
    conn: quinn::Connection,
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    identity_store: Arc<S>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // Extract the peer cert once per connection; it does not change.
    let peer_cert_der: Option<Vec<u8>> = peer_leaf_cert_der(&conn);
    let peer_addr = conn.remote_address();

    // Perform the wire-version handshake on the first bidi stream before
    // dispatching any RPCs. The client opens a dedicated stream for this
    // exchange; subsequent streams on the same connection are RPC streams.
    let agreed_version = {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                // Spurious change — retry the accept.
                conn.accept_bi().await
            }
            result = conn.accept_bi() => result,
        };

        let (mut hs_send, mut hs_recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept handshake stream from {peer_addr}: {e}"),
                });
            }
        };

        let local = local_version_range();
        match perform_version_handshake_server(&conn, &mut hs_send, &mut hs_recv).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    peer_addr = %peer_addr,
                    local_min = %local.min,
                    local_max = %local.max,
                    error = %e,
                    "wire version handshake failed; closing connection"
                );
                // perform_version_handshake_server already closed the QUIC
                // connection on range mismatch; propagate the error so the
                // caller logs it and the connection task exits.
                return Err(e);
            }
        }
    };

    debug!(
        peer_addr = %peer_addr,
        agreed_version = %agreed_version,
        "wire version handshake complete"
    );

    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = conn.accept_bi() => result,
        };

        let (send, recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept_bi: {e}"),
                });
            }
        };

        let ctx = StreamContext {
            handler: handler.clone(),
            auth: auth.clone(),
            identity_store: identity_store.clone(),
            peer_cert_der: peer_cert_der.clone(),
            conn: conn.clone(),
            shutdown: shutdown.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = handle_stream(ctx, send, recv).await {
                debug!(error = %e, "raft RPC stream error");
            }
        });
    }
}

/// Per-stream context passed to [`handle_stream`].
///
/// Bundles the shared, connection-scoped handles so [`handle_stream`] stays
/// under the `too_many_arguments` threshold while remaining generic over
/// handler and identity-store types.
struct StreamContext<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized> {
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    identity_store: Arc<S>,
    peer_cert_der: Option<Vec<u8>>,
    conn: quinn::Connection,
    shutdown: watch::Receiver<bool>,
}

/// Handle a single bidi stream: read request → dispatch → write response.
///
/// Every long-lived await is racing a shutdown signal — see the
/// module docstring for the rationale.
async fn handle_stream<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized>(
    ctx: StreamContext<H, S>,
    send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) -> Result<()> {
    let mut send = ReplyStream::new(send);
    let StreamContext {
        handler,
        auth,
        identity_store,
        peer_cert_der,
        conn,
        mut shutdown,
    } = ctx;
    let work = async {
        // 1. Read one envelope.
        let envelope = read_envelope(&mut recv).await?;
        let (fields, inner_frame) = auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;

        // 2. Replay window — under the advertised from_node_id (MAC-verified).
        //    Self-addressed frames skip the window: when a node dispatches
        //    an RPC to itself over the transport, the shared `AuthContext`
        //    means one window is updated by both the server-side request
        //    accept (here) and the client-side response accept (in
        //    `send.rs::parse_inbound`). Skipping when `from == local`
        //    keeps the two flows from tripping on each other's entries —
        //    a self-addressed frame can't have been replayed by an
        //    external attacker by definition.
        //    A refused frame is answered with a typed `FrameRefused`, never a
        //    dropped stream: the MAC verified, so the sender is genuine and
        //    its link is up. It retries under a fresh sequence number, and a
        //    dropped stream would count against this node's health instead.
        if fields.from_node_id != auth.local_node_id
            && let Err(e) = auth.peer_seq_in.accept(fields.from_node_id, fields.seq)
        {
            debug!(
                from_node_id = fields.from_node_id,
                error = %e,
                "raft RPC frame refused by the replay window"
            );
            let refusal = RaftRpc::FrameRefused(FrameRefusal {
                detail: e.to_string(),
            });
            write_rpc_frame(&mut send, &auth, &refusal, "frame refusal").await?;
            finish_stream(&mut send, "frame refusal")?;
            return Ok::<(), ClusterError>(());
        }

        // 3. Decode before the identity decision so an unknown, CA-verified
        // peer can be restricted to exactly one enrollment operation.
        let request = rpc_codec::decode(inner_frame, &auth.epoch)?;
        validate_join_sender(&request, fields.from_node_id)?;

        // 3b. Bind the MAC-authenticated node id to the mTLS leaf identity.
        verify_stream_identity(
            &conn,
            &*identity_store,
            peer_cert_der.as_deref(),
            &auth,
            fields.from_node_id,
            &request,
        )?;
        // The handler can run the request from here on. A stream that ends
        // without an answer is reset, so the sender never resends it.
        send.arm();

        // 4b. Streaming path: an `ExecuteStreamRequest` produces a multi-frame
        //     response — N `RPC_EXECUTE_STREAM_CHUNK` envelopes (each written
        //     inline so QUIC flow control throttles the producer) followed by
        //     exactly one `RPC_EXECUTE_STREAM_END` envelope, then `finish()`.
        if let RaftRpc::ExecuteStreamRequest(req) = request {
            let terminal = {
                let sink = QuicChunkSink {
                    send: &mut send,
                    auth: &auth,
                };
                handler.handle_rpc_streaming(req, sink).await
            };

            let end_rpc = RaftRpc::ExecuteStreamEnd(ExecuteStreamEnd { error: terminal });
            write_rpc_frame(&mut send, &auth, &end_rpc, "stream end").await?;
            finish_stream(&mut send, "stream response")?;
            return Ok::<(), ClusterError>(());
        }

        // 4c. Cross-node streaming shuffle: a `ShufflePushRequest` opens a
        //     producer → receiver stream on this read half. The server
        //     deposits each inbound frame and writes no reply.
        if let RaftRpc::ShufflePushRequest(req) = request {
            let opener = fields.from_node_id;
            return drain_shuffle_push(&*handler, &auth, &mut recv, req, opener, |node| {
                reject_peer_identity(&conn, node)
            })
            .await;
        }

        // 4d onward. One-shot RPCs: one request, one response, no further
        //     frames on `recv`.
        let request =
            match stream_dispatch::try_handle_oneshot_rpc(&*handler, request, &mut send, &auth)
                .await?
            {
                None => return Ok::<(), ClusterError>(()),
                Some(req) => req,
            };

        // A handler error is answered with a typed `RequestRefused`, never a
        // dropped stream. The MAC verified, so the sender is genuine and its
        // link is up. The sender reads a dropped stream as a link failure.
        let outcome = handler.handle_rpc(request).await;
        if let Err(e) = &outcome {
            debug!(
                from_node_id = fields.from_node_id,
                error = %e,
                "raft RPC refused by the handler"
            );
        }
        let response = reply_for(outcome);

        // 5. Wrap the response in its own envelope. `from = local_node_id`,
        //    `seq = next outbound seq scoped to the caller`.
        reply_and_finish(&mut send, &auth, &response, "response").await
    };

    let outcome = tokio::select! {
        biased;
        _ = shutdown.changed() => Ok(false),
        result = work => result.map(|()| true),
    };
    if let Ok(true) = outcome {
        send.answered();
    }
    outcome.map(|_| ())
}

/// The frame that answers a one-shot request: the handler's response, or a
/// typed refusal carrying its error.
fn reply_for(outcome: Result<RaftRpc>) -> RaftRpc {
    match outcome {
        Ok(response) => response,
        Err(error) => RaftRpc::RequestRefused(RequestRefusal::from(error)),
    }
}

fn validate_join_sender(request: &RaftRpc, authenticated_node_id: u64) -> Result<()> {
    if let RaftRpc::JoinRequest(join) = request
        && join.node_id != authenticated_node_id
    {
        return Err(ClusterError::Transport {
            detail: format!(
                "join request node_id {} does not match authenticated sender {}",
                join.node_id, authenticated_node_id
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::{JoinRequest, RefusalReason};

    #[test]
    fn a_handler_error_is_answered_with_a_typed_refusal() {
        let reply = reply_for(Err(ClusterError::GroupNotFound { group_id: 4 }));
        match reply {
            RaftRpc::RequestRefused(refusal) => assert!(matches!(
                refusal.reason,
                RefusalReason::GroupNotHosted { group_id: 4 }
            )),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_handler_response_is_answered_as_is() {
        let pong = RaftRpc::Pong(crate::rpc_codec::PongResponse {
            responder_id: 1,
            topology_version: 3,
        });
        assert!(matches!(reply_for(Ok(pong)), RaftRpc::Pong(_)));
    }

    #[test]
    fn join_request_must_match_authenticated_sender() {
        let request = RaftRpc::JoinRequest(JoinRequest {
            node_id: 9,
            listen_addr: "127.0.0.1:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: Some(vec![1; 32]),
            swim_addr: None,
        });
        assert!(validate_join_sender(&request, 9).is_ok());
        let error = validate_join_sender(&request, 8).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match authenticated sender 8")
        );
    }
}
