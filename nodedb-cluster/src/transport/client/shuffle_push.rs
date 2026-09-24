// SPDX-License-Identifier: BUSL-1.1

//! Outbound streaming-shuffle push: a producer streams row batches to a
//! receiver over one QUIC bidi stream, each frame in an authenticated
//! envelope.

use std::sync::Arc;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{
    self, RaftRpc, ShufflePushChunk, ShufflePushEnd, ShufflePushRequest, TypedClusterError,
    auth_envelope,
};
use crate::transport::auth_context::AuthContext;

use super::transport::NexarTransport;

impl NexarTransport {
    /// Open a cross-node streaming-shuffle push to `target`.
    ///
    /// Producer → receiver direction (the mirror of [`NexarTransport::send_rpc_stream`], which
    /// streams a response back): opens a bidi stream on the pooled connection,
    /// writes the [`ShufflePushRequest`] envelope, then one [`ShufflePushChunk`]
    /// envelope per pre-batched payload, then exactly one [`ShufflePushEnd`]
    /// (clean EOF), and `finish()`es the send half. It does **not** read a
    /// reply — the server deposits the chunks and never writes back, so the
    /// helper fire-and-finishes.
    ///
    /// Each `batches` element is a standalone msgpack array of rows (the same
    /// convention as `RowBatch.payload`). The planner-side caller is
    /// responsible for computing `partition_hash(row, keys) % num_parts` and
    /// grouping rows into per-partition batches before calling this. This
    /// helper takes already-partitioned payloads.
    pub async fn send_shuffle_push(
        &self,
        target: u64,
        req: ShufflePushRequest,
        batches: Vec<Vec<u8>>,
    ) -> Result<()> {
        // Reimplemented on top of the incremental [`ShufflePushStream`] handle so
        // the one-shot path and the fanout sink share one wire encoder: open
        // → push each pre-batched payload → finish with a clean EOF.
        let mut stream = ShufflePushStream::open(self, target, req).await?;
        for payload in batches {
            stream.push_chunk(payload).await?;
        }
        stream.finish(None).await
    }

    /// Open an incremental shuffle-push stream to `target`.
    ///
    /// Thin convenience wrapper around [`ShufflePushStream::open`] for callers
    /// that hold `&self` (the fanout sink opens one per part).
    pub async fn open_shuffle_push_stream(
        &self,
        target: u64,
        req: ShufflePushRequest,
    ) -> Result<ShufflePushStream> {
        ShufflePushStream::open(self, target, req).await
    }
}

/// An incremental producer → receiver shuffle-push stream.
///
/// The one-shot [`NexarTransport::send_shuffle_push`] writes every chunk up
/// front. The fanout sink instead opens one of these per target part and
/// pushes chunks as the local scan produces rows, finishing the stream (clean
/// EOF or terminal error) only once the scan ends. It owns its `quinn::SendStream`
/// and an `Arc` clone of the transport's [`AuthContext`] so each frame is wrapped
/// with a fresh outbound `seq` exactly like the one-shot path — no borrow of the
/// transport is held for the stream's lifetime.
///
/// Each `write_all` is awaited inline, so QUIC flow control back-pressures the
/// producer when the receiver falls behind — bounded memory, never a buffered
/// whole side.
pub struct ShufflePushStream {
    send: quinn::SendStream,
    auth: Arc<AuthContext>,
    target: u64,
}

impl ShufflePushStream {
    /// Open a bidi stream to `target` and write the opening
    /// [`ShufflePushRequest`] envelope. The send half stays open for subsequent
    /// [`push_chunk`](Self::push_chunk) calls; the receiver deposits frames and
    /// never writes back.
    pub async fn open(
        transport: &NexarTransport,
        target: u64,
        req: ShufflePushRequest,
    ) -> Result<Self> {
        let conn = transport.get_or_connect(target).await?;
        transport.verify_connection_target(&conn, target)?;
        let (mut send, _recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi (shuffle push) to node {target}: {e}"),
        })?;

        let auth = Arc::clone(transport.auth());
        let req_envelope = wrap_with_auth(&auth, &RaftRpc::ShufflePushRequest(req))?;
        send.write_all(&req_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle push request to node {target}: {e}"),
            })?;

        Ok(Self { send, auth, target })
    }

    /// Write one [`ShufflePushChunk`] envelope (a standalone msgpack row array).
    pub async fn push_chunk(&mut self, payload: Vec<u8>) -> Result<()> {
        let chunk_envelope = wrap_with_auth(
            &self.auth,
            &RaftRpc::ShufflePushChunk(ShufflePushChunk { payload }),
        )?;
        self.send
            .write_all(&chunk_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle push chunk to node {}: {e}", self.target),
            })
    }

    /// Write the terminal [`ShufflePushEnd`] envelope (`error: None` for a clean
    /// EOF, `Some(e)` to fail the receiver fast) and finish the send half.
    pub async fn finish(mut self, error: Option<TypedClusterError>) -> Result<()> {
        let end_envelope = wrap_with_auth(
            &self.auth,
            &RaftRpc::ShufflePushEnd(ShufflePushEnd { error }),
        )?;
        self.send
            .write_all(&end_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle push end to node {}: {e}", self.target),
            })?;
        self.send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish shuffle push to node {}: {e}", self.target),
        })?;
        Ok(())
    }
}

/// Encode `rpc` and wrap it in an authenticated envelope with a fresh outbound
/// `seq` — the standalone form of [`NexarTransport::wrap_outbound`] for the
/// owned-[`AuthContext`] [`ShufflePushStream`].
fn wrap_with_auth(auth: &AuthContext, rpc: &RaftRpc) -> Result<Vec<u8>> {
    let inner = rpc_codec::encode(rpc, &auth.epoch)?;
    let seq = auth.peer_seq_out.next();
    let mut out = Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + inner.len());
    auth_envelope::write_envelope(auth.local_node_id, seq, &inner, &auth.mac_key, &mut out)?;
    Ok(out)
}
