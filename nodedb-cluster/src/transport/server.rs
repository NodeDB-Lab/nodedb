//! Inbound Raft RPC handling.
//!
//! Accepts connections from the QUIC endpoint, dispatches incoming bidi
//! streams to a [`RaftRpcHandler`], and writes back the response frame.

use std::sync::Arc;

use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc};

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;
}

/// Handle all bidi streams on a single connection.
pub(crate) async fn handle_connection<H: RaftRpcHandler>(
    conn: quinn::Connection,
    handler: Arc<H>,
) -> Result<()> {
    loop {
        let (send, recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept_bi: {e}"),
                });
            }
        };

        let h = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(h, send, recv).await {
                debug!(error = %e, "raft RPC stream error");
            }
        });
    }
}

/// Handle a single bidi stream: read request → dispatch → write response.
async fn handle_stream<H: RaftRpcHandler>(
    handler: Arc<H>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) -> Result<()> {
    let request_frame = read_frame(&mut recv).await?;
    let request = rpc_codec::decode(&request_frame)?;

    let response = handler.handle_rpc(request).await?;

    let response_frame = rpc_codec::encode(&response)?;
    send.write_all(&response_frame)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("write response: {e}"),
        })?;
    send.finish().map_err(|e| ClusterError::Transport {
        detail: format!("finish response: {e}"),
    })?;

    Ok(())
}

/// Read a complete RPC frame from a QUIC receive stream.
///
/// Reads the header first to determine frame size, then reads the payload.
pub(crate) async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    let mut header = [0u8; rpc_codec::HEADER_SIZE];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read header: {e}"),
        })?;

    let total = rpc_codec::frame_size(&header)?;
    let mut frame = vec![0u8; total];
    frame[..rpc_codec::HEADER_SIZE].copy_from_slice(&header);

    if total > rpc_codec::HEADER_SIZE {
        recv.read_exact(&mut frame[rpc_codec::HEADER_SIZE..])
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("read payload: {e}"),
            })?;
    }

    Ok(frame)
}
