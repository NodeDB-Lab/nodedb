// SPDX-License-Identifier: BUSL-1.1

//! The native session's frame read/route/write loop: version handshake,
//! absolute/idle timeout enforcement, frame decode, and response emission
//! (including chunking for oversized responses).

use std::time::Duration;

use tracing::{debug, instrument};

use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse};

use super::codec::{self, FrameFormat};
use super::dispatch;
use super::{NativeSession, chunk_large_response};

impl NativeSession {
    /// Run the session: drives the frame loop, then reclaims any
    /// still-open transaction's Data-Plane overlays on every exit path
    /// (clean EOF, idle/absolute timeout, or abrupt disconnect/error).
    pub async fn run(mut self) -> crate::Result<()> {
        let result = self.run_loop().await;
        self.reclaim_open_txn().await;
        result
    }

    /// Reclaim a still-open transaction's Data-Plane overlays if the
    /// connection ended mid-transaction (no COMMIT/ROLLBACK). Idempotent:
    /// a no-op when the session is idle, so a graceful end does not
    /// double-drop.
    async fn reclaim_open_txn(&self) {
        use crate::control::server::native::dispatch::NativeTxnDp;
        use crate::control::server::shared::session::{TransactionState, lifecycle};

        let Some(identity) = self.identity.as_ref() else {
            return;
        };
        if self.sessions.transaction_state(&self.peer_addr) == TransactionState::Idle {
            return;
        }
        let dp = NativeTxnDp {
            state: self.state.as_ref(),
        };
        lifecycle::run_rollback(
            &self.sessions,
            &self.peer_addr,
            identity,
            self.state.as_ref(),
            &dp,
        )
        .await;
    }

    /// Run the session loop: read frames, route by opcode, write responses.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    async fn run_loop(&mut self) -> crate::Result<()> {
        // Perform the version-negotiation handshake before any frame exchange.
        let limits = self.state.limits.clone();
        self.proto_ver =
            super::super::handshake::perform_server_handshake(&mut self.stream, &limits).await?;

        let idle_timeout_secs = self.state.idle_timeout_secs();
        let absolute_timeout_secs = self.state.session_absolute_timeout_secs();

        loop {
            // Enforce absolute session lifetime (SQLSTATE 57P01 "admin shutdown").
            if absolute_timeout_secs > 0
                && self.connected_at.elapsed().as_secs() >= absolute_timeout_secs
            {
                debug!(
                    "session absolute timeout ({}s), closing connection",
                    absolute_timeout_secs
                );
                let shutdown_resp = NativeResponse::error(
                    0,
                    "57P01",
                    "session timeout: absolute lifetime exceeded",
                );
                if let Ok(bytes) = super::codec::encode_response(
                    &shutdown_resp,
                    self.format.unwrap_or(FrameFormat::MessagePack),
                ) {
                    let _ = super::codec::write_frame(&mut self.stream, &bytes).await;
                }
                return Ok(());
            }

            // Read a frame with idle timeout.
            let frame_result = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    codec::read_frame(&mut self.stream),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        debug!("session idle timeout ({}s)", idle_timeout_secs);
                        return Ok(());
                    }
                }
            } else {
                codec::read_frame(&mut self.stream).await
            };

            let payload = match frame_result {
                Ok(Some(p)) => p,
                Ok(None) => return Ok(()), // clean EOF
                Err(crate::Error::BadRequest { detail }) => {
                    // Send a typed error before closing so the client knows why.
                    let err_resp =
                        NativeResponse::error(0, "54000", format!("frame rejected: {detail}"));
                    let format = self.format.unwrap_or(FrameFormat::MessagePack);
                    if let Ok(bytes) = codec::encode_response(&err_resp, format) {
                        let _ = codec::write_frame(&mut self.stream, &bytes).await;
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            // Auto-detect format on first frame.
            if self.format.is_none() {
                self.format = Some(FrameFormat::detect(payload[0]));
            }
            let Some(format) = self.format else {
                return Err(crate::Error::BadRequest {
                    detail: "format detection failed after first frame".into(),
                });
            };

            // Decode and handle.
            let outcome = match codec::decode_request(&payload, format) {
                Ok(req) => self.handle_request(req).await,
                Err(e) => dispatch::SqlOutcome::Response(Box::new(NativeResponse::error(
                    0,
                    "42601",
                    format!("{e}"),
                ))),
            };

            match outcome {
                dispatch::SqlOutcome::Response(response) => {
                    // Encode and write response — chunk if it exceeds frame limit.
                    let resp_bytes = codec::encode_response(&response, format)?;
                    if resp_bytes.len() <= MAX_FRAME_SIZE as usize {
                        codec::write_frame(&mut self.stream, &resp_bytes).await?;
                    } else {
                        // Response too large for a single frame — split rows.
                        let frames = chunk_large_response(*response, format)?;
                        for frame in &frames {
                            codec::write_frame(&mut self.stream, frame).await?;
                        }
                    }
                }
                dispatch::SqlOutcome::Stream(sql_stream) => {
                    super::session_stream::emit_sql_stream(&mut self.stream, sql_stream, format)
                        .await?;
                }
            }
        }
    }
}
