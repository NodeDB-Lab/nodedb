// SPDX-License-Identifier: BUSL-1.1

//! The native session's frame read/route/write loop: version handshake,
//! absolute/idle timeout enforcement, frame decode, and response emission
//! (including chunking for oversized responses).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;

use tracing::{debug, instrument};

use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse};

use super::NativeSession;
use super::codec::{self, FrameFormat};
use super::dispatch;
use super::session_chunk::chunk_large_response;
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Rollback dependencies retained independently from the connection future.
///
/// The session store is private to one native connection. The authenticated
/// identity is published once after successful authentication and never
/// replaced, so teardown cannot accidentally use a later client value.
pub(crate) struct NativeTxnCleanup {
    sessions: Arc<crate::control::server::shared::session::SessionStore>,
    session_id: crate::control::server::shared::session::SessionId,
    state: Arc<crate::control::state::SharedState>,
    identity: OnceLock<crate::control::security::identity::AuthenticatedIdentity>,
    /// DDL this connection buffered in an open transaction, taken at teardown
    /// start. Teardown is detached and runs outside the connection scope, so
    /// the buffer has to be carried across rather than read there.
    discarded_ddl:
        std::sync::Mutex<Option<crate::control::server::shared::session::ddl_buffer::DdlBuffer>>,
    started: AtomicBool,
    completed: AtomicBool,
    completion: Notify,
}

impl NativeTxnCleanup {
    pub(super) fn new(
        sessions: Arc<crate::control::server::shared::session::SessionStore>,
        session_id: crate::control::server::shared::session::SessionId,
        state: Arc<crate::control::state::SharedState>,
    ) -> Self {
        Self {
            sessions,
            session_id,
            state,
            identity: OnceLock::new(),
            discarded_ddl: std::sync::Mutex::new(None),
            started: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            completion: Notify::new(),
        }
    }

    /// Publish the authenticated identity exactly once for teardown.
    pub(super) fn publish_identity(
        &self,
        identity: crate::control::security::identity::AuthenticatedIdentity,
    ) {
        // Re-authentication is rejected before publication, so a failed set is
        // only a duplicate publication of the already authoritative identity.
        let _ = self.identity.set(identity);
    }

    /// Start cleanup exactly once. Completion is published even when rollback
    /// panics, and callers never inspect the discarded panic payload.
    pub(crate) fn start(self: &Arc<Self>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // All production callers run on Tokio. If that invariant is ever
            // violated, do not leave a waiter stuck during shutdown.
            self.publish_completion();
            return;
        };
        self.take_discarded_ddl_into_slot();
        let cleanup = Arc::clone(self);
        handle.spawn(async move {
            let _ = crate::control::server::shared::isolate_connection_future(
                Arc::clone(&cleanup).rollback(),
            )
            .await;
            cleanup.publish_completion();
        });
    }

    fn publish_completion(&self) {
        self.completed.store(true, Ordering::Release);
        self.completion.notify_waiters();
    }

    /// Start cleanup before the first await and wait with a lost-wakeup-safe
    /// completion protocol. Cancellation of this waiter does not cancel the
    /// detached cleanup task.
    pub(crate) async fn start_and_wait(self: &Arc<Self>) {
        self.start();
        loop {
            if self.completed.load(Ordering::Acquire) {
                return;
            }
            let notified = self.completion.notified();
            if self.completed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Move this connection's DDL buffer into the cleanup slot. Runs inside the
    /// connection scope, synchronously, before teardown detaches.
    fn take_discarded_ddl_into_slot(&self) {
        let taken = crate::control::server::shared::session::ddl_buffer::take();
        let mut slot = self
            .discarded_ddl
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = taken;
    }

    async fn rollback(self: Arc<Self>) {
        use crate::control::server::native::dispatch::NativeTxnDp;
        use crate::control::server::shared::session::{TransactionState, ddl_rollback, lifecycle};

        let discarded = {
            let mut slot = self
                .discarded_ddl
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };
        if let Some(identity) = self.identity.get().cloned()
            && self.sessions.transaction_state(self.session_id) != TransactionState::Idle
        {
            let dp = NativeTxnDp {
                state: self.state.as_ref(),
            };
            lifecycle::run_rollback(
                self.sessions.as_ref(),
                self.session_id,
                &identity,
                self.state.as_ref(),
                &dp,
            )
            .await;
        }
        // Runs on every teardown path, including the ones that have no session
        // to roll back: a connection that dropped after a buffered CREATE still
        // moved this node's Data Plane.
        if let Some(discarded) = discarded {
            ddl_rollback::restore_data_plane(self.state.as_ref(), &discarded).await;
        }
    }
}

/// Starts rollback synchronously when the owner is dropped. Completion stays
/// owned by `NativeTxnCleanup`, so an aborted waiter cannot lose teardown.
struct NativeTxnCleanupGuard {
    cleanup: Arc<NativeTxnCleanup>,
}

impl NativeTxnCleanupGuard {
    fn new(cleanup: Arc<NativeTxnCleanup>) -> Self {
        Self { cleanup }
    }

    async fn finish(self) {
        self.cleanup.start_and_wait().await;
    }
}

impl Drop for NativeTxnCleanupGuard {
    fn drop(&mut self) {
        self.cleanup.start();
    }
}

impl NativeSession {
    /// Run the session. The guard begins detached cleanup on normal return,
    /// panic unwinding, or task cancellation; normal completion waits for it.
    pub async fn run(self) -> crate::Result<()> {
        // The connection-scoped slots wrap the cleanup guard too, so its
        // synchronous take still reaches this connection's DDL buffer.
        crate::control::server::shared::session::conn_scope::scoped(self.run_guarded()).await
    }

    async fn run_guarded(mut self) -> crate::Result<()> {
        let guard = NativeTxnCleanupGuard::new(Arc::clone(&self.cleanup));
        let result = self.run_loop().await;
        guard.finish().await;
        result
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
                let shutdown_resp =
                    sqlstate_error(0, "57P01", "session timeout: absolute lifetime exceeded");
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
                    let err_resp = NativeResponse::error_with_code(
                        0,
                        "54000",
                        format!("frame rejected: {detail}"),
                        nodedb_types::error::ErrorCode::BAD_REQUEST.0,
                    );
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
                self.format = Some(FrameFormat::detect_payload(&payload));
            }
            let Some(format) = self.format else {
                return Err(crate::Error::BadRequest {
                    detail: "format detection failed after first frame".into(),
                });
            };

            // Decode and handle.
            let outcome = match codec::decode_request(&payload, format) {
                Ok(req) => self.handle_request(req).await,
                Err(e) => dispatch::SqlOutcome::Response(Box::new(sqlstate_error(
                    0,
                    "42601",
                    format!("{e}"),
                ))),
            };
            // Crash-injection coverage verifies that a panic after a request
            // mutates transaction state still runs detached connection cleanup.
            crate::fail_point!("native_session::after_request");

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
                    super::session_stream::emit_sql_stream(
                        &mut self.stream,
                        *sql_stream,
                        format,
                        self.state.as_ref(),
                    )
                    .await?;
                }
            }
        }
    }
}
