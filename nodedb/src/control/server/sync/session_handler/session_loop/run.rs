// SPDX-License-Identifier: BUSL-1.1

//! WebSocket session loop for NodeDB-Lite sync connections.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::control::security::sessions::session_ended;
use crate::control::server::sync::listener::{SyncListenerState, SyncRegistrationCleanupGuard};
use crate::control::server::sync::session::SyncSession;
use crate::control::server::sync::wire::SyncFrame;
use crate::control::state::SharedState;

use super::channels::{Flow, SessionChannels};
use super::inbound::{InboundCtx, handle_frame};
use super::outbound::{pump_deliveries, register_channels};

/// Check the kill and bring the identity up to date for the next frame.
fn refresh_binding(
    session: &mut SyncSession,
    shared: &Option<Arc<SharedState>>,
    bytes_in: u64,
) -> crate::Result<()> {
    let (Some(state), Some(binding), Some(identity)) = (
        shared.as_ref(),
        session.binding.as_mut(),
        session.identity.as_mut(),
    ) else {
        return Ok(());
    };
    binding.refresh(state, identity)?;
    binding.touch(bytes_in, 0);
    session.tenant_id = Some(identity.tenant_id);
    Ok(())
}

/// Handle one sync session with full RLS, audit, DLQ wired in.
///
/// The session future is boxed, once per connection. Every frame path nests
/// inside it, and unboxed it can overflow the compiler's layout depth limit
/// in the listener's connection task.
pub(in crate::control::server::sync) fn handle_sync_session(
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    session_id: String,
    state: &SyncListenerState,
    shared: Option<Arc<SharedState>>,
) -> BoxFuture<'_, ()> {
    Box::pin(run_sync_session(ws, addr, session_id, state, shared))
}

async fn run_sync_session(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    session_id: String,
    state: &SyncListenerState,
    shared: Option<Arc<SharedState>>,
) {
    let cleanup = SyncRegistrationCleanupGuard::new(shared.clone(), session_id.clone());
    let mut session = crate::control::server::sync::session::SyncSession::with_rate_limit(
        session_id.clone(),
        &state.config.rate_limit,
    );
    session.device_metadata.remote_addr = addr.to_string();

    let mut channels = SessionChannels::default();

    loop {
        // Flush any outbound definition-sync frames before blocking. This
        // handles the window between registration and the next WS message.
        if let Some(ref mut rx) = channels.definition_sync_rx {
            while let Ok(frame_bytes) = rx.try_recv() {
                if ws.send(Message::Binary(frame_bytes.into())).await.is_err() {
                    return;
                }
            }
        }

        // Await the next inbound message OR a definition-sync frame, whichever
        // arrives first.  Without this select! the handler will block on
        // ws.next() indefinitely when no client traffic is expected, starving
        // the server-push delivery path.
        //
        // The kill races only this wait, so a frame in progress finishes.
        let msg_result = if let Some(ref mut rx) = channels.definition_sync_rx {
            tokio::select! {
                biased;
                ended = session_ended(&mut session.binding) => {
                    info!(session = %session_id, error = %ended, "sync: session killed, closing");
                    break;
                }
                ws_msg = ws.next() => {
                    match ws_msg {
                        Some(r) => r,
                        None => break,
                    }
                }
                frame_bytes = rx.recv() => {
                    match frame_bytes {
                        Some(bytes) => {
                            if ws.send(Message::Binary(bytes.into())).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        None => break,
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                ended = session_ended(&mut session.binding) => {
                    info!(session = %session_id, error = %ended, "sync: session killed, closing");
                    break;
                }
                ws_msg = ws.next() => match ws_msg {
                    Some(r) => r,
                    None => break,
                },
            }
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                if let Some(frame) = SyncFrame::from_bytes(&data) {
                    if let Err(error) = refresh_binding(&mut session, &shared, data.len() as u64) {
                        info!(session = %session_id, %error, "sync: session ended, closing");
                        break;
                    }
                    let ctx = InboundCtx {
                        session: &mut session,
                        channels: &mut channels,
                        shared: &shared,
                        session_id: &session_id,
                    };
                    match handle_frame(&mut ws, ctx, &frame).await {
                        Flow::Break => break,
                        Flow::Continue => {}
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let Ok(_) = ws.send(Message::Pong(data)).await else {
                    break;
                };
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                warn!(session = %session_id, error = %e, "sync: WebSocket error");
                break;
            }
            _ => {}
        }

        register_channels(&session, &mut channels, &shared, &session_id).await;

        if pump_deliveries(&mut ws, &mut session, &mut channels, &shared).await == Flow::Break {
            break;
        }

        if session.idle_secs() > state.config.idle_timeout_secs {
            info!(session = %session_id, "sync: idle timeout, closing");
            break;
        }
    }

    cleanup.finish().await;

    state.fold_closed_session(session.mutations_deduplicated, session.ops_trimmed);

    info!(
        session = %session_id,
        admitted = session.mutations_processed,
        applied = session.mutations_applied,
        rejected = session.mutations_rejected,
        not_applied = session.mutations_not_applied,
        deduplicated = session.mutations_deduplicated,
        silent_dropped = session.mutations_silent_dropped,
        ops_trimmed = session.ops_trimmed,
        uptime_secs = session.uptime_secs(),
        "sync: session closed"
    );
}
