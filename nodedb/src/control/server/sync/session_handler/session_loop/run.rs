// SPDX-License-Identifier: BUSL-1.1

//! WebSocket session loop for NodeDB-Lite sync connections.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::control::security::sessions::session_ended;
use crate::control::server::sync::listener::{SyncListenerState, SyncRegistrationCleanupGuard};
use crate::control::server::sync::security::now_epoch_ms;
use crate::control::server::sync::session::SyncSession;
use crate::control::server::sync::wire::SyncFrame;
use crate::control::state::SharedState;

use super::channels::{Flow, SessionChannels};
use super::inbound::{InboundCtx, handle_frame, reset_session_bindings};
use super::outbound::pump_deliveries;
use super::outbound_queue::QueueWake;
use super::register::register_channels;
use super::resync::resync_dropped;
use super::send::{send_frame, send_message};

/// The tenant and database a session's pushes are registered under.
type PushScope = (Option<crate::types::TenantId>, crate::types::DatabaseId);

fn push_scope(session: &SyncSession) -> PushScope {
    (session.tenant_id, session.database_id())
}

/// Release every push registration of the session when a refresh moved its
/// principal out of the scope `before` names, and forget the collections and
/// presence channels announced under it. The loop registers again under the
/// new scope at once. Returns whether the scope moved.
async fn reset_on_scope_change(
    before: PushScope,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
) -> bool {
    if push_scope(session) == before {
        return false;
    }
    info!(
        session = %session.session_id,
        "sync: the principal's scope changed; push registrations reset"
    );
    reset_session_bindings(channels, shared, &session.session_id).await;
    session.announced_collections.clear();
    session.presence_shapes.clear();
    true
}

/// Check the kill and bring the identity up to date, with no activity
/// recorded. Runs before every push drain, so a revoked API key, a dropped
/// user or a lowered role stops the pushes even on a client that sends only
/// pings.
fn refresh_identity(
    session: &mut SyncSession,
    shared: &Option<Arc<SharedState>>,
) -> crate::Result<()> {
    let (Some(state), Some(binding), Some(identity)) = (
        shared.as_ref(),
        session.binding.as_mut(),
        session.identity.as_mut(),
    ) else {
        return Ok(());
    };
    binding.refresh(state, identity)?;
    session.tenant_id = Some(identity.tenant_id);
    Ok(())
}

/// Check the kill, bring the identity up to date for the next frame, and
/// record the frame as session activity.
fn refresh_binding(
    session: &mut SyncSession,
    shared: &Option<Arc<SharedState>>,
    bytes_in: u64,
) -> crate::Result<()> {
    refresh_identity(session, shared)?;
    if let Some(binding) = session.binding.as_ref() {
        binding.touch(bytes_in, 0);
    }
    Ok(())
}

/// What woke the session loop.
enum Wake {
    Inbound(Result<Message, tokio_tungstenite::tungstenite::Error>),
    Outbound(QueueWake),
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
    let mut session = SyncSession::with_rate_limit(session_id.clone(), &state.config.rate_limit);
    session.device_metadata.remote_addr = addr.to_string();

    let mut channels = SessionChannels::default();

    loop {
        // One wait covers the kill, the next inbound message and every
        // outbound queue, so a push goes out with no inbound frame to carry
        // it. The kill races only this wait, so a frame in progress finishes.
        let wake = tokio::select! {
            biased;
            ended = session_ended(&mut session.binding) => {
                info!(session = %session_id, error = %ended, "sync: session killed, closing");
                break;
            }
            ws_msg = ws.next() => match ws_msg {
                Some(r) => Wake::Inbound(r),
                None => break,
            },
            queue = channels.wait_outbound() => Wake::Outbound(queue),
        };

        let msg_result = match wake {
            Wake::Inbound(msg_result) => Some(msg_result),
            Wake::Outbound(QueueWake::Ready) => None,
            Wake::Outbound(QueueWake::Closed) => {
                info!(session = %session_id, "sync: a push registration closed, closing");
                break;
            }
        };

        if let Some(msg_result) = msg_result {
            match msg_result {
                Ok(Message::Binary(data)) => {
                    if let Some(frame) = SyncFrame::from_bytes(&data) {
                        let before = push_scope(&session);
                        if let Err(error) =
                            refresh_binding(&mut session, &shared, data.len() as u64)
                        {
                            info!(session = %session_id, %error, "sync: session ended, closing");
                            break;
                        }
                        reset_on_scope_change(before, &mut session, &mut channels, &shared).await;
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
                    if !send_message(&mut ws, &session, Message::Pong(data)).await {
                        break;
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    warn!(session = %session_id, error = %e, "sync: WebSocket error");
                    break;
                }
                _ => {}
            }
        }

        let before = push_scope(&session);
        if let Err(error) = refresh_identity(&mut session, &shared) {
            info!(session = %session_id, %error, "sync: session ended, closing");
            break;
        }
        reset_on_scope_change(before, &mut session, &mut channels, &shared).await;

        register_channels(&session, &mut channels, &shared, &session_id).await;

        if pump_deliveries(&mut ws, &mut session, &mut channels, &shared).await == Flow::Break {
            break;
        }

        if resync_dropped(&mut ws, &mut session, &mut channels, &shared).await == Flow::Break {
            break;
        }

        // Every wake, inbound or outbound, runs the refresh hint and the idle
        // check.
        if let Some(state) = shared.as_ref()
            && let Some(hint) = session.token_refresh_hint(
                state.tuning.network.token_refresh_window_secs,
                now_epoch_ms(),
            )
            && !send_frame(&mut ws, &session, &hint).await
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_refresh_scope_change_resets_the_bindings() {
        let mut session = SyncSession::new("scope-change".into());
        session.tenant_id = Some(crate::types::TenantId::new(1));
        let mut channels = SessionChannels {
            crdt_registered: true,
            presence_registered: true,
            ..Default::default()
        };
        session.presence_shapes.insert("orders".into());
        let before = push_scope(&session);

        assert!(!reset_on_scope_change(before, &mut session, &mut channels, &None).await);
        assert!(
            channels.crdt_registered,
            "an unchanged scope keeps the bindings"
        );

        session.tenant_id = Some(crate::types::TenantId::new(2));
        assert!(reset_on_scope_change(before, &mut session, &mut channels, &None).await);
        assert!(!channels.crdt_registered);
        assert!(!channels.presence_registered);
        assert!(session.presence_shapes.is_empty());
    }
}
