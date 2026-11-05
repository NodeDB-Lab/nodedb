// SPDX-License-Identifier: BUSL-1.1

//! WebSocket RPC upgrade handler and connection lifecycle.
//!
//! Accepts WebSocket connections at `/v1/ws`. Clients send JSON requests
//! and receive JSON responses. Supports SQL query execution, live query
//! subscriptions, and ping/pong.
//!
//! Protocol:
//! ```json
//! // Request
//! {"id": 1, "method": "query", "params": {"sql": "SELECT * FROM users"}}
//! {"id": 2, "method": "ping"}
//! {"id": 3, "method": "live", "params": {"sql": "LIVE SELECT * FROM orders"}}
//! {"id": 4, "method": "auth", "params": {"session_id": "abc", "cursor": "v1:..."}}
//!
//! // Response
//! {"id": 1, "result": [...]}
//! {"id": 2, "result": "pong"}
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::task::JoinHandle;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use tracing::{debug, warn};

use crate::control::change_stream::LiveSubscriptionSet;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::sessions::{SessionBindRequest, SessionBinding, bind_session};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::{ConnectionFutureOutcome, isolate_connection_future};
use crate::types::DatabaseId;

use super::format;
use super::process_message::{MessageContext, process_message};

/// WebSocket upgrade handler.
///
/// Auth is resolved before the upgrade — if identity resolution fails the
/// HTTP handshake is rejected with 401/403 before any WebSocket state is
/// created. This is the only correct place to enforce auth: a post-upgrade
/// "reject the first message" approach still pins a tenant inside the handler.
pub async fn ws_handler(
    identity: ResolvedIdentity,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    let identity = identity.0;
    let peer_addr = peer.to_string();
    let database_id = match super::super::query::resolve_database_id(
        &headers,
        &super::super::query::DatabaseQueryParam::default(),
        &state,
    ) {
        Ok(database_id) => database_id,
        Err(error) => return error.into_response(),
    };
    // Blacklist + account status, no rate limit, at the upgrade: every RPC on
    // the socket runs the full gate itself (`execute_sql`), but a blacklisted
    // or suspended/banned principal must not get as far as pinning a
    // connection and its per-connection state. The upgrade is admitted once
    // and is not per-query traffic, so the rate-limited door is not the one
    // for it.
    if let Err(error) = crate::control::server::http::admission::admit_without_rate_limit(
        &state,
        &identity,
        database_id,
        &peer_addr,
    ) {
        return error.into_response();
    }
    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    if let Err(error) = authorize_database(&identity, database_id, &emitter) {
        return crate::control::server::http::auth::ApiError::Forbidden(
            crate::Error::from(error).to_string(),
        )
        .into_response();
    }
    // The bind comes before the upgrade, so a session cap refuses the handshake.
    let binding = match bind_session(
        &state.shared,
        SessionBindRequest::new(&identity, "websocket", &peer_addr).in_database(database_id),
    ) {
        Ok(binding) => binding,
        Err(error) => {
            return crate::control::server::http::auth::ApiError::from(error).into_response();
        }
    };
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);
    ws.on_upgrade(move |socket| async move {
        match isolate_connection_future(handle_ws_connection(
            socket,
            state,
            WsSession {
                identity,
                binding,
                database_id,
                trace_id,
                peer_addr,
            },
        ))
        .await
        {
            ConnectionFutureOutcome::Completed(()) => {}
            ConnectionFutureOutcome::Panicked => {
                warn!("ws-rpc connection panicked");
            }
        }
    })
    .into_response()
}

/// Owns a sender task until normal completion or connection teardown.
///
/// Dropping the guard aborts the task synchronously. This ensures a panic or
/// cancellation of the connection future cannot detach the sender task.
struct AbortOnDropJoinHandle {
    handle: Option<JoinHandle<()>>,
}

impl AbortOnDropJoinHandle {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Join the sender after the channel is closed. A sender panic is observed
    /// without inspecting its payload or logging its join error.
    async fn finish(mut self) {
        let terminated_unexpectedly = match self.handle.as_mut() {
            Some(handle) => handle.await.is_err(),
            None => false,
        };
        // Remove only after the await completes. If this future is cancelled
        // while pending, `Drop` still owns and aborts the sender task.
        self.handle.take();
        if terminated_unexpectedly {
            warn!("ws-rpc sender task terminated unexpectedly");
        }
    }
}

impl Drop for AbortOnDropJoinHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Handle a single WebSocket connection.
///
/// The connection future is boxed, once per connection. Every request path
/// nests inside it, and unboxed it can overflow the compiler's layout depth
/// limit in the upgrade task.
fn handle_ws_connection(
    socket: WebSocket,
    state: AppState,
    session: WsSession,
) -> BoxFuture<'static, ()> {
    Box::pin(serve_ws_connection(socket, state, session))
}

/// What one upgraded connection serves under.
struct WsSession {
    identity: crate::control::security::identity::AuthenticatedIdentity,
    binding: SessionBinding,
    database_id: DatabaseId,
    trace_id: nodedb_types::TraceId,
    peer_addr: String,
}

async fn serve_ws_connection(socket: WebSocket, state: AppState, session: WsSession) {
    let WsSession {
        mut identity,
        mut binding,
        database_id,
        trace_id,
        peer_addr,
    } = session;
    let (mut sender, mut receiver) = socket.split();
    let shared = Arc::clone(&state.shared);

    // Bounded channel for live notifications → WS sender.
    // 256 messages provides ~10s of buffer at 25 events/sec.
    let (live_tx, mut live_rx) = tokio::sync::mpsc::channel::<String>(256);

    let sender = AbortOnDropJoinHandle::new(tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(msg) = live_rx.recv() => {
                    if sender.send(Message::Text(msg.into())).await.is_err() {
                        debug!("WebSocket send failed; closing connection");
                        break;
                    }
                }
                else => break,
            }
        }
    }));

    // Connection-scoped live-subscription tasks. Dropping this set on
    // connection exit aborts every forwarder, which drops each captured
    // `Subscription` so `active_subscriptions` returns to 0.
    let mut live_set = LiveSubscriptionSet::new();
    // Resume forwarding is deliberately separate from LIVE SELECT forwarding.
    let mut resume_set = LiveSubscriptionSet::new();
    let mut resume_authenticated = false;

    loop {
        // The kill races only the read. A message in progress finishes, because
        // a dropped statement can stay half done.
        let msg = tokio::select! {
            biased;
            ended = binding.ended() => {
                debug!(error = %ended, "ws-rpc session killed; closing connection");
                let frame = format::error_response(serde_json::Value::Null, &ended.to_string());
                let _ = live_tx.send(frame).await;
                break;
            }
            msg = receiver.next() => match msg {
                Some(Ok(msg)) => msg,
                Some(Err(_)) | None => break,
            },
        };
        match msg {
            Message::Text(text) => {
                // A credential change reaches this message's identity.
                if let Err(error) = binding.refresh(&shared, &mut identity) {
                    let frame = format::error_response(serde_json::Value::Null, &error.to_string());
                    let _ = live_tx.send(frame).await;
                    break;
                }
                binding.touch(text.len() as u64, 0);
                binding.set_statement("websocket_request");
                let context = MessageContext {
                    shared: Arc::clone(&shared),
                    query_ctx: &state.query_ctx,
                    identity: &identity,
                    database_id,
                    trace_id,
                    live_tx: &live_tx,
                    peer_addr: &peer_addr,
                };
                let (response, authenticated) = process_message(
                    context,
                    &text,
                    &mut live_set,
                    &mut resume_set,
                    resume_authenticated,
                )
                .await;
                resume_authenticated |= authenticated;
                binding.touch(0, response.len() as u64);
                // A quiet socket with a live subscription is not idle.
                if live_set.is_empty() && resume_set.is_empty() {
                    binding.clear_statement();
                } else {
                    binding.set_statement("live_subscription");
                }

                if let Err(e) = live_tx.send(response).await {
                    debug!("response channel closed: {e}; dropping connection");
                    break;
                }
            }
            Message::Close(_) => break,
            Message::Ping(_) => {
                if let Err(e) = live_tx
                    .send(serde_json::json!({"pong": true}).to_string())
                    .await
                {
                    debug!("pong send failed: {e}; dropping connection");
                    break;
                }
            }
            _ => {}
        }
    }

    // Drop connection-scoped sets BEFORE closing the channel. Aborting
    // each forwarder drops its `Subscription`, whose `Drop` decrements
    // `active_subscriptions`, so leaked counters can't outlive the socket.
    drop(resume_set);
    drop(live_set);

    drop(live_tx);
    sender.finish().await;
    debug!("WebSocket RPC connection closed");
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio::sync::oneshot;

    use super::{AbortOnDropJoinHandle, ConnectionFutureOutcome, isolate_connection_future};

    struct AbortSignal(Option<oneshot::Sender<()>>);

    impl Drop for AbortSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn sender_is_aborted_when_guard_is_dropped() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let sender = AbortOnDropJoinHandle::new(tokio::spawn(async move {
            let _signal = AbortSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            pending::<()>().await;
        }));

        assert!(started_rx.await.is_ok());
        drop(sender);

        assert!(dropped_rx.await.is_ok());
    }

    #[tokio::test]
    async fn cancellation_while_finishing_aborts_sender() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let sender = AbortOnDropJoinHandle::new(tokio::spawn(async move {
            let _signal = AbortSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            pending::<()>().await;
        }));
        assert!(started_rx.await.is_ok());

        let finishing = tokio::spawn(sender.finish());
        tokio::task::yield_now().await;
        finishing.abort();
        let _ = finishing.await;

        assert!(dropped_rx.await.is_ok());
    }

    #[tokio::test]
    async fn sender_finishes_normally() {
        let (completed_tx, completed_rx) = oneshot::channel();
        let sender = AbortOnDropJoinHandle::new(tokio::spawn(async move {
            let _ = completed_tx.send(());
        }));

        sender.finish().await;

        assert!(completed_rx.await.is_ok());
    }

    #[tokio::test]
    async fn isolation_catches_connection_panic_and_releases_sender() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let outcome = isolate_connection_future(async move {
            let _sender = AbortOnDropJoinHandle::new(tokio::spawn(async move {
                let _signal = AbortSignal(Some(dropped_tx));
                let _ = started_tx.send(());
                pending::<()>().await;
            }));
            let _ = started_rx.await;
            panic!("simulated websocket connection panic");
        })
        .await;

        assert!(matches!(outcome, ConnectionFutureOutcome::Panicked));
        assert!(dropped_rx.await.is_ok());
    }
}
