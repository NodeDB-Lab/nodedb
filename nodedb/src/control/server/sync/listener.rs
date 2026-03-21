//! WebSocket listener for NodeDB-Lite sync connections.
//!
//! Accepts `wss://` (or `ws://` for dev) connections on the Tokio Control
//! Plane. Each connection spawns a sync session that handles the rkyv wire
//! protocol (handshake, delta push, vector clock sync, ping/pong).
//!
//! The listener runs alongside the pgwire and HTTP listeners on the same
//! Tokio runtime. It does NOT run on the Data Plane.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::control::security::jwt::JwtConfig;

/// Configuration for the sync WebSocket listener.
#[derive(Debug, Clone)]
pub struct SyncListenerConfig {
    /// Address to listen on for sync connections.
    pub listen_addr: SocketAddr,
    /// Maximum concurrent sync sessions.
    pub max_sessions: usize,
    /// Session idle timeout in seconds.
    pub idle_timeout_secs: u64,
    /// JWT configuration for authenticating sync clients.
    pub jwt_config: JwtConfig,
}

impl Default for SyncListenerConfig {
    fn default() -> Self {
        Self {
            listen_addr: std::net::SocketAddr::from(([0, 0, 0, 0], 9090)),
            max_sessions: 1024,
            idle_timeout_secs: 300,
            jwt_config: JwtConfig::default(),
        }
    }
}

/// Sync listener state (shared across all sessions).
pub struct SyncListenerState {
    /// Active session count.
    pub active_sessions: AtomicU64,
    /// Total connections accepted.
    pub connections_accepted: AtomicU64,
    /// Total connections rejected (max sessions exceeded).
    pub connections_rejected: AtomicU64,
    /// Configuration.
    pub config: SyncListenerConfig,
}

impl SyncListenerState {
    pub fn new(config: SyncListenerConfig) -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            config,
        }
    }

    /// Whether a new session can be accepted.
    pub fn can_accept(&self) -> bool {
        self.active_sessions.load(Ordering::Relaxed) < self.config.max_sessions as u64
    }

    /// Record a new session.
    pub fn session_opened(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session closed.
    pub fn session_closed(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rejected connection.
    pub fn session_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Start the sync WebSocket listener.
///
/// This is an async function that runs on the Tokio Control Plane.
/// It accepts TCP connections, upgrades them to WebSocket, and spawns
/// a sync session task for each.
///
/// Returns a handle to the listener state for monitoring.
pub async fn start_sync_listener(
    config: SyncListenerConfig,
) -> Result<Arc<SyncListenerState>, String> {
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .map_err(|e| format!("bind sync listener to {}: {e}", config.listen_addr))?;

    let state = Arc::new(SyncListenerState::new(config));

    info!(addr = %state.config.listen_addr, "sync WebSocket listener started");

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        accept_loop(listener, state_clone).await;
    });

    Ok(state)
}

/// Accept loop: accepts TCP connections and spawns session tasks.
async fn accept_loop(listener: TcpListener, state: Arc<SyncListenerState>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                if !state.can_accept() {
                    state.session_rejected();
                    warn!(%addr, "sync: max sessions reached, rejecting");
                    continue;
                }

                state.session_opened();
                let state_clone = Arc::clone(&state);

                tokio::spawn(async move {
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => {
                            info!(%addr, "sync: WebSocket connection established");
                            handle_sync_session(ws, addr, &state_clone).await;
                        }
                        Err(e) => {
                            warn!(%addr, error = %e, "sync: WebSocket upgrade failed");
                        }
                    }
                    state_clone.session_closed();
                });
            }
            Err(e) => {
                warn!(error = %e, "sync: accept failed");
            }
        }
    }
}

/// Handle one sync session over WebSocket.
async fn handle_sync_session(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    state: &SyncListenerState,
) {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let session_id = format!(
        "sync-{addr}-{}",
        state.connections_accepted.load(Ordering::Relaxed)
    );
    let mut session = super::session::SyncSession::new(session_id.clone());
    let jwt_validator =
        crate::control::security::jwt::JwtValidator::new(state.config.jwt_config.clone());

    while let Some(msg_result) = ws.next().await {
        match msg_result {
            Ok(Message::Binary(data)) => {
                if let Some(frame) = super::wire::SyncFrame::from_bytes(&data)
                    && let Some(response) = session.process_frame(&frame, &jwt_validator)
                {
                    let response_bytes = response.to_bytes();
                    if ws
                        .send(Message::Binary(response_bytes.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                if ws.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                warn!(session = %session_id, error = %e, "sync: WebSocket error");
                break;
            }
            _ => {} // Ignore text messages, pongs, etc.
        }

        // Check idle timeout.
        if session.idle_secs() > state.config.idle_timeout_secs {
            info!(session = %session_id, "sync: idle timeout, closing");
            break;
        }
    }

    info!(
        session = %session_id,
        mutations = session.mutations_processed,
        rejected = session.mutations_rejected,
        uptime_secs = session.uptime_secs(),
        "sync: session closed"
    );
}
