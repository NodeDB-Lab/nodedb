//! WebSocket listener for NodeDB-Lite sync connections.
//!
//! Accepts `ws://` connections on the Tokio Control Plane. Each connection
//! spawns a sync session with full RLS, audit, DLQ, and rate limiting.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpListener;
use tracing::{info, warn};

use super::wire::{DeltaPushMsg, PresenceUpdateMsg, SyncMessageType, TimeseriesPushMsg};

use crate::control::security::jwt::JwtConfig;
use crate::control::state::SharedState;

use super::rate_limit::RateLimitConfig;

/// Configuration for the sync WebSocket listener.
#[derive(Debug, Clone)]
pub struct SyncListenerConfig {
    pub listen_addr: SocketAddr,
    pub max_sessions: usize,
    pub idle_timeout_secs: u64,
    pub jwt_config: JwtConfig,
    pub rate_limit: RateLimitConfig,
}

impl Default for SyncListenerConfig {
    fn default() -> Self {
        Self {
            listen_addr: std::net::SocketAddr::from(([0, 0, 0, 0], 9090)),
            max_sessions: 1024,
            idle_timeout_secs: 300,
            jwt_config: JwtConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// Sync listener state (shared across all sessions).
pub struct SyncListenerState {
    pub active_sessions: AtomicU64,
    pub connections_accepted: AtomicU64,
    pub connections_rejected: AtomicU64,
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

    pub fn can_accept(&self) -> bool {
        self.active_sessions.load(Ordering::Relaxed) < self.config.max_sessions as u64
    }

    pub fn session_opened(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_closed(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn session_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Start the sync WebSocket listener with full security context.
pub async fn start_sync_listener(
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
) -> crate::Result<Arc<SyncListenerState>> {
    let listener =
        TcpListener::bind(&config.listen_addr)
            .await
            .map_err(|e| crate::Error::Config {
                detail: format!("bind sync listener to {}: {e}", config.listen_addr),
            })?;

    let state = Arc::new(SyncListenerState::new(config));

    info!(addr = %state.config.listen_addr, "sync WebSocket listener started");

    // Spawn presence TTL sweep timer (before moving `shared` into accept loop).
    if let Some(ref shared) = shared {
        let presence = Arc::clone(&shared.presence);
        let sweep_interval_ms = presence.read().await.sweep_interval_ms();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(sweep_interval_ms));
            loop {
                interval.tick().await;
                let mut mgr = presence.write().await;
                let outbound = mgr.sweep_expired();
                let senders = mgr.senders().clone();
                drop(mgr); // Release lock before fan-out.
                outbound.send_all(&senders);
            }
        });
    }

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        accept_loop(listener, state_clone, shared).await;
    });

    Ok(state)
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<SyncListenerState>,
    shared: Option<Arc<SharedState>>,
) {
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
                let shared_clone = shared.clone();

                tokio::spawn(async move {
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => {
                            info!(%addr, "sync: WebSocket connection established");
                            handle_sync_session(ws, addr, &state_clone, shared_clone.as_deref())
                                .await;
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

/// Handle one sync session with full RLS, audit, DLQ wired in.
async fn handle_sync_session(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    state: &SyncListenerState,
    shared: Option<&SharedState>,
) {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let session_id = format!(
        "sync-{addr}-{}",
        state.connections_accepted.load(Ordering::Relaxed)
    );
    let mut session =
        super::session::SyncSession::with_rate_limit(session_id.clone(), &state.config.rate_limit);
    session.device_metadata.remote_addr = addr.to_string();

    let jwt_validator =
        crate::control::security::jwt::JwtValidator::new(state.config.jwt_config.clone());

    // CRDT sync delivery: register after authentication, unregister on disconnect.
    let mut crdt_delivery_rx: Option<
        tokio::sync::mpsc::Receiver<crate::event::crdt_sync::types::OutboundDelta>,
    > = None;
    // Control-frame channel (CollectionPurged etc.) — separate from
    // the delta queue so a saturated delta path can't drop a purge.
    let mut crdt_control_rx: Option<
        tokio::sync::mpsc::Receiver<nodedb_types::sync::wire::SyncFrame>,
    > = None;
    let mut crdt_registered = false;

    // Presence: register a bounded outbound channel for this session.
    // Presence broadcasts arrive here and are drained alongside CRDT deltas.
    let mut presence_rx: Option<tokio::sync::mpsc::Receiver<std::sync::Arc<Vec<u8>>>> = None;
    let mut presence_registered = false;

    while let Some(msg_result) = ws.next().await {
        match msg_result {
            Ok(Message::Binary(data)) => {
                if let Some(frame) = super::wire::SyncFrame::from_bytes(&data) {
                    // Handle ShapeSubscribe at listener level (needs async WAL LSN + Data Plane).
                    if frame.msg_type == SyncMessageType::ShapeSubscribe
                        && let Some(shared) = shared
                        && let Some(response) = super::async_dispatch::handle_shape_subscribe_async(
                            shared, &session, &frame,
                        )
                        .await
                    {
                        // Auto-join presence channel for this shape's collection.
                        if session.authenticated
                            && presence_registered
                            && let Some(sub_msg) =
                                frame.decode_body::<super::wire::ShapeSubscribeMsg>()
                            && let Some(coll) = sub_msg.shape.collection()
                        {
                            let channel = format!("shape:{coll}");
                            shared
                                .presence
                                .write()
                                .await
                                .subscribe_to_channel(&session_id, &channel);
                        }

                        if ws
                            .send(Message::Binary(response.to_bytes().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }

                    // Handle PresenceUpdate at listener level (needs async RwLock).
                    if frame.msg_type == SyncMessageType::PresenceUpdate
                        && session.authenticated
                        && let Some(shared) = shared
                    {
                        if let Some(msg) = frame.decode_body::<PresenceUpdateMsg>() {
                            let user_id = session.username.as_deref().unwrap_or("anonymous");
                            let mut mgr = shared.presence.write().await;
                            let outbound = mgr.handle_update(&session_id, user_id, &msg);
                            let senders = mgr.senders().clone();
                            drop(mgr); // Release lock before fan-out.
                            outbound.send_all(&senders);
                        }
                        continue;
                    }

                    // Handle TimeseriesPush directly (avoid double-decode in process_frame).
                    if frame.msg_type == SyncMessageType::TimeseriesPush {
                        if let Some(ts_msg) = frame.decode_body::<TimeseriesPushMsg>() {
                            let (ack, ingest_data) = session.handle_timeseries_push(&ts_msg);
                            // Dispatch to Data Plane if we have data and SharedState.
                            if let (Some(ingest), Some(shared)) = (ingest_data, shared) {
                                let tenant_id =
                                    session.tenant_id.unwrap_or(crate::types::TenantId::new(0));
                                let vshard =
                                    crate::types::VShardId::from_collection(&ts_msg.collection);
                                let plan = crate::bridge::envelope::PhysicalPlan::Timeseries(
                                    crate::bridge::physical_plan::TimeseriesOp::Ingest {
                                        collection: ingest.collection,
                                        payload: ingest.ilp_payload,
                                        format: "ilp".to_string(),
                                        wal_lsn: None,
                                    },
                                );
                                // Use CrdtSync source to prevent triggers on synced data.
                                let _ =
                                    crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                                        shared, tenant_id, vshard, plan, 0,
                                        crate::event::EventSource::CrdtSync,
                                    )
                                    .await;
                            }
                            let ack_bytes = ack.to_bytes();
                            if ws.send(Message::Binary(ack_bytes.into())).await.is_err() {
                                break;
                            }
                        }
                        continue; // Skip process_frame for this message type.
                    }

                    // Wire RLS, audit, DLQ from SharedState.
                    let response = if let Some(shared) = shared {
                        let rls_store = &shared.rls;
                        let mut audit = shared.audit.lock().unwrap_or_else(|p| p.into_inner());
                        let mut dlq = shared.sync_dlq.lock().unwrap_or_else(|p| p.into_inner());
                        session.process_frame(
                            &frame,
                            &jwt_validator,
                            Some(rls_store),
                            Some(&mut audit),
                            Some(&mut dlq),
                            Some(&shared.epoch_tracker),
                        )
                    } else {
                        session.process_frame(&frame, &jwt_validator, None, None, None, None)
                    };

                    if let Some(mut response) = response {
                        // For DeltaAck, run async constraint validation before sending.
                        if response.msg_type == SyncMessageType::DeltaAck
                            && let Some(shared) = shared
                            && let Some(delta_msg) = frame.decode_body::<DeltaPushMsg>()
                        {
                            response = super::async_dispatch::validate_delta_constraints(
                                shared, &delta_msg, response,
                            )
                            .await;
                        }

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

        // Register for CRDT sync delivery once authenticated.
        if session.authenticated
            && !crdt_registered
            && let Some(shared) = shared
        {
            let tenant_id = session.tenant_id.map(|t| t.as_u32()).unwrap_or(0);
            let peer_id = session.device_metadata.peer_id;
            let config = crate::event::crdt_sync::types::DeliveryConfig::default();
            let (drx, crx) = shared.crdt_sync_delivery.register(
                session_id.clone(),
                peer_id,
                tenant_id,
                Vec::new(), // All collections — shape filtering done at delivery level.
                &config,
            );
            crdt_delivery_rx = Some(drx);
            crdt_control_rx = Some(crx);
            crdt_registered = true;
        }

        // Register for presence broadcasts once authenticated.
        if session.authenticated
            && !presence_registered
            && let Some(shared) = shared
        {
            let (tx, rx) = tokio::sync::mpsc::channel(256);
            shared
                .presence
                .write()
                .await
                .register_session(session_id.clone(), super::presence::SessionSender::new(tx));
            presence_rx = Some(rx);
            presence_registered = true;
        }

        // Drain outbound presence broadcasts and push to client.
        if let Some(ref mut rx) = presence_rx {
            while let Ok(bytes) = rx.try_recv() {
                use futures::SinkExt;
                use tokio_tungstenite::tungstenite::Message;
                if ws
                    .send(Message::Binary((*bytes).clone().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        // Drain outbound control frames (CollectionPurged,
        // stream-termination, etc.) and push to Lite device. Runs
        // before the delta drain so purge notifications are never
        // starved by a delta backlog.
        if let Some(ref mut rx) = crdt_control_rx {
            while let Ok(frame) = rx.try_recv() {
                use futures::SinkExt;
                use tokio_tungstenite::tungstenite::Message;
                if ws
                    .send(Message::Binary(frame.to_bytes().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        // Drain outbound CRDT deltas and push to Lite device.
        if let Some(ref mut rx) = crdt_delivery_rx {
            while let Ok(delta) = rx.try_recv() {
                let push_msg = nodedb_types::sync::wire::DeltaPushMsg {
                    collection: delta.collection,
                    document_id: delta.document_id,
                    delta: delta.payload,
                    peer_id: delta.peer_id,
                    mutation_id: delta.sequence,
                    checksum: 0,
                };
                if let Some(frame) = nodedb_types::sync::wire::SyncFrame::new_msgpack(
                    nodedb_types::sync::wire::SyncMessageType::DeltaPush,
                    &push_msg,
                ) {
                    use futures::SinkExt;
                    use tokio_tungstenite::tungstenite::Message;
                    if ws
                        .send(Message::Binary(frame.to_bytes().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }

        if session.idle_secs() > state.config.idle_timeout_secs {
            info!(session = %session_id, "sync: idle timeout, closing");
            break;
        }
    }

    // Unregister from CRDT sync delivery.
    if crdt_registered && let Some(shared) = shared {
        shared.crdt_sync_delivery.unregister(&session_id);
    }

    // Unregister from presence: removes from all channels, broadcasts leave.
    if presence_registered && let Some(shared) = shared {
        let mut mgr = shared.presence.write().await;
        let outbound = mgr.unregister_session(&session_id);
        let senders = mgr.senders().clone();
        drop(mgr);
        outbound.send_all(&senders);
    }

    info!(
        session = %session_id,
        mutations = session.mutations_processed,
        rejected = session.mutations_rejected,
        silent_dropped = session.mutations_silent_dropped,
        uptime_secs = session.uptime_secs(),
        "sync: session closed"
    );
}
