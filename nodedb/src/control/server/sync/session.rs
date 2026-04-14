//! Sync session: handles one WebSocket connection from a NodeDB-Lite client.
//!
//! Processes incoming frames (handshake, delta push, vector clock sync,
//! ping/pong) and sends responses. Each session is authenticated via JWT
//! and scoped to a single tenant.

use std::collections::HashMap;
use std::time::Instant;

use tracing::{debug, info, warn};

use crate::control::security::audit::AuditLog;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::JwtValidator;
use crate::control::security::rls::RlsPolicyStore;
use crate::types::TenantId;

use super::dlq::{DeviceMetadata, DlqEnqueueParams, SyncDlq, ViolationType};
use super::rate_limit::{RateLimitConfig, SyncRateLimiter};
use super::security::{SyncRejectionReason, enforce_rls_on_delta, log_silent_rejection};
use super::wire::*;

/// State of a single sync session (one WebSocket connection).
pub struct SyncSession {
    /// Unique session ID.
    pub session_id: String,
    /// Authenticated tenant.
    pub tenant_id: Option<TenantId>,
    /// Authenticated username.
    pub username: Option<String>,
    /// Full authenticated identity (set after handshake).
    pub identity: Option<AuthenticatedIdentity>,
    /// Whether the handshake completed successfully.
    pub authenticated: bool,
    /// Client's vector clock per collection.
    pub client_clock: HashMap<String, HashMap<String, u64>>,
    /// Server's vector clock per collection (latest LSN).
    pub server_clock: HashMap<String, u64>,
    /// Subscribed shape IDs.
    pub subscribed_shapes: Vec<String>,
    /// Mutations processed in this session.
    pub mutations_processed: u64,
    /// Mutations rejected in this session.
    pub mutations_rejected: u64,
    /// Mutations silently dropped (security rejections).
    pub mutations_silent_dropped: u64,
    /// Last activity timestamp.
    pub last_activity: Instant,
    /// Session creation time.
    pub created_at: Instant,
    /// Per-session rate limiter.
    pub rate_limiter: SyncRateLimiter,
    /// Device metadata from handshake (for DLQ entries).
    pub device_metadata: DeviceMetadata,
    /// Per-peer replay deduplication: highest mutation_id successfully processed.
    /// Key = peer_id, Value = highest mutation_id seen from that peer.
    /// Deltas with mutation_id <= this value are idempotently skipped.
    pub last_seen_mutation: HashMap<u64, u64>,
}

impl SyncSession {
    pub fn new(session_id: String) -> Self {
        Self::with_rate_limit(session_id, &RateLimitConfig::default())
    }

    pub fn with_rate_limit(session_id: String, rate_config: &RateLimitConfig) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            tenant_id: None,
            username: None,
            identity: None,
            authenticated: false,
            client_clock: HashMap::new(),
            server_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            mutations_processed: 0,
            mutations_rejected: 0,
            mutations_silent_dropped: 0,
            last_activity: now,
            created_at: now,
            rate_limiter: SyncRateLimiter::new(rate_config),
            device_metadata: DeviceMetadata::default(),
            last_seen_mutation: HashMap::new(),
        }
    }

    /// Process a handshake message: validate JWT, store client clock, detect forks.
    ///
    /// `epoch_tracker` maps `lite_id → last_seen_epoch` for fork detection.
    /// Returns a HandshakeAck frame to send back to the client.
    pub fn handle_handshake(
        &mut self,
        msg: &HandshakeMsg,
        jwt_validator: &JwtValidator,
        current_server_clock: HashMap<String, u64>,
        epoch_tracker: Option<&std::sync::Mutex<HashMap<String, u64>>>,
    ) -> SyncFrame {
        self.last_activity = Instant::now();

        // Wire format compatibility check: reject incompatible clients early.
        // wire_version == 0 means legacy client (pre-wire-version), treat as v1.
        let client_wire = if msg.wire_version == 0 {
            crate::version::LEGACY_CLIENT_WIRE_VERSION
        } else {
            msg.wire_version
        };
        if let Err(e) = crate::version::check_wire_compatibility(client_wire) {
            warn!(
                session = %self.session_id,
                client_wire_version = client_wire,
                error = %e,
                "sync handshake rejected: incompatible wire version"
            );
            let ack = HandshakeAckMsg {
                success: false,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: Some(format!("incompatible wire version: {e}")),
                fork_detected: false,
                server_wire_version: crate::version::WIRE_FORMAT_VERSION,
            };
            return SyncFrame::encode_or_empty(SyncMessageType::HandshakeAck, &ack);
        }

        // Trust mode: empty token auto-authenticates as default identity.
        if msg.jwt_token.is_empty() {
            let identity = AuthenticatedIdentity {
                user_id: 0,
                username: "sync-client".into(),
                tenant_id: TenantId::new(0),
                auth_method: crate::control::security::identity::AuthMethod::Trust,
                roles: vec![crate::control::security::identity::Role::ReadWrite],
                is_superuser: false,
            };
            self.tenant_id = Some(identity.tenant_id);
            self.username = Some(identity.username.clone());
            self.identity = Some(identity);
            self.authenticated = true;
            self.client_clock = msg.vector_clock.clone();
            self.subscribed_shapes = msg.subscribed_shapes.clone();
            self.server_clock = current_server_clock.clone();
            self.device_metadata = DeviceMetadata {
                client_version: msg.client_version.clone(),
                remote_addr: String::new(),
                peer_id: 0,
            };

            // Fork detection.
            if let Some(fork_ack) =
                self.check_fork_detection(msg, &current_server_clock, epoch_tracker)
            {
                return fork_ack;
            }

            info!(session = %self.session_id, "sync handshake OK (trust mode)");

            let ack = HandshakeAckMsg {
                success: true,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: None,
                fork_detected: false,
                server_wire_version: crate::version::WIRE_FORMAT_VERSION,
            };
            return SyncFrame::encode_or_empty(SyncMessageType::HandshakeAck, &ack);
        }

        // Validate JWT.
        match jwt_validator.validate(&msg.jwt_token) {
            Ok(identity) => {
                self.tenant_id = Some(identity.tenant_id);
                self.username = Some(identity.username.clone());
                self.identity = Some(identity.clone());
                self.authenticated = true;
                self.client_clock = msg.vector_clock.clone();
                self.subscribed_shapes = msg.subscribed_shapes.clone();
                self.server_clock = current_server_clock.clone();
                self.device_metadata = DeviceMetadata {
                    client_version: msg.client_version.clone(),
                    remote_addr: String::new(), // Filled by listener.
                    peer_id: 0,                 // Set on first delta push.
                };

                // Fork detection.
                if let Some(fork_ack) =
                    self.check_fork_detection(msg, &current_server_clock, epoch_tracker)
                {
                    return fork_ack;
                }

                info!(
                    session = %self.session_id,
                    user = %identity.username,
                    tenant = identity.tenant_id.as_u32(),
                    shapes = self.subscribed_shapes.len(),
                    "sync handshake OK"
                );

                let ack = HandshakeAckMsg {
                    success: true,
                    session_id: self.session_id.clone(),
                    server_clock: current_server_clock,
                    error: None,
                    fork_detected: false,
                    server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                };
                SyncFrame::encode_or_empty(SyncMessageType::HandshakeAck, &ack)
            }
            Err(e) => {
                warn!(
                    session = %self.session_id,
                    error = %e,
                    "sync handshake FAILED"
                );

                let ack = HandshakeAckMsg {
                    success: false,
                    session_id: self.session_id.clone(),
                    server_clock: HashMap::new(),
                    error: Some(e.to_string()),
                    fork_detected: false,
                    server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                };
                SyncFrame::encode_or_empty(SyncMessageType::HandshakeAck, &ack)
            }
        }
    }

    /// Check fork detection for a handshake message.
    ///
    /// If `lite_id` is non-empty and `epoch <= last_seen_epoch`, this is a cloned
    /// device reusing a database backup. Returns a FORK_DETECTED rejection.
    /// Otherwise, updates the epoch tracker and returns None (proceed normally).
    fn check_fork_detection(
        &self,
        msg: &HandshakeMsg,
        server_clock: &HashMap<String, u64>,
        epoch_tracker: Option<&std::sync::Mutex<HashMap<String, u64>>>,
    ) -> Option<SyncFrame> {
        if msg.lite_id.is_empty() || msg.epoch == 0 {
            return None; // Legacy client, skip fork detection.
        }

        let tracker = epoch_tracker?;

        let mut epochs = tracker.lock().unwrap_or_else(|p| p.into_inner());

        if let Some(&last_epoch) = epochs.get(&msg.lite_id)
            && msg.epoch <= last_epoch
        {
            warn!(
                session = %self.session_id,
                lite_id = %msg.lite_id,
                epoch = msg.epoch,
                last_seen = last_epoch,
                "FORK DETECTED: stale epoch from cloned device"
            );
            let ack = HandshakeAckMsg {
                success: false,
                session_id: self.session_id.clone(),
                server_clock: server_clock.clone(),
                error: Some("FORK_DETECTED: regenerate LiteId and reconnect".into()),
                fork_detected: true,
                server_wire_version: crate::version::WIRE_FORMAT_VERSION,
            };
            return Some(SyncFrame::encode_or_empty(
                SyncMessageType::HandshakeAck,
                &ack,
            ));
        }

        // Update tracker with this epoch.
        epochs.insert(msg.lite_id.clone(), msg.epoch);
        info!(
            session = %self.session_id,
            lite_id = %msg.lite_id,
            epoch = msg.epoch,
            "epoch tracker updated"
        );
        None
    }

    /// Process a delta push: validate, enforce security, and prepare for WAL commit.
    ///
    /// Returns `Some(SyncFrame)` with either DeltaAck or DeltaReject.
    /// Returns `None` when the mutation is silently dropped (security rejection).
    ///
    /// The `rls_store`, `audit_log`, and `dlq` parameters are optional:
    /// when `None`, the corresponding security check is skipped.
    pub fn handle_delta_push(
        &mut self,
        msg: &DeltaPushMsg,
        rls_store: Option<&RlsPolicyStore>,
        audit_log: Option<&mut AuditLog>,
        dlq: Option<&mut SyncDlq>,
    ) -> Option<SyncFrame> {
        self.last_activity = Instant::now();

        if !self.authenticated {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "not authenticated".into(),
                compensation: Some(CompensationHint::PermissionDenied),
            };
            return Some(SyncFrame::encode_or_empty(
                SyncMessageType::DeltaReject,
                &reject,
            ));
        }

        if msg.delta.is_empty() {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "empty delta".into(),
                compensation: None,
            };
            return Some(SyncFrame::encode_or_empty(
                SyncMessageType::DeltaReject,
                &reject,
            ));
        }

        // CRC32C integrity check (skip for legacy clients with checksum=0).
        if msg.checksum != 0 {
            let computed = crc32c::crc32c(&msg.delta);
            if computed != msg.checksum {
                self.mutations_rejected += 1;
                warn!(
                    session = %self.session_id,
                    mutation_id = msg.mutation_id,
                    expected = msg.checksum,
                    computed,
                    "CRC32C checksum mismatch on delta payload"
                );
                let reject = DeltaRejectMsg {
                    mutation_id: msg.mutation_id,
                    reason: format!(
                        "CRC32C mismatch: expected {:#010x}, computed {:#010x}",
                        msg.checksum, computed
                    ),
                    compensation: Some(CompensationHint::IntegrityViolation),
                };
                return Some(SyncFrame::encode_or_empty(
                    SyncMessageType::DeltaReject,
                    &reject,
                ));
            }
        }

        // Update device metadata peer_id on first delta.
        if self.device_metadata.peer_id == 0 {
            self.device_metadata.peer_id = msg.peer_id;
        }

        // --- Replay deduplication ---
        // If we've already processed this mutation_id from this peer, send a
        // duplicate ACK without re-applying. This handles reconnect scenarios
        // where Lite re-sends unACK'd deltas that Origin already committed.
        if let Some(&last_seen) = self.last_seen_mutation.get(&msg.peer_id)
            && msg.mutation_id <= last_seen
        {
            debug!(
                session = %self.session_id,
                peer_id = msg.peer_id,
                mutation_id = msg.mutation_id,
                last_seen,
                "replay dedup: skipping already-processed delta"
            );
            let ack = DeltaAckMsg {
                mutation_id: msg.mutation_id,
                lsn: 0, // Already committed; exact LSN not tracked per-mutation.
            };
            return Some(SyncFrame::encode_or_empty(SyncMessageType::DeltaAck, &ack));
        }

        let identity = match &self.identity {
            Some(id) => id.clone(),
            None => {
                // Should not happen if authenticated, but guard.
                self.mutations_rejected += 1;
                let reject = DeltaRejectMsg {
                    mutation_id: msg.mutation_id,
                    reason: "identity not established".into(),
                    compensation: Some(CompensationHint::PermissionDenied),
                };
                return Some(SyncFrame::encode_or_empty(
                    SyncMessageType::DeltaReject,
                    &reject,
                ));
            }
        };

        // --- Rate limiting ---
        if let Err(retry_after_ms) = self.rate_limiter.try_acquire() {
            let reason = SyncRejectionReason::RateLimited { retry_after_ms };

            // Silent rejection: no frame sent to client.
            if let Some(audit) = audit_log {
                log_silent_rejection(audit, &self.session_id, &identity, msg, &reason);
            }
            if let Some(q) = dlq {
                q.enqueue(DlqEnqueueParams {
                    session_id: self.session_id.clone(),
                    tenant_id: identity.tenant_id.as_u32(),
                    username: identity.username.clone(),
                    collection: msg.collection.clone(),
                    document_id: msg.document_id.clone(),
                    mutation_id: msg.mutation_id,
                    peer_id: msg.peer_id,
                    delta: msg.delta.clone(),
                    violation_type: ViolationType::RateLimited,
                    compensation: Some(CompensationHint::RateLimited { retry_after_ms }),
                    device_metadata: self.device_metadata.clone(),
                });
            }

            self.mutations_silent_dropped += 1;
            return None; // Silent drop — no frame to client.
        }

        // --- RLS enforcement ---
        if let Some(rls) = rls_store
            && let Err(reason) = enforce_rls_on_delta(msg, &identity, rls)
        {
            // Silent rejection with audit.
            if let Some(audit) = audit_log {
                log_silent_rejection(audit, &self.session_id, &identity, msg, &reason);
            }
            if let Some(q) = dlq {
                let violation = match &reason {
                    SyncRejectionReason::RlsPolicyViolation { policy_name } => {
                        ViolationType::RlsPolicyViolation {
                            policy_name: policy_name.clone(),
                        }
                    }
                    _ => ViolationType::PermissionDenied,
                };
                q.enqueue(DlqEnqueueParams {
                    session_id: self.session_id.clone(),
                    tenant_id: identity.tenant_id.as_u32(),
                    username: identity.username.clone(),
                    collection: msg.collection.clone(),
                    document_id: msg.document_id.clone(),
                    mutation_id: msg.mutation_id,
                    peer_id: msg.peer_id,
                    delta: msg.delta.clone(),
                    violation_type: violation,
                    compensation: Some(CompensationHint::PermissionDenied),
                    device_metadata: self.device_metadata.clone(),
                });
            }

            self.mutations_silent_dropped += 1;
            return None; // Silent drop.
        }

        // Delta passes all checks (RLS, rate limit, integrity, dedup) — accept it.
        // CRDT constraint validation (UNIQUE, FK) happens asynchronously in
        // the listener after the delta is dispatched to the Data Plane.
        // If the constraint check fails, the delta is retroactively rejected
        // and a CompensationHint is sent back to the client.
        self.mutations_processed += 1;

        // Track highest mutation_id per peer for replay deduplication.
        self.last_seen_mutation
            .entry(msg.peer_id)
            .and_modify(|v| *v = (*v).max(msg.mutation_id))
            .or_insert(msg.mutation_id);
        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            doc = %msg.document_id,
            mutation_id = msg.mutation_id,
            delta_bytes = msg.delta.len(),
            "delta push accepted"
        );

        let ack = DeltaAckMsg {
            mutation_id: msg.mutation_id,
            lsn: 0, // Caller fills with actual WAL LSN after commit.
        };
        Some(SyncFrame::encode_or_empty(SyncMessageType::DeltaAck, &ack))
    }

    /// Process a vector clock sync message.
    ///
    /// Updates the server's view of the client's clock and returns
    /// the server's current clock.
    pub fn handle_vector_clock_sync(&mut self, msg: &VectorClockSyncMsg) -> SyncFrame {
        self.last_activity = Instant::now();

        // Update server's view of which collections the client knows about.
        for (collection, lsn) in &msg.clocks {
            self.server_clock
                .entry(collection.clone())
                .and_modify(|v| *v = (*v).max(*lsn))
                .or_insert(*lsn);
        }

        debug!(
            session = %self.session_id,
            collections = msg.clocks.len(),
            "vector clock sync"
        );

        let response = VectorClockSyncMsg {
            clocks: self.server_clock.clone(),
            sender_id: 0, // Server node ID (filled by caller).
        };
        SyncFrame::encode_or_empty(SyncMessageType::VectorClockSync, &response)
    }

    pub fn handle_ping(&mut self, msg: &PingPongMsg) -> SyncFrame {
        self.last_activity = Instant::now();

        let pong = PingPongMsg {
            timestamp_ms: msg.timestamp_ms,
            is_pong: true,
        };
        SyncFrame::encode_or_empty(SyncMessageType::PingPong, &pong)
    }

    /// Process an incoming frame and return a response frame (if any).
    ///
    /// Security context parameters are optional — when provided, per-delta
    /// RLS enforcement, rate limiting, silent rejection, and DLQ persistence
    /// are active. When `None`, the session operates in permissive mode
    /// (useful for testing or internal replication channels).
    pub fn process_frame(
        &mut self,
        frame: &SyncFrame,
        jwt_validator: &JwtValidator,
        rls_store: Option<&RlsPolicyStore>,
        audit_log: Option<&mut AuditLog>,
        dlq: Option<&mut SyncDlq>,
        epoch_tracker: Option<&std::sync::Mutex<HashMap<String, u64>>>,
    ) -> Option<SyncFrame> {
        match frame.msg_type {
            SyncMessageType::Handshake => {
                let msg: HandshakeMsg = frame.decode_body()?;
                Some(self.handle_handshake(
                    &msg,
                    jwt_validator,
                    self.server_clock.clone(),
                    epoch_tracker,
                ))
            }
            SyncMessageType::DeltaPush => {
                let msg: DeltaPushMsg = frame.decode_body()?;
                self.handle_delta_push(&msg, rls_store, audit_log, dlq)
            }
            SyncMessageType::VectorClockSync => {
                let msg: VectorClockSyncMsg = frame.decode_body()?;
                Some(self.handle_vector_clock_sync(&msg))
            }
            SyncMessageType::ShapeSubscribe => {
                let msg: super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
                let registry = super::shape::registry::ShapeRegistry::new();
                let tenant_id = self.tenant_id.map(|t| t.as_u32()).unwrap_or(0);
                let current_lsn = self.server_clock.values().copied().max().unwrap_or(0);
                let response = super::shape::handler::handle_subscribe(
                    &self.session_id,
                    tenant_id,
                    &msg,
                    &registry,
                    current_lsn,
                    |_shape, _lsn| super::shape::handler::ShapeSnapshotData::empty(),
                );
                Some(response)
            }
            SyncMessageType::ShapeUnsubscribe => {
                let msg: super::shape::handler::ShapeUnsubscribeMsg = frame.decode_body()?;
                let registry = super::shape::registry::ShapeRegistry::new();
                super::shape::handler::handle_unsubscribe(&self.session_id, &msg, &registry);
                None // No response to client.
            }
            SyncMessageType::TimeseriesPush => {
                let msg: TimeseriesPushMsg = frame.decode_body()?;
                let (ack, _ingest_data) = self.handle_timeseries_push(&msg);
                // The caller (listener) is responsible for dispatching _ingest_data
                // to the Data Plane via dispatch_to_data_plane(TimeseriesIngest).
                // For now, the ingest data is available via handle_timeseries_push
                // directly when the listener needs it.
                Some(ack)
            }
            SyncMessageType::TimeseriesAck => None, // Client-side only.
            SyncMessageType::ResyncRequest => {
                // Client detected a sequence gap or checksum failure.
                // Log the request — full re-sync is handled by shape subscription.
                if let Some(msg) = frame.decode_body::<ResyncRequestMsg>() {
                    warn!(
                        session = %self.session_id,
                        reason = ?msg.reason,
                        from_mutation_id = msg.from_mutation_id,
                        collection = %msg.collection,
                        "client requested re-sync"
                    );
                }
                None // Re-sync is initiated via shape re-subscribe, not a direct response.
            }
            SyncMessageType::TokenRefresh => {
                let msg: TokenRefreshMsg = frame.decode_body()?;
                Some(self.handle_token_refresh(&msg, jwt_validator))
            }
            SyncMessageType::Throttle => {
                if let Some(msg) = frame.decode_body::<ThrottleMsg>() {
                    info!(
                        session = %self.session_id,
                        throttle = msg.throttle,
                        queue_depth = msg.queue_depth,
                        suggested_rate = msg.suggested_rate,
                        "client throttle signal received"
                    );
                }
                None // Informational — Origin adjusts push rate internally.
            }
            SyncMessageType::PingPong => {
                let msg: PingPongMsg = frame.decode_body()?;
                if msg.is_pong {
                    None
                } else {
                    Some(self.handle_ping(&msg))
                }
            }
            // Presence frames are handled at the listener level (need async RwLock).
            // If they reach here, it means the session is unauthenticated — ignore.
            SyncMessageType::PresenceUpdate
            | SyncMessageType::PresenceBroadcast
            | SyncMessageType::PresenceLeave => {
                debug!(
                    session = %self.session_id,
                    msg_type = frame.msg_type as u8,
                    "presence frame ignored (handled at listener level)"
                );
                None
            }
            _ => {
                warn!(
                    session = %self.session_id,
                    msg_type = frame.msg_type as u8,
                    "unhandled sync message type"
                );
                None
            }
        }
    }

    /// Handle a token refresh request: validate the new JWT, upgrade the session.
    ///
    /// If the new token is valid and belongs to the same tenant, the session
    /// continues with the new credentials. If invalid, the session stays on the
    /// old credentials (no disruption) and sends an error response.
    pub fn handle_token_refresh(
        &mut self,
        msg: &TokenRefreshMsg,
        jwt_validator: &JwtValidator,
    ) -> SyncFrame {
        self.last_activity = Instant::now();

        if msg.new_token.is_empty() {
            let ack = TokenRefreshAckMsg {
                success: false,
                error: Some("empty token".into()),
                expires_in_secs: 0,
            };
            return SyncFrame::encode_or_empty(SyncMessageType::TokenRefreshAck, &ack);
        }

        match jwt_validator.validate(&msg.new_token) {
            Ok(new_identity) => {
                // Verify the new token is for the same tenant (prevent tenant escalation).
                if let Some(current_tenant) = self.tenant_id
                    && new_identity.tenant_id != current_tenant
                {
                    warn!(
                        session = %self.session_id,
                        current_tenant = current_tenant.as_u32(),
                        new_tenant = new_identity.tenant_id.as_u32(),
                        "token refresh rejected: tenant mismatch"
                    );
                    let ack = TokenRefreshAckMsg {
                        success: false,
                        error: Some("tenant mismatch".into()),
                        expires_in_secs: 0,
                    };
                    return SyncFrame::encode_or_empty(SyncMessageType::TokenRefreshAck, &ack);
                }

                // Upgrade the session with the new identity.
                self.username = Some(new_identity.username.clone());
                self.identity = Some(new_identity);

                info!(
                    session = %self.session_id,
                    "JWT token refreshed successfully"
                );

                let ack = TokenRefreshAckMsg {
                    success: true,
                    error: None,
                    expires_in_secs: 3600, // Default 1h; real value from JWT claims.
                };
                SyncFrame::encode_or_empty(SyncMessageType::TokenRefreshAck, &ack)
            }
            Err(e) => {
                warn!(
                    session = %self.session_id,
                    error = %e,
                    "token refresh FAILED — keeping existing credentials"
                );
                let ack = TokenRefreshAckMsg {
                    success: false,
                    error: Some(e.to_string()),
                    expires_in_secs: 0,
                };
                SyncFrame::encode_or_empty(SyncMessageType::TokenRefreshAck, &ack)
            }
        }
    }

    /// Session uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Seconds since last activity.
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::jwt::JwtConfig;
    use crate::control::security::rls::{PolicyType, RlsPolicy};

    fn make_session() -> SyncSession {
        SyncSession::new("test-session-1".into())
    }

    fn make_authenticated_session() -> SyncSession {
        let mut session = make_session();
        session.authenticated = true;
        session.tenant_id = Some(TenantId::new(1));
        session.username = Some("alice".into());
        session.identity = Some(AuthenticatedIdentity {
            user_id: 1,
            username: "alice".into(),
            tenant_id: TenantId::new(1),
            auth_method: crate::control::security::identity::AuthMethod::ApiKey,
            roles: vec![crate::control::security::identity::Role::ReadWrite],
            is_superuser: false,
        });
        session
    }

    #[test]
    fn handshake_rejects_invalid_jwt() {
        let mut session = make_session();
        let validator = JwtValidator::new(JwtConfig::default());

        let msg = HandshakeMsg {
            jwt_token: "invalid.token.here".into(),
            vector_clock: HashMap::new(),
            subscribed_shapes: vec![],
            client_version: "0.1".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: 1,
        };

        let response = session.handle_handshake(&msg, &validator, HashMap::new(), None);
        assert_eq!(response.msg_type, SyncMessageType::HandshakeAck);

        let ack: HandshakeAckMsg = response.decode_body().unwrap();
        assert!(!ack.success);
        assert!(ack.error.is_some());
        assert!(!session.authenticated);
    }

    #[test]
    fn delta_push_rejected_before_auth() {
        let mut session = make_session();

        let msg = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: vec![1, 2, 3],
            peer_id: 1,
            mutation_id: 100,
            checksum: 0,
        };

        let response = session.handle_delta_push(&msg, None, None, None);
        assert!(response.is_some());
        let frame = response.unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
    }

    #[test]
    fn delta_push_accepted_when_authenticated() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"status": "active"});
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
        };

        let response = session.handle_delta_push(&msg, None, None, None);
        assert!(response.is_some());
        assert_eq!(response.unwrap().msg_type, SyncMessageType::DeltaAck);
        assert_eq!(session.mutations_processed, 1);
    }

    #[test]
    fn delta_push_rls_silent_rejection() {
        let mut session = make_authenticated_session();

        // Create an RLS write policy requiring status=active.
        let rls_store = RlsPolicyStore::new();
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("active".into()),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();
        rls_store
            .create_policy(RlsPolicy {
                name: "require_active".into(),
                collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                predicate,
                compiled_predicate: None,
                mode: crate::control::security::predicate::PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .unwrap();

        let mut audit_log = AuditLog::new(100);
        let mut dlq = SyncDlq::new(super::super::dlq::DlqConfig::default());

        // Delta with status=draft → should be silently dropped.
        let data = serde_json::json!({"status": "draft"});
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
        };

        let response =
            session.handle_delta_push(&msg, Some(&rls_store), Some(&mut audit_log), Some(&mut dlq));

        // Silent rejection: no frame returned to client.
        assert!(response.is_none());
        assert_eq!(session.mutations_silent_dropped, 1);
        assert_eq!(session.mutations_processed, 0);

        // Audit log has the rejection.
        assert_eq!(audit_log.len(), 1);

        // DLQ has the entry.
        assert_eq!(dlq.total_entries(), 1);
        let entries = dlq.entries_for_collection(1, "orders");
        assert_eq!(entries[0].mutation_id, 42);
    }

    #[test]
    fn delta_push_rate_limited_silent_drop() {
        let rate_config = RateLimitConfig {
            rate_per_sec: 0.0, // No refill.
            burst: 1,          // Only 1 token.
        };
        let mut session = SyncSession::with_rate_limit("rate-test".into(), &rate_config);
        session.authenticated = true;
        session.tenant_id = Some(TenantId::new(1));
        session.username = Some("bob".into());
        session.identity = Some(AuthenticatedIdentity {
            user_id: 2,
            username: "bob".into(),
            tenant_id: TenantId::new(1),
            auth_method: crate::control::security::identity::AuthMethod::ApiKey,
            roles: vec![crate::control::security::identity::Role::ReadWrite],
            is_superuser: false,
        });

        let data = serde_json::json!({"key": "value"});
        let msg = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 1,
            checksum: 0,
        };

        // First should succeed (1 token available).
        let r1 = session.handle_delta_push(&msg, None, None, None);
        assert!(r1.is_some());
        assert_eq!(session.mutations_processed, 1);

        // Second should be silently dropped (rate limited).
        let mut audit_log = AuditLog::new(100);
        let mut dlq = SyncDlq::new(super::super::dlq::DlqConfig::default());

        let msg2 = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d2".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 2,
            checksum: 0,
        };
        let r2 = session.handle_delta_push(&msg2, None, Some(&mut audit_log), Some(&mut dlq));
        assert!(r2.is_none()); // Silent drop.
        assert_eq!(session.mutations_silent_dropped, 1);
        assert_eq!(dlq.total_entries(), 1);
    }

    #[test]
    fn ping_pong() {
        let mut session = make_session();

        let ping = PingPongMsg {
            timestamp_ms: 99999,
            is_pong: false,
        };
        let response = session.handle_ping(&ping);
        let pong: PingPongMsg = response.decode_body().unwrap();
        assert!(pong.is_pong);
        assert_eq!(pong.timestamp_ms, 99999);
    }

    #[test]
    fn vector_clock_sync() {
        let mut session = make_session();
        session.authenticated = true;

        let mut clocks = HashMap::new();
        clocks.insert("orders".into(), 42u64);

        let msg = VectorClockSyncMsg {
            clocks,
            sender_id: 5,
        };
        let response = session.handle_vector_clock_sync(&msg);
        let sync: VectorClockSyncMsg = response.decode_body().unwrap();
        assert_eq!(*sync.clocks.get("orders").unwrap(), 42);
    }

    #[test]
    fn replay_dedup_skips_already_processed() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"key": "value"});
        let delta = nodedb_types::json_to_msgpack(&data).unwrap();

        let msg = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: delta.clone(),
            peer_id: 42,
            mutation_id: 5,
            checksum: 0,
        };

        // First push — accepted.
        let r1 = session.handle_delta_push(&msg, None, None, None);
        assert!(r1.is_some());
        assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);
        assert_eq!(session.mutations_processed, 1);

        // Replay same mutation_id — dedup'd, returns ACK without re-processing.
        let r2 = session.handle_delta_push(&msg, None, None, None);
        assert!(r2.is_some());
        assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaAck);
        // mutations_processed should NOT increment (dedup).
        assert_eq!(session.mutations_processed, 1);

        // Lower mutation_id — also dedup'd.
        let msg_old = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d0".into(),
            delta: delta.clone(),
            peer_id: 42,
            mutation_id: 3,
            checksum: 0,
        };
        let r3 = session.handle_delta_push(&msg_old, None, None, None);
        assert!(r3.is_some());
        assert_eq!(r3.unwrap().msg_type, SyncMessageType::DeltaAck);
        assert_eq!(session.mutations_processed, 1);

        // Higher mutation_id — accepted.
        let msg_new = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d2".into(),
            delta,
            peer_id: 42,
            mutation_id: 6,
            checksum: 0,
        };
        let r4 = session.handle_delta_push(&msg_new, None, None, None);
        assert!(r4.is_some());
        assert_eq!(r4.unwrap().msg_type, SyncMessageType::DeltaAck);
        assert_eq!(session.mutations_processed, 2);
    }

    #[test]
    fn crc32c_mismatch_rejects_delta() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"key": "value"});
        let delta = nodedb_types::json_to_msgpack(&data).unwrap();

        // Valid checksum — accepted.
        let valid_checksum = crc32c::crc32c(&delta);
        let msg_ok = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: delta.clone(),
            peer_id: 1,
            mutation_id: 1,
            checksum: valid_checksum,
        };
        let r1 = session.handle_delta_push(&msg_ok, None, None, None);
        assert!(r1.is_some());
        assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);

        // Bad checksum — rejected.
        let msg_bad = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d2".into(),
            delta,
            peer_id: 1,
            mutation_id: 2,
            checksum: valid_checksum ^ 0xDEAD, // Corrupt the checksum.
        };
        let r2 = session.handle_delta_push(&msg_bad, None, None, None);
        assert!(r2.is_some());
        assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
    }
}
