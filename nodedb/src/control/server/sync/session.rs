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
        }
    }

    /// Process a handshake message: validate JWT, store client clock.
    ///
    /// Returns a HandshakeAck frame to send back to the client.
    pub fn handle_handshake(
        &mut self,
        msg: &HandshakeMsg,
        jwt_validator: &JwtValidator,
        current_server_clock: HashMap<String, u64>,
    ) -> SyncFrame {
        self.last_activity = Instant::now();

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

            info!(session = %self.session_id, "sync handshake OK (trust mode)");

            let ack = HandshakeAckMsg {
                success: true,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: None,
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
                };
                SyncFrame::encode_or_empty(SyncMessageType::HandshakeAck, &ack)
            }
        }
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

        // Update device metadata peer_id on first delta.
        if self.device_metadata.peer_id == 0 {
            self.device_metadata.peer_id = msg.peer_id;
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

        // Delta is valid — the caller commits to WAL/Raft and assigns an LSN.
        self.mutations_processed += 1;
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

    /// Process a ping message. Returns a pong.
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
    ) -> Option<SyncFrame> {
        match frame.msg_type {
            SyncMessageType::Handshake => {
                let msg: HandshakeMsg = frame.decode_body()?;
                Some(self.handle_handshake(&msg, jwt_validator, self.server_clock.clone()))
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
                let current_lsn = 0u64; // TODO: get from WAL when wired
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
            SyncMessageType::PingPong => {
                let msg: PingPongMsg = frame.decode_body()?;
                if msg.is_pong {
                    None
                } else {
                    Some(self.handle_ping(&msg))
                }
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
        };

        let response = session.handle_handshake(&msg, &validator, HashMap::new());
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
            delta: rmp_serde::to_vec_named(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
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
            value: serde_json::json!("active"),
            clauses: Vec::new(),
        };
        let predicate = rmp_serde::to_vec_named(&vec![filter]).unwrap();
        rls_store
            .create_policy(RlsPolicy {
                name: "require_active".into(),
                collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                predicate,
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
            delta: rmp_serde::to_vec_named(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
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
            delta: rmp_serde::to_vec_named(&data).unwrap(),
            peer_id: 1,
            mutation_id: 1,
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
            delta: rmp_serde::to_vec_named(&data).unwrap(),
            peer_id: 1,
            mutation_id: 2,
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
}
