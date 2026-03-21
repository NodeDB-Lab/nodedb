//! Sync session: handles one WebSocket connection from a NodeDB-Lite client.
//!
//! Processes incoming frames (handshake, delta push, vector clock sync,
//! ping/pong) and sends responses. Each session is authenticated via JWT
//! and scoped to a single tenant.

use std::collections::HashMap;
use std::time::Instant;

use tracing::{debug, info, warn};

use crate::control::security::jwt::JwtValidator;
use crate::types::TenantId;

use super::wire::*;

/// State of a single sync session (one WebSocket connection).
pub struct SyncSession {
    /// Unique session ID.
    pub session_id: String,
    /// Authenticated tenant.
    pub tenant_id: Option<TenantId>,
    /// Authenticated username.
    pub username: Option<String>,
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
    /// Last activity timestamp.
    pub last_activity: Instant,
    /// Session creation time.
    pub created_at: Instant,
}

impl SyncSession {
    pub fn new(session_id: String) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            tenant_id: None,
            username: None,
            authenticated: false,
            client_clock: HashMap::new(),
            server_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            mutations_processed: 0,
            mutations_rejected: 0,
            last_activity: now,
            created_at: now,
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

        // Validate JWT.
        match jwt_validator.validate(&msg.jwt_token) {
            Ok(identity) => {
                self.tenant_id = Some(identity.tenant_id);
                self.username = Some(identity.username.clone());
                self.authenticated = true;
                self.client_clock = msg.vector_clock.clone();
                self.subscribed_shapes = msg.subscribed_shapes.clone();
                self.server_clock = current_server_clock.clone();

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

    /// Process a delta push: validate and prepare for WAL commit.
    ///
    /// Returns either a DeltaAck or DeltaReject frame.
    pub fn handle_delta_push(&mut self, msg: &DeltaPushMsg) -> SyncFrame {
        self.last_activity = Instant::now();

        if !self.authenticated {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "not authenticated".into(),
                compensation: Some(CompensationHint::PermissionDenied),
            };
            return SyncFrame::encode_or_empty(SyncMessageType::DeltaReject, &reject);
        }

        if msg.delta.is_empty() {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "empty delta".into(),
                compensation: None,
            };
            return SyncFrame::encode_or_empty(SyncMessageType::DeltaReject, &reject);
        }

        // Delta is valid — the caller commits to WAL/Raft and assigns an LSN.
        // For now, return a pending ack with LSN=0 (caller fills in real LSN).
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
        SyncFrame::encode_or_empty(SyncMessageType::DeltaAck, &ack)
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
    pub fn process_frame(
        &mut self,
        frame: &SyncFrame,
        jwt_validator: &JwtValidator,
    ) -> Option<SyncFrame> {
        match frame.msg_type {
            SyncMessageType::Handshake => {
                let msg: HandshakeMsg = frame.decode_body()?;
                Some(self.handle_handshake(&msg, jwt_validator, self.server_clock.clone()))
            }
            SyncMessageType::DeltaPush => {
                let msg: DeltaPushMsg = frame.decode_body()?;
                Some(self.handle_delta_push(&msg))
            }
            SyncMessageType::VectorClockSync => {
                let msg: VectorClockSyncMsg = frame.decode_body()?;
                Some(self.handle_vector_clock_sync(&msg))
            }
            SyncMessageType::PingPong => {
                let msg: PingPongMsg = frame.decode_body()?;
                if msg.is_pong {
                    None // Pong is a response, no reply needed.
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

    fn make_session() -> SyncSession {
        SyncSession::new("test-session-1".into())
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

        let response = session.handle_delta_push(&msg);
        assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
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
