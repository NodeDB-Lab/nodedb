//! Handshake + fork detection.

use std::collections::HashMap;
use std::time::Instant;

use tracing::{info, warn};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::JwtValidator;
use crate::types::TenantId;

use super::super::dlq::DeviceMetadata;
use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
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
            self.last_seen_lsn = msg
                .vector_clock
                .values()
                .flat_map(|m| m.values().copied())
                .max()
                .unwrap_or(0);
            self.device_metadata = DeviceMetadata {
                client_version: msg.client_version.clone(),
                remote_addr: String::new(),
                peer_id: 0,
            };

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
                self.last_seen_lsn = msg
                    .vector_clock
                    .values()
                    .flat_map(|m| m.values().copied())
                    .max()
                    .unwrap_or(0);
                self.device_metadata = DeviceMetadata {
                    client_version: msg.client_version.clone(),
                    remote_addr: String::new(),
                    peer_id: 0,
                };

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

    /// Check fork detection. If `lite_id` is non-empty and
    /// `epoch <= last_seen_epoch`, this is a cloned device reusing a
    /// database backup — return a FORK_DETECTED rejection.
    pub(super) fn check_fork_detection(
        &self,
        msg: &HandshakeMsg,
        server_clock: &HashMap<String, u64>,
        epoch_tracker: Option<&std::sync::Mutex<HashMap<String, u64>>>,
    ) -> Option<SyncFrame> {
        if msg.lite_id.is_empty() || msg.epoch == 0 {
            return None;
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

        epochs.insert(msg.lite_id.clone(), msg.epoch);
        info!(
            session = %self.session_id,
            lite_id = %msg.lite_id,
            epoch = msg.epoch,
            "epoch tracker updated"
        );
        None
    }
}
