// SPDX-License-Identifier: BUSL-1.1

//! Handshake + fork detection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use crate::control::state::SharedState;

use super::super::dlq::DeviceMetadata;
use super::super::wire::*;
use super::fencing::FencingDecision;
use super::state::SyncSession;

impl SyncSession {
    /// Process a handshake message: validate JWT, store client clock, detect forks.
    ///
    /// `shared` carries both the JWT verifier (the configured `[auth.jwt]`
    /// providers) and the durable `SyncProducerRegistry` consulted when the
    /// session is a Lite client. When it is `None` (unit-test path without
    /// SharedState) the handshake proceeds with no fencing (`producer_id = 0`)
    /// and any presented token is refused, since nothing can verify it.
    ///
    /// Returns a HandshakeAck frame to send back to the client.
    pub async fn handle_handshake(
        &mut self,
        msg: &HandshakeMsg,
        current_server_clock: HashMap<String, u64>,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        // A handshake is an authentication boundary, not an additive update to
        // a prior one. Clear the prior binding before examining this attempt so
        // every failure leaves the connection unable to use its old identity.
        self.clear_handshake_binding();
        self.last_activity = Instant::now();

        // A client of another sync protocol version frames messages in a
        // layout this server does not decode, so it is refused before any
        // other frame. Version 0 (missing field) is refused the same way.
        if let Err(mismatch) = msg.check_wire_version() {
            let e = crate::Error::from(mismatch);
            warn!(
                session = %self.session_id,
                client_wire_version = msg.wire_version,
                error = %e,
                "sync handshake rejected: incompatible wire version"
            );
            let ack = HandshakeAckMsg {
                success: false,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: Some(format!("incompatible wire version: {e}")),
                fork_detected: false,
                server_wire_version: SYNC_WIRE_VERSION,
                producer_id: 0,
                accepted_epoch: 0,
                delta_signing_key: [0; 32],
            };
            return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
        }

        // Trust mode: an empty token resolves to the configured durable
        // principal. Never fabricate an identity that cannot own catalog data.
        if msg.jwt_token.is_empty() {
            let Some(identity) = shared.and_then(|state| {
                crate::control::server::session_auth::configured_trust_identity(state)
            }) else {
                let ack = HandshakeAckMsg {
                    success: false,
                    session_id: self.session_id.clone(),
                    server_clock: current_server_clock,
                    error: Some("configured trust identity is unavailable".into()),
                    fork_detected: false,
                    server_wire_version: SYNC_WIRE_VERSION,
                    producer_id: 0,
                    accepted_epoch: 0,
                    delta_signing_key: [0; 32],
                };
                return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
            };
            self.tenant_id = Some(identity.tenant_id);
            self.username = Some(identity.username.clone());
            let user_id = identity.user_id;
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
                // Preserve the accept-time TCP peer address set once in
                // `handle_sync_session` before this handshake frame arrives —
                // overwriting it with an empty string here silently defeats
                // the IP half of `check_blacklist_and_status` for every delta
                // this session pushes after a successful handshake.
                remote_addr: std::mem::take(&mut self.device_metadata.remote_addr),
                peer_id: 0,
            };

            let tenant_id = self.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
            match self
                .durable_fencing_decision(msg, shared, tenant_id, user_id)
                .await
            {
                Some(FencingDecision::Reject) => {
                    return self.fork_reject_frame(
                        &current_server_clock,
                        "FORK_DETECTED: stale epoch — regenerate LiteId and reconnect",
                    );
                }
                Some(FencingDecision::RejectTransient) => {
                    return self.transient_reject_frame(
                        &current_server_clock,
                        "SYNC_UNAVAILABLE: transient server error — retry the handshake",
                    );
                }
                Some(FencingDecision::Accept {
                    producer_id,
                    accepted_epoch,
                }) => {
                    self.producer_id = producer_id;
                    self.accepted_epoch = accepted_epoch;
                }
                None => {}
            }

            info!(session = %self.session_id, "sync handshake OK (trust mode)");

            let ack = HandshakeAckMsg {
                success: true,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: None,
                fork_detected: false,
                server_wire_version: SYNC_WIRE_VERSION,
                producer_id: self.producer_id,
                accepted_epoch: self.accepted_epoch,
                delta_signing_key: [0; 32],
            };
            return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
        }

        // A presented token goes through the same gate as every other bearer
        // route: the configured `[auth.jwt]` providers plus the stateful JWT
        // policy. Without `SharedState` there is no gate at all, so the token
        // is unverifiable and the handshake is refused rather than falling
        // through to the trust branch above.
        let identity = match shared {
            Some(state) => crate::control::server::session_auth::authenticate_bearer_jwt(
                state,
                &msg.jwt_token,
                &self.device_metadata.remote_addr,
                &self.session_id,
            )
            .await
            .map(|(identity, _claims)| identity),
            None => None,
        };
        let Some(identity) = identity else {
            warn!(
                session = %self.session_id,
                "sync handshake FAILED: bearer token not authenticated"
            );
            let ack = HandshakeAckMsg {
                success: false,
                session_id: self.session_id.clone(),
                server_clock: HashMap::new(),
                // Deliberately generic, matching the HTTP bearer route: the
                // client learns it is unauthenticated, never why.
                error: Some("invalid bearer token".into()),
                fork_detected: false,
                server_wire_version: SYNC_WIRE_VERSION,
                producer_id: 0,
                accepted_epoch: 0,
                delta_signing_key: [0; 32],
            };
            return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
        };

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
            // See the trust-mode branch above: preserve the accept-time TCP
            // peer address rather than wiping it.
            remote_addr: std::mem::take(&mut self.device_metadata.remote_addr),
            peer_id: 0,
        };

        let tenant_id = identity.tenant_id.as_u64();
        match self
            .durable_fencing_decision(msg, shared, tenant_id, identity.user_id)
            .await
        {
            Some(FencingDecision::Reject) => {
                return self.fork_reject_frame(
                    &current_server_clock,
                    "FORK_DETECTED: stale epoch — regenerate LiteId and reconnect",
                );
            }
            Some(FencingDecision::RejectTransient) => {
                return self.transient_reject_frame(
                    &current_server_clock,
                    "SYNC_UNAVAILABLE: transient server error — retry the handshake",
                );
            }
            Some(FencingDecision::Accept {
                producer_id,
                accepted_epoch,
            }) => {
                self.producer_id = producer_id;
                self.accepted_epoch = accepted_epoch;
            }
            None => {}
        }

        self.delta_signing_key = match shared {
            Some(state) if state.wal.payloads_authenticated() => {
                match state
                    .credentials
                    .catalog()
                    .get_or_create_crdt_signing_key(identity.tenant_id.as_u64(), identity.user_id)
                {
                    Ok(key) => Some(key),
                    Err(error) => {
                        warn!(session = %self.session_id, %error, "sync signing key unavailable");
                        return self.transient_reject_frame(
                            &current_server_clock,
                            "SYNC_UNAVAILABLE: signing key persistence failed",
                        );
                    }
                }
            }
            Some(_) | None => None,
        };

        info!(
            session = %self.session_id,
            user = %identity.username,
            tenant = identity.tenant_id.as_u64(),
            shapes = self.subscribed_shapes.len(),
            "sync handshake OK"
        );

        let ack = HandshakeAckMsg {
            success: true,
            session_id: self.session_id.clone(),
            server_clock: current_server_clock,
            error: None,
            fork_detected: false,
            server_wire_version: SYNC_WIRE_VERSION,
            producer_id: self.producer_id,
            accepted_epoch: self.accepted_epoch,
            delta_signing_key: self.delta_signing_key.unwrap_or([0; 32]),
        };
        SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack)
    }

    /// Clear all state derived from a successful handshake or from the
    /// candidate currently being authenticated. Connection lifetime, activity,
    /// rate-limiting, and telemetry counters deliberately survive.
    pub(super) fn clear_handshake_binding(&mut self) {
        self.tenant_id = None;
        self.username = None;
        self.identity = None;
        self.authenticated = false;
        self.client_clock.clear();
        self.server_clock.clear();
        self.subscribed_shapes.clear();
        self.last_seen_lsn = 0;
        self.producer_id = 0;
        self.accepted_epoch = 0;
        self.delta_signing_key = None;
        // The peer address is a property of the accepted TCP connection, not
        // of the authentication binding: it is stamped once before any frame
        // is read and is the same address whether this handshake succeeds or
        // fails. Clearing it here will leave the IP half of
        // `check_blacklist_and_status` inert for the rest of the connection,
        // so it survives alongside the other connection-lifetime state above.
        // Everything genuinely derived from the handshake is reset.
        self.device_metadata = DeviceMetadata {
            remote_addr: std::mem::take(&mut self.device_metadata.remote_addr),
            ..DeviceMetadata::default()
        };
        self.tracked_collections.clear();
        self.announced_collections.clear();
    }

    /// Build a bounded, generic rejection for a Handshake frame whose body
    /// cannot be decoded. The fixed message intentionally exposes no
    /// decoder or payload details to the peer.
    pub(super) fn malformed_handshake_reject_frame(&self) -> Option<SyncFrame> {
        self.build_reject_frame(&HashMap::new(), "malformed handshake", false)
    }

    /// Build a failed HandshakeAck rejection frame: `success=false`, the
    /// supplied `message` in the `error` field, and the given `fork_detected`
    /// flag. Shared by [`Self::fork_reject_frame`] and
    /// [`Self::transient_reject_frame`] so the envelope stays identical.
    fn build_reject_frame(
        &self,
        server_clock: &HashMap<String, u64>,
        message: &str,
        fork_detected: bool,
    ) -> Option<SyncFrame> {
        let ack = HandshakeAckMsg {
            success: false,
            session_id: self.session_id.clone(),
            server_clock: server_clock.clone(),
            error: Some(message.into()),
            fork_detected,
            server_wire_version: SYNC_WIRE_VERSION,
            producer_id: 0,
            accepted_epoch: 0,
            delta_signing_key: [0; 32],
        };
        SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack)
    }

    /// Build a fork-detected rejection frame (`fork_detected=true`): the client
    /// treats the session as a permanent fork and wipes its local state.
    fn fork_reject_frame(
        &mut self,
        server_clock: &HashMap<String, u64>,
        message: &str,
    ) -> Option<SyncFrame> {
        warn!(
            session = %self.session_id,
            "FORK DETECTED: {message}"
        );
        self.clear_handshake_binding();
        self.build_reject_frame(server_clock, message, true)
    }

    /// Build a failed-but-retryable handshake ack for a transient server-side
    /// error (registry I/O, Raft propose failure / leader mid-election).
    ///
    /// Distinct from [`Self::fork_reject_frame`]: `fork_detected = false`, so
    /// the client retries the handshake rather than treating the session as a
    /// permanent fork and wiping its local state.
    fn transient_reject_frame(
        &mut self,
        server_clock: &HashMap<String, u64>,
        message: &str,
    ) -> Option<SyncFrame> {
        warn!(
            session = %self.session_id,
            "sync handshake transient error (retryable): {message}"
        );
        self.clear_handshake_binding();
        self.build_reject_frame(server_clock, message, false)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_types::sync::wire::{
        DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg, HandshakeMsg, SYNC_WIRE_VERSION, SyncFrame,
        SyncMessageType, SyncWireVersionMismatch,
    };

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::sync::session::state::SyncSession;
    use crate::control::server::sync::wire::CompensationHint;
    use crate::control::state::SharedState;
    use crate::control::sync_producer::registry::SyncProducerRegistry;
    use crate::wal::WalManager;

    fn make_handshake(wire_version: u16) -> HandshakeMsg {
        HandshakeMsg {
            jwt_token: String::new(),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version,
        }
    }

    fn trust_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("sync-trust.wal")).expect("open WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        state
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
        (state, dir)
    }

    #[tokio::test]
    async fn handshake_rejects_invalid_jwt() {
        let mut session = SyncSession::new("test-session-1".into());

        let msg = HandshakeMsg {
            jwt_token: "invalid.token.here".into(),
            vector_clock: HashMap::new(),
            subscribed_shapes: vec![],
            client_version: "0.1".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: SYNC_WIRE_VERSION,
        };

        let response = session
            .handle_handshake(&msg, HashMap::new(), None)
            .await
            .expect("handshake response");
        assert_eq!(response.msg_type, SyncMessageType::HandshakeAck);

        let ack: HandshakeAckMsg = response.decode_body().unwrap();
        assert!(!ack.success);
        assert!(ack.error.is_some());
        assert!(!session.authenticated);
    }

    #[tokio::test]
    async fn empty_token_uses_configured_durable_trust_identity() {
        let (state, _dir) = trust_state();
        let mut session = SyncSession::new("test-session".into());
        let msg = make_handshake(SYNC_WIRE_VERSION);

        let frame = session
            .handle_handshake(&msg, HashMap::new(), Some(&state))
            .await
            .expect("handshake response");
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(ack.success, "configured trust handshake should succeed");
        let identity = session.identity.expect("durable trust identity");
        assert_eq!(identity.username, "nodedb");
        assert_ne!(identity.user_id, 0);
        assert!(identity.is_superuser);
    }

    /// The accept-time TCP peer address (`handle_sync_session` sets
    /// `session.device_metadata.remote_addr` once, before any frame is
    /// processed) must survive a successful handshake. Without this, the IP
    /// half of `check_blacklist_and_status` goes silently inert for every
    /// delta a session pushes after authenticating.
    #[tokio::test]
    async fn successful_handshake_preserves_accept_time_remote_addr() {
        let (state, _dir) = trust_state();
        let mut session = SyncSession::new("test-session".into());
        session.device_metadata.remote_addr = "203.0.113.9:4433".into();
        let msg = make_handshake(SYNC_WIRE_VERSION);

        let frame = session
            .handle_handshake(&msg, HashMap::new(), Some(&state))
            .await
            .expect("handshake response");
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(ack.success, "configured trust handshake should succeed");
        assert_eq!(
            session.device_metadata.remote_addr, "203.0.113.9:4433",
            "a successful handshake must not wipe the real accept-time peer address"
        );
    }

    #[tokio::test]
    async fn empty_token_without_configured_identity_fails_closed() {
        let mut session = SyncSession::new("test-session".into());
        let msg = make_handshake(SYNC_WIRE_VERSION);

        let frame = session
            .handle_handshake(&msg, HashMap::new(), None)
            .await
            .expect("handshake response");
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(!ack.success);
        assert!(!session.authenticated);
        assert!(session.identity.is_none());
    }

    /// A deployment with no `[auth.jwt]` provider cannot verify a presented
    /// credential. The token must be refused outright — never accepted, and
    /// never quietly downgraded to the configured trust identity, which the
    /// empty-token branch above will otherwise hand out.
    #[tokio::test]
    async fn presented_token_without_jwks_registry_is_refused() {
        let (state, _dir) = trust_state();
        assert!(
            state.jwks_registry.is_none(),
            "this fixture must have no JWT verifier for the refusal to be meaningful"
        );
        let mut session = SyncSession::new("test-session".into());
        let mut msg = make_handshake(SYNC_WIRE_VERSION);
        // Synthetic fixture token, not a credential.
        msg.jwt_token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2ln".into(); // gitleaks:allow

        let frame = session
            .handle_handshake(&msg, HashMap::new(), Some(&state))
            .await
            .expect("handshake response");
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(
            !ack.success,
            "an unverifiable bearer token authenticated the sync session"
        );
        assert!(!session.authenticated);
        assert!(session.identity.is_none());
        assert!(session.tenant_id.is_none());
    }

    /// The same refusal without any `SharedState`: there is no verifier at
    /// all, so the token cannot be authenticated.
    #[tokio::test]
    async fn presented_token_without_shared_state_is_refused() {
        let mut session = SyncSession::new("test-session".into());
        let mut msg = make_handshake(SYNC_WIRE_VERSION);
        // Synthetic fixture token, not a credential.
        msg.jwt_token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2ln".into(); // gitleaks:allow

        let frame = session
            .handle_handshake(&msg, HashMap::new(), None)
            .await
            .expect("handshake response");
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(!ack.success);
        assert!(!session.authenticated);
        assert!(session.identity.is_none());
    }

    #[tokio::test]
    async fn handshake_rejects_wire_version_zero() {
        let mut session = SyncSession::new("test-session".into());
        let msg = make_handshake(0);

        let frame = session
            .handle_handshake(&msg, HashMap::new(), None)
            .await
            .expect("should return a frame");

        let ack: HandshakeAckMsg = frame.decode_body().expect("should decode HandshakeAckMsg");
        assert!(
            !ack.success,
            "wire_version=0 must be rejected, got success=true"
        );
        let error = ack.error.expect("error message must be present");
        assert!(
            error.contains("wire version") || error.contains("incompatible"),
            "error message should mention wire version, got: {error}"
        );
    }

    /// A client built against the previous sync layout sends version 1. The
    /// server refuses it at the handshake, before any delta frame: the ack
    /// carries the server's version, so the client reads the refusal as the
    /// typed mismatch, and the session stays unauthenticated even where a
    /// trust identity is configured.
    #[tokio::test]
    async fn handshake_refuses_a_client_of_the_previous_sync_layout() {
        let (state, _dir) = trust_state();
        let mut session = SyncSession::new("old-layout-session".into());
        let msg = make_handshake(SYNC_WIRE_VERSION - 1);

        let frame = session
            .handle_handshake(&msg, HashMap::new(), Some(&state))
            .await
            .expect("handshake response");
        assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
        let ack: HandshakeAckMsg = frame.decode_body().expect("decode handshake ack");

        assert!(!ack.success);
        assert_eq!(ack.server_wire_version, SYNC_WIRE_VERSION);
        let expected = SyncWireVersionMismatch {
            client: SYNC_WIRE_VERSION - 1,
            server: SYNC_WIRE_VERSION,
        };
        assert_eq!(msg.check_wire_version(), Err(expected));
        let error = ack.error.expect("the refusal names its reason");
        assert!(error.contains(&expected.to_string()), "{error}");
        assert!(!session.authenticated);
        assert!(session.identity.is_none());
    }

    #[tokio::test]
    async fn malformed_handshake_clears_binding_and_denies_following_delta() {
        let (shared, _dir) = trust_state();
        let mut session = SyncSession::new("malformed-handshake-session".into());

        // Establish a real production-backed identity, then stage the
        // identity-bound state a prior authenticated Lite handshake can hold.
        let valid_handshake = make_handshake(SYNC_WIRE_VERSION);
        let valid_frame = session
            .handle_handshake(&valid_handshake, HashMap::new(), Some(&shared))
            .await
            .expect("valid handshake response");
        let valid_ack: HandshakeAckMsg = valid_frame.decode_body().expect("decode valid ack");
        assert!(valid_ack.success);
        assert!(session.authenticated);
        assert!(session.identity.is_some());

        session
            .client_clock
            .insert("orders".into(), HashMap::from([("peer-1".into(), 4)]));
        session.server_clock.insert("orders".into(), 8);
        session.subscribed_shapes.push("orders-shape".into());
        session.last_seen_lsn = 8;
        session.producer_id = 42;
        session.accepted_epoch = 7;
        session.delta_signing_key = Some([9; 32]);
        session.device_metadata.remote_addr = "127.0.0.1:1234".into();
        session.device_metadata.peer_id = 99;
        session.track_collection(1, "orders");
        session.announced_collections.insert("orders".into());

        // Round-trip through the framing codec so this is a CRC-valid
        // Handshake frame whose MessagePack body has the wrong shape.
        let malformed_wire = SyncFrame::try_encode(
            SyncMessageType::Handshake,
            &"not a handshake message".to_string(),
        )
        .expect("encode malformed handshake body")
        .to_bytes();
        let malformed_frame =
            SyncFrame::from_bytes(&malformed_wire).expect("CRC-valid malformed handshake frame");
        let response = session
            .process_frame(
                &malformed_frame,
                Some(&shared.rls),
                None,
                None,
                Some(&shared),
            )
            .await
            .expect("malformed handshake rejection");

        assert_eq!(response.msg_type, SyncMessageType::HandshakeAck);
        let ack: HandshakeAckMsg = response.decode_body().expect("decode malformed ack");
        assert!(!ack.success);
        assert!(!ack.fork_detected);
        assert_eq!(ack.error.as_deref(), Some("malformed handshake"));
        assert!(!session.authenticated);
        assert!(session.tenant_id.is_none());
        assert!(session.username.is_none());
        assert!(session.identity.is_none());
        assert!(session.client_clock.is_empty());
        assert!(session.server_clock.is_empty());
        assert!(session.subscribed_shapes.is_empty());
        assert_eq!(session.last_seen_lsn, 0);
        assert_eq!(session.producer_id, 0);
        assert_eq!(session.accepted_epoch, 0);
        assert!(session.delta_signing_key.is_none());
        assert!(session.device_metadata.client_version.is_empty());
        assert_eq!(session.device_metadata.peer_id, 0);
        // The connection's peer address is not handshake binding state and
        // deliberately survives, so the IP blacklist check keeps working on
        // the rest of the connection.
        assert_eq!(session.device_metadata.remote_addr, "127.0.0.1:1234");
        assert!(session.tracked_collections.is_empty());
        assert!(session.announced_collections.is_empty());

        let delta = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: nodedb_types::json_to_msgpack(&serde_json::json!({"status": "active"}))
                .expect("encode valid delta"),
            peer_id: 1,
            mutation_id: 17,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 42,
            epoch: 7,
            seq: 1,
        };
        let delta_frame =
            SyncFrame::try_encode(SyncMessageType::DeltaPush, &delta).expect("encode delta frame");
        let response = session
            .process_frame(&delta_frame, Some(&shared.rls), None, None, Some(&shared))
            .await
            .expect("permission denial response");
        assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = response.decode_body().expect("decode delta rejection");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::PermissionDenied)
        );
        assert_eq!(session.mutations_processed, 0);
    }

    /// A Lite handshake proposes its producer registration and epoch fence
    /// through the metadata group and blocks on the commit, so the test runs
    /// on a one-node cluster with a multi-thread runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_lite_rehandshake_clears_trust_binding_before_delta_dispatch() {
        // The registry shares the credential store's catalog, as boot wires it.
        let cluster = crate::control::cluster::test_one_node::boot_with(|state| {
            let catalog = Arc::new(state.credentials.catalog().clone());
            state.producer_registry = Some(Arc::new(
                SyncProducerRegistry::open(catalog).expect("open producer registry"),
            ));
        })
        .await;
        let shared = Arc::clone(&cluster.state);
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");

        let mut high_epoch_session = SyncSession::new("high-epoch-session".into());
        // Stamped once at accept time, as `handle_sync_session` does.
        high_epoch_session.device_metadata.remote_addr = "203.0.113.12:6666".into();
        let mut high_epoch_msg = make_handshake(SYNC_WIRE_VERSION);
        high_epoch_msg.lite_id = "fenced-lite-id".into();
        high_epoch_msg.epoch = 9;
        let high_epoch_frame = high_epoch_session
            .handle_handshake(&high_epoch_msg, HashMap::new(), Some(&shared))
            .await
            .expect("high epoch handshake response");
        let high_epoch_ack: HandshakeAckMsg = high_epoch_frame.decode_body().expect("decode ack");
        assert!(high_epoch_ack.success, "higher epoch must be accepted");
        assert!(high_epoch_session.authenticated);
        assert_ne!(high_epoch_session.producer_id, 0);
        assert_eq!(high_epoch_session.accepted_epoch, 9);

        // Seed identity-bound state that a prior successful handshake can
        // accumulate. The next handshake attempt must not retain any of it.
        high_epoch_session
            .client_clock
            .insert("orders".into(), HashMap::from([("peer-1".into(), 4)]));
        high_epoch_session.server_clock.insert("orders".into(), 8);
        high_epoch_session
            .subscribed_shapes
            .push("orders-shape".into());
        high_epoch_session.last_seen_lsn = 8;
        high_epoch_session.track_collection(1, "orders");
        high_epoch_session
            .announced_collections
            .insert("orders".into());

        // This new handshake attempt stages the configured trust identity
        // before the registry discovers that this LiteId's epoch is stale.
        let mut stale_msg = make_handshake(SYNC_WIRE_VERSION);
        stale_msg.lite_id = "fenced-lite-id".into();
        stale_msg.epoch = 3;
        let stale_frame = high_epoch_session
            .handle_handshake(&stale_msg, HashMap::new(), Some(&shared))
            .await
            .expect("stale handshake response");
        let stale_ack: HandshakeAckMsg = stale_frame.decode_body().expect("decode stale ack");

        assert!(!stale_ack.success);
        assert!(stale_ack.fork_detected);
        assert!(!high_epoch_session.authenticated);
        assert!(high_epoch_session.tenant_id.is_none());
        assert!(high_epoch_session.username.is_none());
        assert!(high_epoch_session.identity.is_none());
        assert!(high_epoch_session.client_clock.is_empty());
        assert!(high_epoch_session.server_clock.is_empty());
        assert!(high_epoch_session.subscribed_shapes.is_empty());
        assert_eq!(high_epoch_session.last_seen_lsn, 0);
        assert_eq!(high_epoch_session.producer_id, 0);
        assert_eq!(high_epoch_session.accepted_epoch, 0);
        assert!(high_epoch_session.delta_signing_key.is_none());
        assert!(high_epoch_session.device_metadata.client_version.is_empty());
        assert_eq!(high_epoch_session.device_metadata.peer_id, 0);
        // Connection-level, not binding-level: survives a rejected handshake.
        assert_eq!(
            high_epoch_session.device_metadata.remote_addr,
            "203.0.113.12:6666"
        );
        assert!(high_epoch_session.tracked_collections.is_empty());
        assert!(high_epoch_session.announced_collections.is_empty());

        // The configured trust identity is a superuser, so this will be
        // authorized and provisionally ACKed if the stale handshake retained
        // its staged identity. The production dispatch gate must deny it.
        let delta = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: nodedb_types::json_to_msgpack(&serde_json::json!({"status": "active"}))
                .expect("encode valid delta"),
            peer_id: 1,
            mutation_id: 17,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let delta_frame =
            SyncFrame::try_encode(SyncMessageType::DeltaPush, &delta).expect("encode delta frame");
        let response = high_epoch_session
            .process_frame(&delta_frame, Some(&shared.rls), None, None, Some(&shared))
            .await
            .expect("permission denial response");
        assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = response.decode_body().expect("decode delta rejection");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::PermissionDenied)
        );
        assert_eq!(high_epoch_session.mutations_processed, 0);
        cluster.shutdown().await;
    }
}
