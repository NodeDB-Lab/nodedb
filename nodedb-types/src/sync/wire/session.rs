// SPDX-License-Identifier: Apache-2.0

//! Session lifecycle messages: handshake, token refresh, keepalive.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// The sync protocol version this build speaks. A client sends it in
/// [`HandshakeMsg::wire_version`], and a server answers its own in
/// [`HandshakeAckMsg::server_wire_version`]. A server refuses a handshake
/// whose version differs from its own: the two sides frame messages
/// differently, and a frame of the other layout does not decode.
///
/// It rises whenever the layout of a sync message changes. At version 2,
/// `DeltaPushMsg` carries `collection_incarnation`.
pub const SYNC_WIRE_VERSION: u16 = 2;

/// A handshake whose sync protocol version differs from the server's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "sync wire version {client} does not match the server's {server}: rebuild the client \
     against a nodedb-types release with sync wire version {server}"
)]
pub struct SyncWireVersionMismatch {
    /// The version the client's handshake carries.
    pub client: u16,
    /// The version the server speaks.
    pub server: u16,
}

/// Handshake message (client → server, 0x01).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HandshakeMsg {
    /// JWT bearer token for authentication.
    pub jwt_token: String,
    /// Client's vector clock: `{ collection: { doc_id: lamport_ts } }`.
    pub vector_clock: HashMap<String, HashMap<String, u64>>,
    /// Shape IDs the client is subscribed to.
    pub subscribed_shapes: Vec<String>,
    /// Client version string.
    pub client_version: String,
    /// Lite instance identity (UUID v7). Default empty for non-Lite peers.
    #[serde(default)]
    pub lite_id: String,
    /// Monotonic epoch counter (incremented on every open). Default 0 for non-Lite peers.
    #[serde(default)]
    pub epoch: u64,
    /// Sync protocol version: [`SYNC_WIRE_VERSION`] of the client's build.
    /// A server refuses any other version. A missing field reads as 0 and is
    /// refused.
    #[serde(default)]
    pub wire_version: u16,
}

impl HandshakeMsg {
    /// `Err` when this handshake's sync protocol version is not this build's
    /// [`SYNC_WIRE_VERSION`].
    pub fn check_wire_version(&self) -> Result<(), SyncWireVersionMismatch> {
        if self.wire_version == SYNC_WIRE_VERSION {
            return Ok(());
        }
        Err(SyncWireVersionMismatch {
            client: self.wire_version,
            server: SYNC_WIRE_VERSION,
        })
    }
}

/// Handshake acknowledgment (server → client, 0x02).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HandshakeAckMsg {
    /// Whether the handshake succeeded.
    pub success: bool,
    /// Session ID assigned by the server.
    pub session_id: String,
    /// Server's vector clock (for initial sync).
    pub server_clock: HashMap<String, u64>,
    /// Error message (if !success).
    pub error: Option<String>,
    /// Fork detection: if true, client must regenerate LiteId and reconnect.
    #[serde(default)]
    pub fork_detected: bool,
    /// Server's sync protocol version ([`SYNC_WIRE_VERSION`]).
    #[serde(default)]
    pub server_wire_version: u16,
    /// Server-assigned producer ID for this session. 0 if not yet assigned.
    #[serde(default)]
    pub producer_id: u64,
    /// Server's current accepted epoch for this producer. 0 if not yet tracked.
    #[serde(default)]
    pub accepted_epoch: u64,
    /// Stable per-user key for signing queued CRDT deltas. Issued only after
    /// successful authentication over the protected sync transport. All zeros
    /// means signing is unavailable for this session.
    #[serde(default)]
    pub delta_signing_key: [u8; 32],
}

impl HandshakeAckMsg {
    /// The version refusal this ack carries: `Some` when the server refused
    /// the handshake and speaks a sync protocol version other than this
    /// build's [`SYNC_WIRE_VERSION`].
    pub fn wire_version_mismatch(&self) -> Option<SyncWireVersionMismatch> {
        (!self.success && self.server_wire_version != SYNC_WIRE_VERSION).then_some(
            SyncWireVersionMismatch {
                client: SYNC_WIRE_VERSION,
                server: self.server_wire_version,
            },
        )
    }
}

/// Token refresh request (client → server, 0x60).
///
/// Sent by Lite before the current JWT expires. The client provides
/// a fresh token obtained from the application's auth layer.
/// Origin validates the new token and either upgrades the session
/// or disconnects if the token is invalid.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TokenRefreshMsg {
    /// New JWT bearer token.
    pub new_token: String,
}

/// Token refresh acknowledgment (server → client, 0x61).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TokenRefreshAckMsg {
    /// Whether the token refresh succeeded.
    pub success: bool,
    /// Error message (if !success).
    pub error: Option<String>,
    /// Seconds until this new token expires (so Lite can schedule next refresh).
    #[serde(default)]
    pub expires_in_secs: u64,
}

/// Ping/Pong keepalive (0xFF).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PingPongMsg {
    /// Timestamp (epoch milliseconds) for RTT measurement.
    pub timestamp_ms: u64,
    /// Whether this is a pong (response to ping).
    pub is_pong: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn handshake_serialization() {
        let msg = HandshakeMsg {
            jwt_token: "test.jwt.token".into(),
            vector_clock: HashMap::new(),
            subscribed_shapes: vec!["shape1".into()],
            client_version: "0.1.0".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: SYNC_WIRE_VERSION,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::Handshake, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert!(bytes.len() > SyncFrame::HEADER_SIZE);
        assert_eq!(bytes[0], SyncFrame::FORMAT_VERSION);
        assert_eq!(bytes[1], 0x01);
    }

    /// A handshake at another sync protocol version is refused with the
    /// typed mismatch, and an ack refusing at another version names it.
    #[test]
    fn a_handshake_at_another_wire_version_is_a_typed_mismatch() {
        let handshake = |wire_version| HandshakeMsg {
            jwt_token: String::new(),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "0.1.0".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version,
        };
        assert_eq!(handshake(SYNC_WIRE_VERSION).check_wire_version(), Ok(()));
        assert_eq!(
            handshake(1).check_wire_version(),
            Err(SyncWireVersionMismatch {
                client: 1,
                server: SYNC_WIRE_VERSION
            })
        );

        let ack = |success, server_wire_version| HandshakeAckMsg {
            success,
            session_id: String::new(),
            server_clock: HashMap::new(),
            error: None,
            fork_detected: false,
            server_wire_version,
            producer_id: 0,
            accepted_epoch: 0,
            delta_signing_key: [0; 32],
        };
        assert_eq!(
            ack(false, SYNC_WIRE_VERSION + 1).wire_version_mismatch(),
            Some(SyncWireVersionMismatch {
                client: SYNC_WIRE_VERSION,
                server: SYNC_WIRE_VERSION + 1
            })
        );
        assert_eq!(ack(false, SYNC_WIRE_VERSION).wire_version_mismatch(), None);
        assert_eq!(ack(true, SYNC_WIRE_VERSION).wire_version_mismatch(), None);
    }
}
