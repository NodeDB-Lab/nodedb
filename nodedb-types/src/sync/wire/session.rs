//! Session lifecycle messages: handshake, token refresh, keepalive.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    /// Lite instance identity (UUID v7). Empty for legacy clients.
    #[serde(default)]
    pub lite_id: String,
    /// Monotonic epoch counter (incremented on every open). 0 for legacy clients.
    #[serde(default)]
    pub epoch: u64,
    /// Wire format version. Server rejects connections with incompatible versions.
    /// 0 = legacy client (pre-wire-version; treated as version 1).
    #[serde(default)]
    pub wire_version: u16,
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
    /// Server's wire format version (for client-side compatibility check).
    #[serde(default)]
    pub server_wire_version: u16,
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
