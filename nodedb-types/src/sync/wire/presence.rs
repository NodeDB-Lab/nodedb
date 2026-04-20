//! Presence / awareness messages.

use serde::{Deserialize, Serialize};

/// Presence update message (client → server, 0x80).
///
/// Sends ephemeral user state to a channel. The server broadcasts the state
/// to all other subscribers of the same channel. Presence is NOT persisted,
/// NOT CRDT-merged — it is fire-and-forget with latest-state-wins semantics.
///
/// Sending a `PresenceUpdate` implicitly subscribes the sender to the channel
/// (if not already subscribed).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceUpdateMsg {
    /// Channel scoping key (e.g., `"doc:doc-123"`, `"workspace:ws-acme"`).
    pub channel: String,
    /// Opaque user state (MessagePack-encoded application-defined payload).
    /// Common fields: user_id, user_name, cursor_position, selection_range,
    /// active_document_id, color, avatar_url.
    pub state: Vec<u8>,
}

/// A single peer's presence state within a channel.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PeerPresence {
    /// User identifier.
    pub user_id: String,
    /// Opaque user state (same format as `PresenceUpdateMsg::state`).
    pub state: Vec<u8>,
    /// Milliseconds since this peer's last update.
    pub last_seen_ms: u64,
}

/// Presence broadcast message (server → all subscribers except sender, 0x81).
///
/// Contains the full set of currently-present peers in the channel.
/// Sent whenever any peer updates their state or leaves.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceBroadcastMsg {
    /// Channel this broadcast belongs to.
    pub channel: String,
    /// All currently-present peers and their latest state.
    pub peers: Vec<PeerPresence>,
}

/// Presence leave message (server → all subscribers, 0x82).
///
/// Emitted when a peer disconnects (WebSocket close) or when their
/// presence TTL expires (no heartbeat within `presence_ttl_ms`).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceLeaveMsg {
    /// Channel the user left.
    pub channel: String,
    /// User who left.
    pub user_id: String,
}
