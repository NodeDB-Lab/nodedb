//! Sync wire protocol: frame format and message types.
//!
//! Frame format: `[msg_type: 1B][length: 4B LE][rkyv/msgpack body]`
//!
//! Message types:
//! - `0x01` Handshake (client → server)
//! - `0x02` HandshakeAck (server → client)
//! - `0x10` DeltaPush (client → server)
//! - `0x11` DeltaAck (server → client)
//! - `0x12` DeltaReject (server → client)
//! - `0x20` ShapeSubscribe (client → server)
//! - `0x21` ShapeSnapshot (server → client)
//! - `0x22` ShapeDelta (server → client)
//! - `0x23` ShapeUnsubscribe (client → server)
//! - `0x30` VectorClockSync (bidirectional)
//! - `0xFF` Ping/Pong (bidirectional)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::compensation::CompensationHint;
use super::shape::ShapeDefinition;

/// Sync message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SyncMessageType {
    Handshake = 0x01,
    HandshakeAck = 0x02,
    DeltaPush = 0x10,
    DeltaAck = 0x11,
    DeltaReject = 0x12,
    ShapeSubscribe = 0x20,
    ShapeSnapshot = 0x21,
    ShapeDelta = 0x22,
    ShapeUnsubscribe = 0x23,
    VectorClockSync = 0x30,
    /// Timeseries metric batch push (client → server, 0x40).
    TimeseriesPush = 0x40,
    /// Timeseries push acknowledgment (server → client, 0x41).
    TimeseriesAck = 0x41,
    /// Re-sync request (bidirectional, 0x50).
    /// Sent when sequence gaps or checksum failures are detected.
    ResyncRequest = 0x50,
    /// Downstream throttle (client → server, 0x52).
    /// Sent when Lite's incoming queue is overwhelmed.
    Throttle = 0x52,
    /// Token refresh request (client → server, 0x60).
    TokenRefresh = 0x60,
    /// Token refresh acknowledgment (server → client, 0x61).
    TokenRefreshAck = 0x61,
    /// Definition sync (server → client, 0x70).
    /// Carries function/trigger/procedure definitions from Origin to Lite.
    DefinitionSync = 0x70,
    PingPong = 0xFF,
}

impl SyncMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Handshake),
            0x02 => Some(Self::HandshakeAck),
            0x10 => Some(Self::DeltaPush),
            0x11 => Some(Self::DeltaAck),
            0x12 => Some(Self::DeltaReject),
            0x20 => Some(Self::ShapeSubscribe),
            0x21 => Some(Self::ShapeSnapshot),
            0x22 => Some(Self::ShapeDelta),
            0x23 => Some(Self::ShapeUnsubscribe),
            0x30 => Some(Self::VectorClockSync),
            0x40 => Some(Self::TimeseriesPush),
            0x41 => Some(Self::TimeseriesAck),
            0x50 => Some(Self::ResyncRequest),
            0x52 => Some(Self::Throttle),
            0x60 => Some(Self::TokenRefresh),
            0x61 => Some(Self::TokenRefreshAck),
            0x70 => Some(Self::DefinitionSync),
            0xFF => Some(Self::PingPong),
            _ => None,
        }
    }
}

/// Wire frame: wraps a message type + serialized body.
///
/// Layout: `[msg_type: 1B][length: 4B LE][body: N bytes]`
/// Total header: 5 bytes.
pub struct SyncFrame {
    pub msg_type: SyncMessageType,
    pub body: Vec<u8>,
}

impl SyncFrame {
    pub const HEADER_SIZE: usize = 5;

    /// Serialize a frame to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.body.len() as u32;
        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + self.body.len());
        buf.push(self.msg_type as u8);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&self.body);
        buf
    }

    /// Deserialize a frame from bytes.
    ///
    /// Returns `None` if the data is too short or the message type is unknown.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::HEADER_SIZE {
            return None;
        }
        let msg_type = SyncMessageType::from_u8(data[0])?;
        let len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        if data.len() < Self::HEADER_SIZE + len {
            return None;
        }
        let body = data[Self::HEADER_SIZE..Self::HEADER_SIZE + len].to_vec();
        Some(Self { msg_type, body })
    }

    /// Create a frame with a MessagePack-serialized body.
    pub fn new_msgpack<T: Serialize>(msg_type: SyncMessageType, value: &T) -> Option<Self> {
        let body = rmp_serde::to_vec_named(value).ok()?;
        Some(Self { msg_type, body })
    }

    /// Create a frame from a serializable value, falling back to an empty
    /// body if serialization fails.
    pub fn encode_or_empty<T: Serialize>(msg_type: SyncMessageType, value: &T) -> Self {
        Self::new_msgpack(msg_type, value).unwrap_or(Self {
            msg_type,
            body: Vec::new(),
        })
    }

    /// Deserialize the body from MessagePack.
    pub fn decode_body<'a, T: Deserialize<'a>>(&'a self) -> Option<T> {
        rmp_serde::from_slice(&self.body).ok()
    }
}

// ─── Message Payloads ───────────────────────────────────────────────────────

/// Handshake message (client → server, 0x01).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Delta push message (client → server, 0x10).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaPushMsg {
    /// Collection the delta applies to.
    pub collection: String,
    /// Document ID.
    pub document_id: String,
    /// Loro CRDT delta bytes.
    pub delta: Vec<u8>,
    /// Client's peer ID (for CRDT identity).
    pub peer_id: u64,
    /// Per-mutation unique ID for dedup.
    pub mutation_id: u64,
    /// CRC32C checksum of `delta` bytes for integrity verification.
    /// Computed by sender, validated by receiver. 0 for legacy clients.
    #[serde(default)]
    pub checksum: u32,
}

/// Delta acknowledgment (server → client, 0x11).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaAckMsg {
    /// Mutation ID being acknowledged.
    pub mutation_id: u64,
    /// Server-assigned LSN for this mutation.
    pub lsn: u64,
}

/// Delta rejection (server → client, 0x12).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaRejectMsg {
    /// Mutation ID being rejected.
    pub mutation_id: u64,
    /// Reason for rejection.
    pub reason: String,
    /// Compensation hints for the client.
    pub compensation: Option<CompensationHint>,
}

/// Shape subscribe request (client → server, 0x20).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeSubscribeMsg {
    /// Shape definition to subscribe to.
    pub shape: ShapeDefinition,
}

/// Shape snapshot response (server → client, 0x21).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeSnapshotMsg {
    /// Shape ID this snapshot belongs to.
    pub shape_id: String,
    /// Initial dataset: serialized document rows matching the shape.
    pub data: Vec<u8>,
    /// LSN at snapshot time — deltas after this LSN will follow.
    pub snapshot_lsn: u64,
    /// Number of documents in the snapshot.
    pub doc_count: usize,
}

/// Shape delta message (server → client, 0x22).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeDeltaMsg {
    /// Shape ID this delta applies to.
    pub shape_id: String,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Operation type: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Delta payload (CRDT delta bytes or document value).
    pub delta: Vec<u8>,
    /// WAL LSN of this mutation.
    pub lsn: u64,
}

/// Shape unsubscribe request (client → server, 0x23).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeUnsubscribeMsg {
    pub shape_id: String,
}

/// Vector clock sync message (bidirectional, 0x30).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorClockSyncMsg {
    /// Per-collection clock: `{ collection: max_lsn }`.
    pub clocks: HashMap<String, u64>,
    /// Sender's node/peer ID.
    pub sender_id: u64,
}

/// Re-sync request message (bidirectional, 0x50).
///
/// Sent when a receiver detects:
/// - Sequence gap: missing `mutation_id`s in the delta stream
/// - Checksum failure: CRC32C mismatch on a delta payload
/// - State divergence: local state inconsistent with received deltas
///
/// On receiving a ResyncRequest, the sender should:
/// 1. Re-send all deltas from `from_mutation_id` onwards, OR
/// 2. Send a full snapshot if `from_mutation_id` is 0
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncRequestMsg {
    /// Reason for requesting re-sync.
    pub reason: ResyncReason,
    /// Resume from this mutation ID (0 = full re-sync).
    pub from_mutation_id: u64,
    /// Collection scope (empty = all collections).
    pub collection: String,
}

/// Reason for a re-sync request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResyncReason {
    /// Detected missing mutation IDs in the delta stream.
    SequenceGap {
        /// The expected next mutation ID.
        expected: u64,
        /// The mutation ID that was actually received.
        received: u64,
    },
    /// CRC32C checksum mismatch on a delta payload.
    ChecksumMismatch {
        /// The mutation ID of the corrupted delta.
        mutation_id: u64,
    },
    /// Corruption detected on cold start, need full re-sync.
    CorruptedState,
}

/// Downstream throttle message (client → server, 0x52).
///
/// Sent by Lite when its incoming shape delta queue is overwhelmed.
/// Origin should reduce its push rate for this peer until a
/// `Throttle { throttle: false }` is received.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottleMsg {
    /// `true` to enable throttling, `false` to release.
    pub throttle: bool,
    /// Current queue depth at Lite (informational).
    pub queue_depth: u64,
    /// Suggested max deltas per second (0 = use server default).
    pub suggested_rate: u64,
}

/// Token refresh request (client → server, 0x60).
///
/// Sent by Lite before the current JWT expires. The client provides
/// a fresh token obtained from the application's auth layer.
/// Origin validates the new token and either upgrades the session
/// or disconnects if the token is invalid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRefreshMsg {
    /// New JWT bearer token.
    pub new_token: String,
}

/// Token refresh acknowledgment (server → client, 0x61).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingPongMsg {
    /// Timestamp (epoch milliseconds) for RTT measurement.
    pub timestamp_ms: u64,
    /// Whether this is a pong (response to ping).
    pub is_pong: bool,
}

/// Timeseries metric batch push (client → server, 0x40).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesPushMsg {
    /// Source Lite instance ID (UUID v7).
    pub lite_id: String,
    /// Collection name.
    pub collection: String,
    /// Gorilla-encoded timestamp block.
    pub ts_block: Vec<u8>,
    /// Gorilla-encoded value block.
    pub val_block: Vec<u8>,
    /// Raw LE u64 series ID block.
    pub series_block: Vec<u8>,
    /// Number of samples in this batch.
    pub sample_count: u64,
    /// Min timestamp in this batch.
    pub min_ts: i64,
    /// Max timestamp in this batch.
    pub max_ts: i64,
    /// Per-series sync watermark: highest LSN already synced for each series.
    /// Only samples after these watermarks are included.
    pub watermarks: HashMap<u64, u64>,
}

/// Timeseries push acknowledgment (server → client, 0x41).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Number of samples accepted.
    pub accepted: u64,
    /// Number of samples rejected (duplicates, out-of-retention, etc.)
    pub rejected: u64,
    /// Server-assigned LSN for this batch (used as sync watermark).
    pub lsn: u64,
}

/// Definition sync message (server → client, 0x70).
///
/// Carries function/trigger/procedure definitions from Origin to Lite.
/// Sent when definitions are created, modified, or dropped on Origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionSyncMsg {
    /// Type of definition: "function", "trigger", "procedure".
    pub definition_type: String,
    /// The definition name.
    pub name: String,
    /// Action: "put" (create/replace) or "delete" (drop).
    pub action: String,
    /// Serialized definition body (JSON). Empty for "delete" actions.
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let ping = PingPongMsg {
            timestamp_ms: 12345,
            is_pong: false,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::PingPong, &ping).unwrap();
        let bytes = frame.to_bytes();
        let decoded = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, SyncMessageType::PingPong);
        let decoded_ping: PingPongMsg = decoded.decode_body().unwrap();
        assert_eq!(decoded_ping.timestamp_ms, 12345);
        assert!(!decoded_ping.is_pong);
    }

    #[test]
    fn handshake_serialization() {
        let msg = HandshakeMsg {
            jwt_token: "test.jwt.token".into(),
            vector_clock: HashMap::new(),
            subscribed_shapes: vec!["shape1".into()],
            client_version: "0.1.0".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: 1,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::Handshake, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert!(bytes.len() > SyncFrame::HEADER_SIZE);
        assert_eq!(bytes[0], 0x01);
    }

    #[test]
    fn delta_reject_with_compensation() {
        let reject = DeltaRejectMsg {
            mutation_id: 42,
            reason: "unique violation".into(),
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "alice@example.com".into(),
            }),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::DeltaReject, &reject).unwrap();
        let decoded: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.mutation_id, 42);
        assert!(matches!(
            decoded.compensation,
            Some(CompensationHint::UniqueViolation { .. })
        ));
    }

    #[test]
    fn message_type_roundtrip() {
        for v in [
            0x01, 0x02, 0x10, 0x11, 0x12, 0x20, 0x21, 0x22, 0x23, 0x30, 0x40, 0x41, 0x50, 0x52,
            0x60, 0x61, 0x70, 0xFF,
        ] {
            let mt = SyncMessageType::from_u8(v).unwrap();
            assert_eq!(mt as u8, v);
        }
        assert!(SyncMessageType::from_u8(0x99).is_none());
    }

    #[test]
    fn shape_subscribe_roundtrip() {
        let msg = ShapeSubscribeMsg {
            shape: ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: super::super::shape::ShapeType::Vector {
                    collection: "embeddings".into(),
                    field_name: None,
                },
                description: "all embeddings".into(),
                field_filter: vec![],
            },
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSubscribe, &msg).unwrap();
        let decoded: ShapeSubscribeMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.shape.shape_id, "s1");
    }
}
