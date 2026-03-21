//! rkyv wire protocol for NodeDB-Lite sync.
//!
//! Frame format: `[msg_type: 1B][length: 4B][rkyv body]`
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

use serde::{Deserialize, Serialize};

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
    /// body if serialization fails. Logs a warning on fallback.
    pub fn encode_or_empty<T: Serialize>(msg_type: SyncMessageType, value: &T) -> Self {
        Self::new_msgpack(msg_type, value).unwrap_or_else(|| {
            tracing::warn!(?msg_type, "frame serialization failed, sending empty body");
            Self {
                msg_type,
                body: Vec::new(),
            }
        })
    }

    /// Deserialize the body from MessagePack.
    pub fn decode_body<'a, T: Deserialize<'a>>(&'a self) -> Option<T> {
        rmp_serde::from_slice(&self.body).ok()
    }
}

/// Handshake message (client → server).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMsg {
    /// JWT bearer token for authentication.
    pub jwt_token: String,
    /// Client's vector clock: `{ collection: { doc_id: lamport_ts } }`.
    pub vector_clock: std::collections::HashMap<String, std::collections::HashMap<String, u64>>,
    /// Shape IDs the client is subscribed to.
    pub subscribed_shapes: Vec<String>,
    /// Client version string.
    pub client_version: String,
}

/// Handshake acknowledgment (server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeAckMsg {
    /// Whether the handshake succeeded.
    pub success: bool,
    /// Session ID assigned by the server.
    pub session_id: String,
    /// Server's vector clock (for initial sync).
    pub server_clock: std::collections::HashMap<String, u64>,
    /// Error message (if !success).
    pub error: Option<String>,
}

/// Delta push message (client → server).
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
}

/// Delta acknowledgment (server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaAckMsg {
    /// Mutation ID being acknowledged.
    pub mutation_id: u64,
    /// Server-assigned LSN for this mutation.
    pub lsn: u64,
}

/// Delta rejection (server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaRejectMsg {
    /// Mutation ID being rejected.
    pub mutation_id: u64,
    /// Reason for rejection.
    pub reason: String,
    /// Compensation hints for the client.
    pub compensation: Option<CompensationHint>,
}

/// Typed compensation hints for rejected deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompensationHint {
    UniqueViolation {
        field: String,
        conflicting_value: String,
    },
    ForeignKeyMissing {
        referenced_id: String,
    },
    PermissionDenied,
    RateLimited {
        retry_after_ms: u64,
    },
}

/// Vector clock sync message (bidirectional).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorClockSyncMsg {
    /// Per-collection clock: `{ collection: max_lsn }`.
    pub clocks: std::collections::HashMap<String, u64>,
    /// Sender's node/peer ID.
    pub sender_id: u64,
}

/// Ping/Pong keepalive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingPongMsg {
    /// Timestamp (epoch milliseconds) for RTT measurement.
    pub timestamp_ms: u64,
    /// Whether this is a pong (response to ping).
    pub is_pong: bool,
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
            vector_clock: std::collections::HashMap::new(),
            subscribed_shapes: vec!["shape1".into()],
            client_version: "0.1.0".into(),
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
            0x01, 0x02, 0x10, 0x11, 0x12, 0x20, 0x21, 0x22, 0x23, 0x30, 0xFF,
        ] {
            let mt = SyncMessageType::from_u8(v).unwrap();
            assert_eq!(mt as u8, v);
        }
        assert!(SyncMessageType::from_u8(0x99).is_none());
    }
}
