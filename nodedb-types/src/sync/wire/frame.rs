//! Wire frame format and message-type discriminants.

/// Sync message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SyncMessageType {
    Handshake = 0x01,
    HandshakeAck = 0x02,
    DeltaPush = 0x10,
    DeltaAck = 0x11,
    DeltaReject = 0x12,
    /// Collection purged notification (server → client, 0x14).
    /// Sent when an Origin collection is hard-deleted (UNDROP window
    /// expired or explicit `DROP COLLECTION ... PURGE`). The client
    /// must drop local Loro state and remove the collection's redb
    /// record; future deltas for the collection are not sync-eligible.
    CollectionPurged = 0x14,
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
    /// Presence update (client → server, 0x80).
    PresenceUpdate = 0x80,
    /// Presence broadcast (server → all subscribers except sender, 0x81).
    PresenceBroadcast = 0x81,
    /// Presence leave (server → all subscribers, 0x82).
    PresenceLeave = 0x82,
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
            0x14 => Some(Self::CollectionPurged),
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
            0x80 => Some(Self::PresenceUpdate),
            0x81 => Some(Self::PresenceBroadcast),
            0x82 => Some(Self::PresenceLeave),
            0xFF => Some(Self::PingPong),
            _ => None,
        }
    }
}

/// Wire frame: wraps a message type + serialized body.
///
/// Layout: `[msg_type: 1B][length: 4B LE][body: N bytes]`
/// Total header: 5 bytes.
#[derive(Clone)]
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
    pub fn new_msgpack<T: zerompk::ToMessagePack>(
        msg_type: SyncMessageType,
        value: &T,
    ) -> Option<Self> {
        let body = zerompk::to_msgpack_vec(value).ok()?;
        Some(Self { msg_type, body })
    }

    /// Create a frame from a serializable value, falling back to an empty
    /// body if serialization fails.
    pub fn encode_or_empty<T: zerompk::ToMessagePack>(
        msg_type: SyncMessageType,
        value: &T,
    ) -> Self {
        Self::new_msgpack(msg_type, value).unwrap_or(Self {
            msg_type,
            body: Vec::new(),
        })
    }

    /// Deserialize the body from MessagePack.
    pub fn decode_body<T: zerompk::FromMessagePackOwned>(&self) -> Option<T> {
        zerompk::from_msgpack(&self.body).ok()
    }
}
