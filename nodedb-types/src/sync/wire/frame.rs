// SPDX-License-Identifier: Apache-2.0

//! Wire frame format and message-type discriminants.
//!
//! Additional opcodes beyond what the module-level doc in `mod.rs` lists:
//! - `0xA2` VectorInsert (client → server)
//! - `0xA3` VectorInsertAck (server → client)
//! - `0xA4` VectorDelete (client → server)
//! - `0xA5` VectorDeleteAck (server → client)
//! - `0xA6` FtsIndex (client → server)
//! - `0xA7` FtsIndexAck (server → client)
//! - `0xA8` FtsDelete (client → server)
//! - `0xA9` FtsDeleteAck (server → client)
//! - `0xAA` SpatialInsert (client → server)
//! - `0xAB` SpatialInsertAck (server → client)
//! - `0xAC` SpatialDelete (client → server)
//! - `0xAD` SpatialDeleteAck (server → client)
//! - `0xAE` KvPush (client → server)
//! - `0xAF` KvPushAck (server → client)
//! - `0x16` RowPushReject (client → server)

/// Sync message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum SyncMessageType {
    Handshake = 0x01,
    HandshakeAck = 0x02,
    DeltaPush = 0x10,
    DeltaAck = 0x11,
    DeltaReject = 0x12,
    /// Collection schema descriptor announcement (bidirectional, 0x13).
    /// Carries a `CollectionDescriptor` so any peer can materialize the
    /// collection in its catalog with the correct engine + config.
    CollectionSchema = 0x13,
    /// Collection purged notification (server → client, 0x14).
    /// Sent when an Origin collection is hard-deleted (UNDROP window
    /// expired or explicit `DROP COLLECTION ... PURGE`). The client
    /// must drop local Loro state and remove the collection's redb
    /// record; future deltas for the collection are not sync-eligible.
    CollectionPurged = 0x14,
    /// Row post-image push (server → client, 0x15).
    ///
    /// Carries a row's full post-image as MessagePack, NOT Loro update
    /// bytes — distinct from [`Self::DeltaPush`], which is client → server
    /// and carries a CRDT delta. Origin fans these out for writes that
    /// originated on the server (SQL DML, DDL-managed system rows), where no
    /// client-authored CRDT operation exists to replicate.
    RowPush = 0x15,
    /// Row push refusal (client → server, 0x16).
    ///
    /// Lite sends this for a [`Self::RowPush`] it could not apply.
    RowPushReject = 0x16,
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
    /// Array CRDT delta (single op, client → server, 0x90).
    ArrayDelta = 0x90,
    /// Array CRDT delta batch (multiple ops, client → server, 0x91).
    ArrayDeltaBatch = 0x91,
    /// Array snapshot header (server → client, 0x92).
    ArraySnapshot = 0x92,
    /// Array snapshot chunk (server → client, 0x93).
    ArraySnapshotChunk = 0x93,
    /// Array schema CRDT sync (bidirectional, 0x94).
    ArraySchema = 0x94,
    /// Array ack — advances GC frontier (client → server, 0x95).
    ArrayAck = 0x95,
    /// Array reject (server → client, 0x96). Compensation hint.
    ArrayReject = 0x96,
    /// Array catchup request (client → server, 0x97).
    ArrayCatchupRequest = 0x97,
    /// Columnar batch insert (client → server, 0xA0).
    ColumnarInsert = 0xA0,
    /// Columnar insert acknowledgment (server → client, 0xA1).
    ColumnarInsertAck = 0xA1,
    /// Vector insert (client → server, 0xA2).
    VectorInsert = 0xA2,
    /// Vector insert acknowledgment (server → client, 0xA3).
    VectorInsertAck = 0xA3,
    /// Vector delete (client → server, 0xA4).
    VectorDelete = 0xA4,
    /// Vector delete acknowledgment (server → client, 0xA5).
    VectorDeleteAck = 0xA5,
    /// FTS document index (client → server, 0xA6).
    FtsIndex = 0xA6,
    /// FTS index acknowledgment (server → client, 0xA7).
    FtsIndexAck = 0xA7,
    /// FTS document delete (client → server, 0xA8).
    FtsDelete = 0xA8,
    /// FTS delete acknowledgment (server → client, 0xA9).
    FtsDeleteAck = 0xA9,
    /// Spatial geometry insert (client → server, 0xAA).
    SpatialInsert = 0xAA,
    /// Spatial insert acknowledgment (server → client, 0xAB).
    SpatialInsertAck = 0xAB,
    /// Spatial geometry delete (client → server, 0xAC).
    SpatialDelete = 0xAC,
    /// Spatial delete acknowledgment (server → client, 0xAD).
    SpatialDeleteAck = 0xAD,
    /// KV write push (client → server, 0xAE).
    KvPush = 0xAE,
    /// KV push acknowledgment (server → client, 0xAF).
    KvPushAck = 0xAF,
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
            0x13 => Some(Self::CollectionSchema),
            0x14 => Some(Self::CollectionPurged),
            0x15 => Some(Self::RowPush),
            0x16 => Some(Self::RowPushReject),
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
            0x90 => Some(Self::ArrayDelta),
            0x91 => Some(Self::ArrayDeltaBatch),
            0x92 => Some(Self::ArraySnapshot),
            0x93 => Some(Self::ArraySnapshotChunk),
            0x94 => Some(Self::ArraySchema),
            0x95 => Some(Self::ArrayAck),
            0x96 => Some(Self::ArrayReject),
            0x97 => Some(Self::ArrayCatchupRequest),
            0xA0 => Some(Self::ColumnarInsert),
            0xA1 => Some(Self::ColumnarInsertAck),
            0xA2 => Some(Self::VectorInsert),
            0xA3 => Some(Self::VectorInsertAck),
            0xA4 => Some(Self::VectorDelete),
            0xA5 => Some(Self::VectorDeleteAck),
            0xA6 => Some(Self::FtsIndex),
            0xA7 => Some(Self::FtsIndexAck),
            0xA8 => Some(Self::FtsDelete),
            0xA9 => Some(Self::FtsDeleteAck),
            0xAA => Some(Self::SpatialInsert),
            0xAB => Some(Self::SpatialInsertAck),
            0xAC => Some(Self::SpatialDelete),
            0xAD => Some(Self::SpatialDeleteAck),
            0xAE => Some(Self::KvPush),
            0xAF => Some(Self::KvPushAck),
            0xFF => Some(Self::PingPong),
            _ => None,
        }
    }
}

/// Wire frame: wraps a message type + serialized body with format versioning
/// and CRC32C integrity protection.
///
/// Layout: `[version: 1B][msg_type: 1B][length: 4B LE][crc32c: 4B LE][body: N bytes]`
/// Total header: 10 bytes.
///
/// Mirrors the WAL record header model: every frame carries a format version
/// byte (enabling hard rejection of future incompatible formats) and a CRC32C
/// checksum over the body for silent-corruption detection. There is no
/// checksum-skip sentinel — CRC32C is always computed and always verified.
///
/// `#[non_exhaustive]` — additional header fields (e.g. compression flag,
/// session token) may be added without breaking downstream consumers.
#[non_exhaustive]
#[derive(Clone)]
pub struct SyncFrame {
    pub msg_type: SyncMessageType,
    pub body: Vec<u8>,
}

impl SyncFrame {
    /// Current wire format version embedded in every frame header.
    pub const FORMAT_VERSION: u8 = 1;

    /// Total size of the frame header in bytes.
    ///
    /// Layout: `[version:1][msg_type:1][len:4 LE][crc32c:4 LE]` = 10 bytes.
    pub const HEADER_SIZE: usize = 10;

    /// Serialize a frame to bytes.
    ///
    /// Produces `[FORMAT_VERSION][msg_type][body_len as u32 LE][crc32c(body) as u32 LE][body]`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.body.len() as u32;
        let crc = crc32c::crc32c(&self.body);
        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + self.body.len());
        buf.push(Self::FORMAT_VERSION);
        buf.push(self.msg_type as u8);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&self.body);
        buf
    }

    /// Deserialize a frame from bytes.
    ///
    /// Returns `None` if:
    /// - the buffer is shorter than `HEADER_SIZE`,
    /// - the version byte does not match `FORMAT_VERSION` (unknown future version),
    /// - the message type discriminant is unrecognised,
    /// - the buffer is too short to contain the declared body length, or
    /// - the CRC32C of the body does not match the header checksum (corrupt frame).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::HEADER_SIZE {
            return None;
        }
        let version = data[0];
        if version != Self::FORMAT_VERSION {
            return None;
        }
        let msg_type = SyncMessageType::from_u8(data[1])?;
        let len = u32::from_le_bytes(data[2..6].try_into().ok()?) as usize;
        let expected_crc = u32::from_le_bytes(data[6..10].try_into().ok()?);
        if data.len() < Self::HEADER_SIZE + len {
            return None;
        }
        let body = data[Self::HEADER_SIZE..Self::HEADER_SIZE + len].to_vec();
        let actual_crc = crc32c::crc32c(&body);
        if actual_crc != expected_crc {
            tracing::warn!(
                msg_type = data[1],
                expected_crc,
                actual_crc,
                "sync frame CRC32C mismatch; dropping corrupt frame"
            );
            return None;
        }
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

    /// Try to encode a value into a SyncFrame body.
    ///
    /// Returns `None` and logs an error on serialization failure — callers
    /// should propagate via `?`. The protocol must never ship a frame
    /// whose body did not serialize successfully.
    pub fn try_encode<T: zerompk::ToMessagePack>(
        msg_type: SyncMessageType,
        value: &T,
    ) -> Option<Self> {
        match zerompk::to_msgpack_vec(value) {
            Ok(body) => Some(Self { msg_type, body }),
            Err(e) => {
                tracing::error!(
                    msg_type = msg_type as u8,
                    error = %e,
                    "failed to encode sync frame body; dropping response"
                );
                None
            }
        }
    }

    /// Deserialize the body from MessagePack.
    pub fn decode_body<T: zerompk::FromMessagePackOwned>(&self) -> Option<T> {
        zerompk::from_msgpack(&self.body).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(msg_type: SyncMessageType, body: Vec<u8>) -> SyncFrame {
        SyncFrame { msg_type, body }
    }

    #[test]
    fn roundtrip_preserves_msg_type_and_body() {
        let body = b"hello sync world".to_vec();
        let frame = make_frame(SyncMessageType::PingPong, body.clone());
        let bytes = frame.to_bytes();
        let decoded = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, SyncMessageType::PingPong);
        assert_eq!(decoded.body, body);
    }

    #[test]
    fn flipped_body_byte_returns_none() {
        let body = b"integrity check".to_vec();
        let frame = make_frame(SyncMessageType::DeltaPush, body);
        let mut bytes = frame.to_bytes();
        // Flip a bit in the first body byte (located at HEADER_SIZE).
        bytes[SyncFrame::HEADER_SIZE] ^= 0xFF;
        assert!(SyncFrame::from_bytes(&bytes).is_none());
    }

    #[test]
    fn truncated_buffer_returns_none() {
        // Buffer shorter than HEADER_SIZE.
        assert!(SyncFrame::from_bytes(&[]).is_none());
        assert!(SyncFrame::from_bytes(&[1u8; SyncFrame::HEADER_SIZE - 1]).is_none());

        // Header intact but body truncated by one byte.
        let frame = make_frame(SyncMessageType::PingPong, b"abcdef".to_vec());
        let bytes = frame.to_bytes();
        let truncated = &bytes[..bytes.len() - 1];
        assert!(SyncFrame::from_bytes(truncated).is_none());
    }

    #[test]
    fn wrong_version_returns_none() {
        let frame = make_frame(SyncMessageType::PingPong, b"version test".to_vec());
        let mut bytes = frame.to_bytes();
        // Overwrite the version byte with something other than FORMAT_VERSION.
        bytes[0] = SyncFrame::FORMAT_VERSION.wrapping_add(1);
        assert!(SyncFrame::from_bytes(&bytes).is_none());
    }

    #[test]
    fn header_size_is_ten_and_total_length_is_correct() {
        assert_eq!(SyncFrame::HEADER_SIZE, 10);
        let body = b"nodedb".to_vec();
        let frame = make_frame(SyncMessageType::PingPong, body.clone());
        let bytes = frame.to_bytes();
        assert_eq!(bytes.len(), SyncFrame::HEADER_SIZE + body.len());
    }

    #[test]
    fn crc32c_field_matches_crate_output() {
        let body = b"crc check".to_vec();
        let frame = make_frame(SyncMessageType::Handshake, body.clone());
        let bytes = frame.to_bytes();
        // CRC32C is stored at bytes[6..10] LE.
        let stored = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
        let expected = crc32c::crc32c(&body);
        assert_eq!(stored, expected);
    }

    #[test]
    fn frame_roundtrip() {
        let ping = crate::sync::wire::PingPongMsg {
            timestamp_ms: 12345,
            is_pong: false,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::PingPong, &ping).unwrap();
        let bytes = frame.to_bytes();
        let decoded = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, SyncMessageType::PingPong);
        let decoded_ping: crate::sync::wire::PingPongMsg = decoded.decode_body().unwrap();
        assert_eq!(decoded_ping.timestamp_ms, 12345);
        assert!(!decoded_ping.is_pong);
    }

    #[test]
    fn message_type_roundtrip() {
        for v in [
            0x01, 0x02, 0x10, 0x11, 0x12, 0x14, 0x20, 0x21, 0x22, 0x23, 0x30, 0x40, 0x41, 0x50,
            0x52, 0x60, 0x61, 0x70, 0x80, 0x81, 0x82, 0xFF,
        ] {
            let mt = SyncMessageType::from_u8(v).unwrap();
            assert_eq!(mt as u8, v);
        }
        assert!(SyncMessageType::from_u8(0x99).is_none());
    }
}
