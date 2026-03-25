//! Sync wire protocol — re-exports from `nodedb-types`.
//!
//! All wire types are defined in `nodedb-types::sync::wire` so that both
//! Origin and NodeDB-Lite share identical serialization. This module
//! re-exports them for backwards-compatible use within the Origin codebase.

// ── Re-export all wire types from nodedb-types ──
pub use nodedb_types::sync::wire::{
    DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg, HandshakeMsg, PingPongMsg,
    ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, ShapeUnsubscribeMsg, SyncFrame,
    SyncMessageType, TimeseriesAckMsg, TimeseriesPushMsg, VectorClockSyncMsg,
};

// ── Re-export CompensationHint (used by dlq.rs and session.rs) ──
pub use nodedb_types::sync::compensation::CompensationHint;

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
            lite_id: String::new(),
            epoch: 0,
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
            0x01, 0x02, 0x10, 0x11, 0x12, 0x20, 0x21, 0x22, 0x23, 0x30, 0x40, 0x41, 0xFF,
        ] {
            let mt = SyncMessageType::from_u8(v).unwrap();
            assert_eq!(mt as u8, v);
        }
        assert!(SyncMessageType::from_u8(0x99).is_none());
    }
}
