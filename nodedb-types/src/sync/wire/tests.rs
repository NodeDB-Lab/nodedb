use std::collections::HashMap;

use crate::sync::compensation::CompensationHint;
use crate::sync::shape::{ShapeDefinition, ShapeType};
use crate::sync::wire::{
    CollectionPurgedMsg, DeltaRejectMsg, HandshakeMsg, PeerPresence, PingPongMsg,
    PresenceBroadcastMsg, PresenceLeaveMsg, PresenceUpdateMsg, ShapeSubscribeMsg, SyncFrame,
    SyncMessageType,
};

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
        0x01, 0x02, 0x10, 0x11, 0x12, 0x14, 0x20, 0x21, 0x22, 0x23, 0x30, 0x40, 0x41, 0x50, 0x52,
        0x60, 0x61, 0x70, 0x80, 0x81, 0x82, 0xFF,
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
            shape_type: ShapeType::Vector {
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

#[test]
fn presence_update_roundtrip() {
    let msg = PresenceUpdateMsg {
        channel: "doc:doc-123".into(),
        state: b"user_id:user-42,cursor:blk-7:42".to_vec(),
    };
    let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceUpdate, &msg).unwrap();
    let bytes = frame.to_bytes();
    assert_eq!(bytes[0], 0x80);
    let decoded: PresenceUpdateMsg = SyncFrame::from_bytes(&bytes)
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(decoded.channel, "doc:doc-123");
    assert!(!decoded.state.is_empty());
}

#[test]
fn presence_broadcast_roundtrip() {
    let msg = PresenceBroadcastMsg {
        channel: "doc:doc-123".into(),
        peers: vec![
            PeerPresence {
                user_id: "user-42".into(),
                state: vec![0xDE, 0xAD],
                last_seen_ms: 150,
            },
            PeerPresence {
                user_id: "user-99".into(),
                state: vec![0xBE, 0xEF],
                last_seen_ms: 2300,
            },
        ],
    };
    let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceBroadcast, &msg).unwrap();
    let decoded: PresenceBroadcastMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(decoded.channel, "doc:doc-123");
    assert_eq!(decoded.peers.len(), 2);
    assert_eq!(decoded.peers[0].user_id, "user-42");
    assert_eq!(decoded.peers[1].last_seen_ms, 2300);
}

#[test]
fn presence_leave_roundtrip() {
    let msg = PresenceLeaveMsg {
        channel: "doc:doc-123".into(),
        user_id: "user-42".into(),
    };
    let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceLeave, &msg).unwrap();
    let bytes = frame.to_bytes();
    assert_eq!(bytes[0], 0x82);
    let decoded: PresenceLeaveMsg = SyncFrame::from_bytes(&bytes)
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(decoded.channel, "doc:doc-123");
    assert_eq!(decoded.user_id, "user-42");
}

#[test]
fn collection_purged_roundtrip() {
    let msg = CollectionPurgedMsg {
        tenant_id: 7,
        name: "embeddings".into(),
        purge_lsn: 987_654_321,
    };
    let frame = SyncFrame::new_msgpack(SyncMessageType::CollectionPurged, &msg).unwrap();
    let bytes = frame.to_bytes();
    assert_eq!(bytes[0], 0x14);
    let decoded: CollectionPurgedMsg = SyncFrame::from_bytes(&bytes)
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(decoded.tenant_id, 7);
    assert_eq!(decoded.name, "embeddings");
    assert_eq!(decoded.purge_lsn, 987_654_321);
}
