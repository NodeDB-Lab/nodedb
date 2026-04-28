//! Integration tests verifying Origin and Lite speak the exact same sync protocol.
//!
//! Uses shared types from `nodedb-types` to build frames and verify roundtrip
//! compatibility between what Lite sends and what Origin parses.

use std::collections::HashMap;

use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::*;

#[test]
fn lite_handshake_frame_roundtrips() {
    let msg = HandshakeMsg {
        jwt_token: "eyJ.test.token".into(),
        vector_clock: {
            let mut m = HashMap::new();
            let mut inner = HashMap::new();
            inner.insert("0000000000000001".into(), 42u64);
            m.insert("_global".into(), inner);
            m
        },
        subscribed_shapes: vec!["shape-1".into(), "shape-2".into()],
        client_version: "0.1.0-lite".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: 1,
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::Handshake, &msg).unwrap();
    let bytes = frame.to_bytes();
    let parsed: HandshakeMsg = SyncFrame::from_bytes(&bytes)
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.jwt_token, "eyJ.test.token");
    assert_eq!(parsed.subscribed_shapes.len(), 2);
    assert_eq!(parsed.client_version, "0.1.0-lite");
}

#[test]
fn lite_delta_push_frame_roundtrips() {
    let msg = DeltaPushMsg {
        collection: "knowledge_base".into(),
        document_id: "doc-42".into(),
        delta: vec![0xDE, 0xAD, 0xBE, 0xEF],
        peer_id: 12345,
        mutation_id: 7,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::DeltaPush, &msg).unwrap();
    let parsed: DeltaPushMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.collection, "knowledge_base");
    assert_eq!(parsed.delta, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(parsed.peer_id, 12345);
    assert_eq!(parsed.mutation_id, 7);
}

#[test]
fn origin_reject_unique_violation_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 42,
        reason: "UNIQUE(users.email)".into(),
        compensation: Some(CompensationHint::UniqueViolation {
            field: "email".into(),
            conflicting_value: "alice@example.com".into(),
        }),
    };

    let frame = SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject).expect("encode");
    let parsed: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.mutation_id, 42);
    match parsed.compensation.unwrap() {
        CompensationHint::UniqueViolation {
            field,
            conflicting_value,
        } => {
            assert_eq!(field, "email");
            assert_eq!(conflicting_value, "alice@example.com");
        }
        other => panic!("expected UniqueViolation, got {other:?}"),
    }
}

#[test]
fn origin_reject_fk_missing_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 99,
        reason: "FK missing".into(),
        compensation: Some(CompensationHint::ForeignKeyMissing {
            referenced_id: "user-999".into(),
        }),
    };

    let frame = SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject).expect("encode");
    let parsed: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert!(matches!(
        parsed.compensation,
        Some(CompensationHint::ForeignKeyMissing { .. })
    ));
}

#[test]
fn origin_reject_permission_denied_no_leak() {
    let reject = DeltaRejectMsg {
        mutation_id: 5,
        reason: "security".into(),
        compensation: Some(CompensationHint::PermissionDenied),
    };

    let frame = SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject).expect("encode");
    let parsed: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert!(matches!(
        parsed.compensation,
        Some(CompensationHint::PermissionDenied)
    ));
    assert!(!parsed.reason.contains("rls_policy"));
}

#[test]
fn origin_reject_rate_limited_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 10,
        reason: "rate limited".into(),
        compensation: Some(CompensationHint::RateLimited {
            retry_after_ms: 5000,
        }),
    };

    let frame = SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject).expect("encode");
    let parsed: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    match parsed.compensation.unwrap() {
        CompensationHint::RateLimited { retry_after_ms } => assert_eq!(retry_after_ms, 5000),
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

#[test]
fn lite_document_shape_subscribe() {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "user-data".into(),
            tenant_id: 5,
            shape_type: ShapeType::Document {
                collection: "users".into(),
                predicate: Vec::new(),
            },
            description: "all users".into(),
            field_filter: vec![],
        },
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSubscribe, &msg).unwrap();
    let parsed: ShapeSubscribeMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.shape.shape_id, "user-data");
    assert!(parsed.shape.could_match("users", "u1"));
    assert!(!parsed.shape.could_match("orders", "o1"));
}

#[test]
fn lite_graph_shape_subscribe() {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "alice-net".into(),
            tenant_id: 1,
            shape_type: ShapeType::Graph {
                root_nodes: vec!["alice".into()],
                max_depth: 2,
                edge_label: Some("KNOWS".into()),
            },
            description: "alice network".into(),
            field_filter: vec![],
        },
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSubscribe, &msg).unwrap();
    let parsed: ShapeSubscribeMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    match &parsed.shape.shape_type {
        ShapeType::Graph {
            root_nodes,
            max_depth,
            edge_label,
        } => {
            assert_eq!(root_nodes, &["alice"]);
            assert_eq!(*max_depth, 2);
            assert_eq!(edge_label.as_deref(), Some("KNOWS"));
        }
        _ => panic!("expected Graph shape"),
    }
}

#[test]
fn lite_vector_shape_subscribe() {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "emb-all".into(),
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
    let parsed: ShapeSubscribeMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert!(parsed.shape.could_match("embeddings", "e1"));
}

#[test]
fn vector_clock_sync_roundtrip() {
    let msg = VectorClockSyncMsg {
        clocks: {
            let mut m = HashMap::new();
            m.insert("0000000000000001".into(), 100u64);
            m.insert("0000000000000002".into(), 200u64);
            m
        },
        sender_id: 1,
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::VectorClockSync, &msg).unwrap();
    let parsed: VectorClockSyncMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.clocks.len(), 2);
    assert_eq!(*parsed.clocks.get("0000000000000001").unwrap(), 100);
}

#[test]
fn ping_pong_roundtrip() {
    let ping = PingPongMsg {
        timestamp_ms: 1711000000000,
        is_pong: false,
    };

    let frame = SyncFrame::new_msgpack(SyncMessageType::PingPong, &ping).unwrap();
    let parsed: PingPongMsg = SyncFrame::from_bytes(&frame.to_bytes())
        .unwrap()
        .decode_body()
        .unwrap();

    assert_eq!(parsed.timestamp_ms, 1711000000000);
    assert!(!parsed.is_pong);
}

#[test]
fn all_11_message_types_valid() {
    for (code, expected) in [
        (0x01u8, SyncMessageType::Handshake),
        (0x02, SyncMessageType::HandshakeAck),
        (0x10, SyncMessageType::DeltaPush),
        (0x11, SyncMessageType::DeltaAck),
        (0x12, SyncMessageType::DeltaReject),
        (0x20, SyncMessageType::ShapeSubscribe),
        (0x21, SyncMessageType::ShapeSnapshot),
        (0x22, SyncMessageType::ShapeDelta),
        (0x23, SyncMessageType::ShapeUnsubscribe),
        (0x30, SyncMessageType::VectorClockSync),
        (0xFF, SyncMessageType::PingPong),
    ] {
        let parsed = SyncMessageType::from_u8(code).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed as u8, code);
    }
    assert!(SyncMessageType::from_u8(0x99).is_none());
}

/// Roundtrip the `VectorPrimaryConfig` through the same MessagePack channel
/// the catalog and sync layer use. If this fails, vector-primary collections
/// either lose configuration on persist/restore or diverge between Origin
/// and Lite after a sync handshake.
#[test]
fn vector_primary_config_msgpack_roundtrip_via_sync_channel() {
    use nodedb_types::collection_config::{PrimaryEngine, VectorPrimaryConfig};
    use nodedb_types::vector_ann::VectorQuantization;
    use nodedb_types::vector_distance::DistanceMetric;

    let cfg = VectorPrimaryConfig {
        vector_field: "embedding".into(),
        dim: 1024,
        quantization: VectorQuantization::RaBitQ,
        m: 32,
        ef_construction: 200,
        metric: DistanceMetric::Cosine,
        payload_indexes: vec![
            ("category".into(), nodedb_types::PayloadIndexKind::Equality),
            ("timestamp".into(), nodedb_types::PayloadIndexKind::Range),
        ],
    };

    let bytes = zerompk::to_msgpack_vec(&cfg).expect("encode");
    let back: VectorPrimaryConfig = zerompk::from_msgpack(&bytes).expect("decode");

    assert_eq!(back, cfg);
    // Sanity: PrimaryEngine roundtrips too (used as a sibling field on
    // StoredCollection).
    let pe = PrimaryEngine::Vector;
    let pe_bytes = zerompk::to_msgpack_vec(&pe).expect("encode primary");
    let pe_back: PrimaryEngine = zerompk::from_msgpack(&pe_bytes).expect("decode primary");
    assert_eq!(pe_back, pe);
}

/// A pre-existing catalog entry written before the `primary` /
/// `vector_primary` fields existed must still deserialize, defaulting to
/// `PrimaryEngine::Document` and `vector_primary = None`. This guards the
/// upgrade path when an older Lite client connects to a newer Origin (or
/// vice-versa).
#[test]
fn legacy_stored_collection_without_primary_fields_roundtrips() {
    use nodedb_types::collection_config::PrimaryEngine;

    // A msgpack map with a single field — analogous to a 1-field early
    // version of `StoredCollection`. We then read it back as a struct
    // that has the new `primary` and `vector_primary` fields with
    // `#[msgpack(default)]`. This proves the defaults are honored.
    #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq)]
    #[msgpack(map)]
    struct LegacyShape {
        pub name: String,
    }

    #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq)]
    #[msgpack(map)]
    struct NewShape {
        pub name: String,
        #[msgpack(default)]
        pub primary: PrimaryEngine,
        #[msgpack(default)]
        pub vector_primary: Option<nodedb_types::collection_config::VectorPrimaryConfig>,
    }

    let legacy = LegacyShape {
        name: "users".into(),
    };
    let bytes = zerompk::to_msgpack_vec(&legacy).expect("encode legacy");
    let upgraded: NewShape = zerompk::from_msgpack(&bytes).expect("decode upgraded");
    assert_eq!(upgraded.name, "users");
    assert_eq!(upgraded.primary, PrimaryEngine::Document);
    assert!(upgraded.vector_primary.is_none());
}
