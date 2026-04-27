use std::collections::HashMap;

use crate::control::security::audit::AuditLog;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::{JwtConfig, JwtValidator};
use crate::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
use crate::types::TenantId;

use super::super::dlq::{DlqConfig, SyncDlq};
use super::super::rate_limit::RateLimitConfig;
use super::super::wire::*;
use super::state::SyncSession;

fn make_session() -> SyncSession {
    SyncSession::new("test-session-1".into())
}

fn make_authenticated_session() -> SyncSession {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("alice".into());
    session.identity = Some(AuthenticatedIdentity {
        user_id: 1,
        username: "alice".into(),
        tenant_id: TenantId::new(1),
        auth_method: crate::control::security::identity::AuthMethod::ApiKey,
        roles: vec![crate::control::security::identity::Role::ReadWrite],
        is_superuser: false,
    });
    session
}

#[test]
fn handshake_rejects_invalid_jwt() {
    let mut session = make_session();
    let validator = JwtValidator::new(JwtConfig::default());

    let msg = HandshakeMsg {
        jwt_token: "invalid.token.here".into(),
        vector_clock: HashMap::new(),
        subscribed_shapes: vec![],
        client_version: "0.1".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: 1,
    };

    let response = session
        .handle_handshake(&msg, &validator, HashMap::new(), None)
        .expect("handshake response");
    assert_eq!(response.msg_type, SyncMessageType::HandshakeAck);

    let ack: HandshakeAckMsg = response.decode_body().unwrap();
    assert!(!ack.success);
    assert!(ack.error.is_some());
    assert!(!session.authenticated);
}

#[test]
fn delta_push_rejected_before_auth() {
    let mut session = make_session();

    let msg = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: vec![1, 2, 3],
        peer_id: 1,
        mutation_id: 100,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let response = session.handle_delta_push(&msg, None, None, None);
    assert!(response.is_some());
    let frame = response.unwrap();
    assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
    assert_eq!(session.mutations_rejected, 1);
}

#[test]
fn delta_push_accepted_when_authenticated() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"status": "active"});
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 42,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let response = session.handle_delta_push(&msg, None, None, None);
    assert!(response.is_some());
    assert_eq!(response.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 1);
    // The subscription tracker picked up the collection.
    assert!(
        session
            .tracked_collections
            .contains(&(1, "orders".to_string()))
    );
}

#[test]
fn delta_push_rls_silent_rejection() {
    let mut session = make_authenticated_session();

    let rls_store = RlsPolicyStore::new();
    let filter = crate::bridge::scan_filter::ScanFilter {
        field: "status".into(),
        op: "eq".into(),
        value: nodedb_types::Value::String("active".into()),
        clauses: Vec::new(),
        expr: None,
    };
    let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();
    rls_store
        .create_policy(RlsPolicy {
            name: "require_active".into(),
            collection: "orders".into(),
            tenant_id: 1,
            policy_type: PolicyType::Write,
            predicate,
            compiled_predicate: None,
            mode: crate::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    let mut audit_log = AuditLog::new(100);
    let mut dlq = SyncDlq::new(DlqConfig::default());

    let data = serde_json::json!({"status": "draft"});
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 42,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let response =
        session.handle_delta_push(&msg, Some(&rls_store), Some(&mut audit_log), Some(&mut dlq));

    assert!(response.is_none());
    assert_eq!(session.mutations_silent_dropped, 1);
    assert_eq!(session.mutations_processed, 0);
    assert_eq!(audit_log.len(), 1);
    assert_eq!(dlq.total_entries(), 1);
    let entries = dlq.entries_for_collection(1, "orders");
    assert_eq!(entries[0].mutation_id, 42);
}

#[test]
fn delta_push_rate_limited_silent_drop() {
    let rate_config = RateLimitConfig {
        rate_per_sec: 0.0,
        burst: 1,
    };
    let mut session = SyncSession::with_rate_limit("rate-test".into(), &rate_config);
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("bob".into());
    session.identity = Some(AuthenticatedIdentity {
        user_id: 2,
        username: "bob".into(),
        tenant_id: TenantId::new(1),
        auth_method: crate::control::security::identity::AuthMethod::ApiKey,
        roles: vec![crate::control::security::identity::Role::ReadWrite],
        is_superuser: false,
    });

    let data = serde_json::json!({"key": "value"});
    let msg = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let r1 = session.handle_delta_push(&msg, None, None, None);
    assert!(r1.is_some());
    assert_eq!(session.mutations_processed, 1);

    let mut audit_log = AuditLog::new(100);
    let mut dlq = SyncDlq::new(DlqConfig::default());

    let msg2 = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d2".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 2,
        checksum: 0,
        device_valid_time_ms: None,
    };
    let r2 = session.handle_delta_push(&msg2, None, Some(&mut audit_log), Some(&mut dlq));
    assert!(r2.is_none());
    assert_eq!(session.mutations_silent_dropped, 1);
    assert_eq!(dlq.total_entries(), 1);
}

#[test]
fn ping_pong() {
    let mut session = make_session();

    let ping = PingPongMsg {
        timestamp_ms: 99999,
        is_pong: false,
    };
    let response = session.handle_ping(&ping).expect("ping response");
    let pong: PingPongMsg = response.decode_body().unwrap();
    assert!(pong.is_pong);
    assert_eq!(pong.timestamp_ms, 99999);
}

#[test]
fn vector_clock_sync() {
    let mut session = make_session();
    session.authenticated = true;

    let mut clocks = HashMap::new();
    clocks.insert("orders".into(), 42u64);

    let msg = VectorClockSyncMsg {
        clocks,
        sender_id: 5,
    };
    let response = session
        .handle_vector_clock_sync(&msg)
        .expect("clock sync response");
    let sync: VectorClockSyncMsg = response.decode_body().unwrap();
    assert_eq!(*sync.clocks.get("orders").unwrap(), 42);
}

#[test]
fn replay_dedup_skips_already_processed() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"key": "value"});
    let delta = nodedb_types::json_to_msgpack(&data).unwrap();

    let msg = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: delta.clone(),
        peer_id: 42,
        mutation_id: 5,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let r1 = session.handle_delta_push(&msg, None, None, None);
    assert!(r1.is_some());
    assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 1);

    let r2 = session.handle_delta_push(&msg, None, None, None);
    assert!(r2.is_some());
    assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 1);

    let msg_old = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d0".into(),
        delta: delta.clone(),
        peer_id: 42,
        mutation_id: 3,
        checksum: 0,
        device_valid_time_ms: None,
    };
    let r3 = session.handle_delta_push(&msg_old, None, None, None);
    assert!(r3.is_some());
    assert_eq!(r3.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 1);

    let msg_new = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d2".into(),
        delta,
        peer_id: 42,
        mutation_id: 6,
        checksum: 0,
        device_valid_time_ms: None,
    };
    let r4 = session.handle_delta_push(&msg_new, None, None, None);
    assert!(r4.is_some());
    assert_eq!(r4.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 2);
}

#[test]
fn crc32c_mismatch_rejects_delta() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"key": "value"});
    let delta = nodedb_types::json_to_msgpack(&data).unwrap();

    let valid_checksum = crc32c::crc32c(&delta);
    let msg_ok = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: delta.clone(),
        peer_id: 1,
        mutation_id: 1,
        checksum: valid_checksum,
        device_valid_time_ms: None,
    };
    let r1 = session.handle_delta_push(&msg_ok, None, None, None);
    assert!(r1.is_some());
    assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);

    let msg_bad = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d2".into(),
        delta,
        peer_id: 1,
        mutation_id: 2,
        checksum: valid_checksum ^ 0xDEAD,
        device_valid_time_ms: None,
    };
    let r2 = session.handle_delta_push(&msg_bad, None, None, None);
    assert!(r2.is_some());
    assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaReject);
    assert_eq!(session.mutations_rejected, 1);
}
