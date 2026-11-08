// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex};

use super::*;

type MockCallLog = Arc<Mutex<Vec<(TenantId, String, Vec<Vec<Value>>)>>>;

struct MockDispatcher {
    calls: MockCallLog,
    result: crate::Result<Vec<u8>>,
}

impl MockDispatcher {
    fn ok(n: u64) -> (Self, MockCallLog) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        // Encode a SyncAckResult so the handler can decode it.
        let ack_result = nodedb_types::sync::wire::SyncAckResult::acked(AckStatus::Applied, n);
        let payload = zerompk::to_msgpack_vec(&ack_result).expect("encode SyncAckResult");
        (
            Self {
                calls: calls.clone(),
                result: Ok(payload),
            },
            calls,
        )
    }

    fn err() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            result: Err(crate::Error::Internal {
                detail: "mock failure".to_string(),
            }),
        }
    }
}

#[async_trait]
impl ColumnarDispatcher for MockDispatcher {
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        _vshard: VShardId,
        collection: String,
        rows: Vec<Vec<Value>>,
        _schema_bytes: Vec<u8>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .push((tenant_id, collection, rows));
        match &self.result {
            Ok(b) => Ok(b.clone()),
            Err(crate::Error::DataPlane(code)) => Err(crate::Error::DataPlane(code.clone())),
            Err(e) => Err(crate::Error::Internal {
                detail: e.to_string(),
            }),
        }
    }
}

fn make_session() -> SyncSession {
    SyncSession::new("test-columnar-session".to_string())
}

fn encode_row(values: Vec<Value>) -> Vec<u8> {
    zerompk::to_msgpack_vec(&values).expect("encode row")
}

// ── columnar_row_pk_bytes tests ──────────────────────────────────────────

fn obj(pairs: &[(&str, Value)]) -> Value {
    use std::collections::HashMap;
    let mut map = HashMap::new();
    for (k, v) in pairs {
        map.insert(k.to_string(), v.clone());
    }
    Value::Object(map)
}

#[test]
fn pk_bytes_id_wins_over_document_id_and_key() {
    let row = obj(&[
        ("id", Value::String("id-val".to_string())),
        ("document_id", Value::String("doc-val".to_string())),
        ("key", Value::String("key-val".to_string())),
    ]);
    assert_eq!(columnar_row_pk_bytes(&row), b"id-val");
}

#[test]
fn pk_bytes_document_id_wins_over_key() {
    let row = obj(&[
        ("document_id", Value::String("doc-val".to_string())),
        ("key", Value::String("key-val".to_string())),
    ]);
    assert_eq!(columnar_row_pk_bytes(&row), b"doc-val");
}

#[test]
fn pk_bytes_key_fallback() {
    let row = obj(&[("key", Value::String("key-val".to_string()))]);
    assert_eq!(columnar_row_pk_bytes(&row), b"key-val");
}

#[test]
fn pk_bytes_integer_renders_as_decimal() {
    let row = obj(&[("id", Value::Integer(5))]);
    assert_eq!(columnar_row_pk_bytes(&row), b"5");
}

#[test]
fn pk_bytes_headless_row_is_empty() {
    let row = obj(&[("reading", Value::Integer(1)), ("level", Value::Float(2.0))]);
    assert!(columnar_row_pk_bytes(&row).is_empty());
}

#[test]
fn pk_bytes_null_pk_is_empty() {
    let row = obj(&[("id", Value::Null)]);
    assert!(columnar_row_pk_bytes(&row).is_empty());
}

#[test]
fn pk_bytes_non_object_is_empty() {
    assert!(columnar_row_pk_bytes(&Value::Integer(42)).is_empty());
    assert!(columnar_row_pk_bytes(&Value::Null).is_empty());
}

#[test]
fn pk_bytes_deterministic() {
    let row = obj(&[("id", Value::Integer(99))]);
    assert_eq!(columnar_row_pk_bytes(&row), columnar_row_pk_bytes(&row));
}

fn make_insert_msg(collection: &str, rows: Vec<Vec<Value>>) -> ColumnarInsertMsg {
    ColumnarInsertMsg {
        lite_id: "lite-test".to_string(),
        collection: collection.to_string(),
        rows: rows.into_iter().map(encode_row).collect(),
        batch_id: 1,
        schema_bytes: Vec::new(),
        producer_id: 0,
        epoch: 0,
        seq: 0,
    }
}

#[tokio::test]
async fn unauthenticated_returns_fence() {
    let mut session = make_session();
    let (mock, calls) = MockDispatcher::ok(0);
    let msg = make_insert_msg(
        "metrics",
        vec![vec![Value::Integer(1), Value::Float(std::f64::consts::PI)]],
    );

    let frame = session.handle_columnar_insert(&msg, &mock).await;
    assert!(frame.is_some());
    let ack: ColumnarInsertAckMsg = frame.unwrap().decode_body().unwrap();
    assert_eq!(ack.accepted, 0);
    assert_eq!(ack.rejected, 1);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn authenticated_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    let (mock, calls) = MockDispatcher::ok(2);
    let msg = make_insert_msg(
        "metrics",
        vec![
            vec![Value::Integer(1), Value::Float(1.0)],
            vec![Value::Integer(2), Value::Float(2.0)],
        ],
    );

    let frame = session.handle_columnar_insert(&msg, &mock).await;
    assert!(frame.is_some());
    let ack: ColumnarInsertAckMsg = frame.unwrap().decode_body().unwrap();
    assert_eq!(ack.accepted, 2);
    assert_eq!(ack.rejected, 0);

    let log = calls.lock().unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].1, "metrics");
    assert_eq!(log[0].2.len(), 2);
}

#[tokio::test]
async fn unknown_dispatch_outcome_closes_without_ack() {
    let mut session = make_session();
    session.authenticated = true;
    let mock = MockDispatcher::err();
    let msg = make_insert_msg("metrics", vec![vec![Value::Integer(1), Value::Float(1.0)]]);

    let frame = session.handle_columnar_insert(&msg, &mock).await;
    assert!(frame.is_none());
}
#[tokio::test]
async fn malformed_rows_and_payloads_close_without_ack() {
    let mut session = make_session();
    session.authenticated = true;
    let (mut mock, calls) = MockDispatcher::ok(1);
    let mut msg = make_insert_msg("metrics", vec![vec![Value::Integer(1)]]);
    msg.rows[0] = vec![0xc1];
    assert!(session.handle_columnar_insert(&msg, &mock).await.is_none());
    assert!(calls.lock().expect("calls").is_empty());
    msg.rows[0] = encode_row(vec![Value::Integer(1)]);
    mock.result = Ok(Vec::new());
    assert!(session.handle_columnar_insert(&msg, &mock).await.is_none());
}

#[tokio::test]
async fn typed_refusal_and_hold_preserve_actual_frontiers() {
    use crate::bridge::envelope::{ErrorCode, SyncHold};
    let mut session = make_session();
    session.authenticated = true;
    let (mut mock, _) = MockDispatcher::ok(1);
    let mut msg = make_insert_msg("metrics", vec![vec![Value::Integer(1)]]);
    msg.seq = 5;
    mock.result = Err(crate::Error::DataPlane(ErrorCode::SyncRejected {
        violation: nodedb_types::sync::violation::ViolationType::PermissionDenied,
        applied_seq: 19,
        provenance: nodedb_types::sync::wire::SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 2,
            seq: 5,
        },
    }));
    let ack: ColumnarInsertAckMsg = session
        .handle_columnar_insert(&msg, &mock)
        .await
        .expect("typed ack")
        .decode_body()
        .expect("ack body");
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    assert_eq!(ack.applied_seq, 19);
    assert_eq!(ack.accepted, 0);
    mock.result = Err(crate::Error::DataPlane(ErrorCode::SyncNotApplied {
        hold: SyncHold::Gap { expected: 3 },
        applied_seq: 2,
    }));
    let ack: ColumnarInsertAckMsg = session
        .handle_columnar_insert(&msg, &mock)
        .await
        .expect("typed ack")
        .decode_body()
        .expect("ack body");
    assert_eq!(ack.status, AckStatus::Gap { expected: 3 });
    assert_eq!(ack.applied_seq, 2);
}
