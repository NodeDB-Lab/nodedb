// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nodedb_query::msgpack_scan::kv_row_msgpack;

use super::*;
use crate::bridge::envelope::{ErrorCode, SyncHold};

/// Records every dispatch and answers with a fixed result.
struct MockDispatcher {
    applied: Arc<Mutex<Vec<KvPushWrite>>>,
    skipped: Arc<Mutex<Vec<u64>>>,
    answer: fn(&KvPushWrite) -> crate::Result<Vec<u8>>,
    skip_answer: fn(SyncProvenance) -> crate::Result<Vec<u8>>,
}

impl MockDispatcher {
    fn answering(answer: fn(&KvPushWrite) -> crate::Result<Vec<u8>>) -> Self {
        Self {
            applied: Arc::new(Mutex::new(Vec::new())),
            skipped: Arc::new(Mutex::new(Vec::new())),
            answer,
            skip_answer: |provenance| {
                Ok(
                    zerompk::to_msgpack_vec(&nodedb_types::sync::wire::SyncAckResult::acked(
                        AckStatus::Applied,
                        provenance.seq,
                    ))
                    .expect("encode skip ack"),
                )
            },
        }
    }
}

#[async_trait]
impl KvPushDispatcher for MockDispatcher {
    async fn apply(&self, _tenant_id: TenantId, write: KvPushWrite) -> crate::Result<Vec<u8>> {
        let answer = (self.answer)(&write);
        self.applied.lock().expect("applied").push(write);
        answer
    }

    async fn skip(
        &self,
        _tenant_id: TenantId,
        _collection: &str,
        provenance: SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        self.skipped.lock().expect("skipped").push(provenance.seq);
        (self.skip_answer)(provenance)
    }
}

fn applied_at(write: &KvPushWrite) -> crate::Result<Vec<u8>> {
    Ok(
        zerompk::to_msgpack_vec(&nodedb_types::sync::wire::SyncAckResult::acked(
            AckStatus::Applied,
            write.provenance.seq,
        ))
        .expect("encode ack"),
    )
}

fn duplicate(_write: &KvPushWrite) -> crate::Result<Vec<u8>> {
    Err(crate::Error::DataPlane(ErrorCode::SyncNotApplied {
        hold: SyncHold::Duplicate,
        applied_seq: 5,
    }))
}

fn authenticated() -> SyncSession {
    let mut session = SyncSession::new("kv-push".to_string());
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    session.producer_id = 11;
    session.accepted_epoch = 1;
    session
}

fn put_msg(key: &str, row: Vec<u8>, seq: u64) -> KvPushMsg {
    KvPushMsg {
        lite_id: "lite".into(),
        collection: "cfg".into(),
        key: key.as_bytes().to_vec(),
        op: KvPushOp::Put {
            row,
            expire_at_ms: 0,
        },
        batch_id: seq,
        producer_id: 11,
        epoch: 1,
        seq,
    }
}

fn ack_of(frame: Option<SyncFrame>) -> KvPushAckMsg {
    frame
        .expect("ack frame")
        .decode_body()
        .expect("decode KvPushAck")
}

#[tokio::test]
async fn a_pushed_row_dispatches_its_stored_body_and_acks_applied() {
    let mut session = authenticated();
    let mock = MockDispatcher::answering(applied_at);
    let msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 5);

    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

    assert_eq!(ack.status, AckStatus::Applied);
    assert!(ack.accepted);
    assert_eq!(ack.applied_seq, 5);
    let applied = mock.applied.lock().expect("applied");
    assert_eq!(
        applied[0].op,
        KvPushWriteOp::Put {
            body: b"v1".to_vec(),
            ttl_ms: 0
        }
    );
    assert_eq!(
        applied[0].provenance.stream_id,
        stream_id_for(EngineKind::Kv, "cfg")
    );
}

#[tokio::test]
async fn a_resent_push_acks_duplicate() {
    let mut session = authenticated();
    let mock = MockDispatcher::answering(duplicate);
    let msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 5);

    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

    assert_eq!(ack.status, AckStatus::Duplicate);
    assert!(ack.accepted);
    assert_eq!(session.mutations_deduplicated, 1);
}

#[test]
fn a_row_that_does_not_decode_is_a_typed_error_that_reaches_the_central_enum() {
    let msg = put_msg("k1", b"v1".to_vec(), 6);
    let provenance = SyncProvenance {
        producer_id: 1,
        epoch: 1,
        stream_id: 0,
        seq: 6,
    };
    let Err(error) = push_write(&msg, provenance) else {
        panic!("a raw byte string is no row map");
    };
    assert!(matches!(error, KvPushRowError::RowUndecodable(_)));
    assert!(matches!(
        crate::Error::from(error),
        crate::Error::BadRequest { .. }
    ));
}

#[tokio::test]
async fn a_row_that_is_not_a_row_map_is_rejected_and_its_sequence_is_skipped() {
    let mut session = authenticated();
    let mock = MockDispatcher::answering(applied_at);
    let msg = put_msg("k1", b"v1".to_vec(), 6);

    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    assert!(!ack.accepted);
    assert!(mock.applied.lock().expect("applied").is_empty());
    assert_eq!(*mock.skipped.lock().expect("skipped"), vec![6]);
    assert_eq!(ack.applied_seq, 6);
}

#[tokio::test]
async fn an_expired_put_applies_as_a_delete() {
    let mut session = authenticated();
    let mock = MockDispatcher::answering(applied_at);
    let mut msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 7);
    msg.op = KvPushOp::Put {
        row: kv_row_msgpack("k1", b"v1").expect("row"),
        expire_at_ms: 1,
    };

    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

    assert_eq!(ack.status, AckStatus::Applied);
    assert_eq!(
        mock.applied.lock().expect("applied")[0].op,
        KvPushWriteOp::Delete
    );
}

#[tokio::test]
async fn an_unauthenticated_push_is_fenced_without_dispatch() {
    let mut session = SyncSession::new("kv-push".to_string());
    let mock = MockDispatcher::answering(applied_at);
    let msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 1);

    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

    assert_eq!(ack.status, AckStatus::Fenced);
    assert!(mock.applied.lock().expect("applied").is_empty());
    assert!(mock.skipped.lock().expect("skipped").is_empty());
}
#[tokio::test]
async fn typed_refusal_preserves_its_actual_frontier() {
    let mut session = authenticated();
    let mock = MockDispatcher::answering(|write| {
        Err(crate::Error::DataPlane(ErrorCode::SyncRejected {
            violation: nodedb_types::sync::violation::ViolationType::PermissionDenied,
            applied_seq: 19,
            provenance: write.provenance.clone(),
        }))
    });
    let msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 5);
    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    assert_eq!(ack.applied_seq, 19);
    assert!(!ack.accepted);
    assert!(mock.skipped.lock().expect("skipped").is_empty());
}

#[tokio::test]
async fn unknown_dispatch_and_malformed_payload_close_without_ack() {
    let mut session = authenticated();
    let msg = put_msg("k1", kv_row_msgpack("k1", b"v1").expect("row"), 5);
    let unknown = MockDispatcher::answering(|_| {
        Err(crate::Error::RetryableRefusal {
            reason: "timeout".into(),
        })
    });
    assert!(session.handle_kv_push(&msg, &unknown).await.is_none());
    let malformed = MockDispatcher::answering(|_| Ok(Vec::new()));
    assert!(session.handle_kv_push(&msg, &malformed).await.is_none());
}

#[tokio::test]
async fn invalid_content_retains_the_exact_nonconsuming_skip_verdict() {
    let mut session = authenticated();
    let mut mock = MockDispatcher::answering(applied_at);
    mock.skip_answer = |_| {
        Err(crate::Error::DataPlane(ErrorCode::SyncNotApplied {
            hold: SyncHold::Gap { expected: 2 },
            applied_seq: 1,
        }))
    };
    let msg = put_msg("k1", b"not a row".to_vec(), 5);
    let ack = ack_of(session.handle_kv_push(&msg, &mock).await);
    assert_eq!(ack.status, AckStatus::Gap { expected: 2 });
    assert_eq!(ack.applied_seq, 1);
    assert!(!ack.accepted);
}

#[tokio::test]
async fn unresolved_skip_never_returns_terminal_rejection() {
    let mut session = authenticated();
    let mut mock = MockDispatcher::answering(applied_at);
    let msg = put_msg("k1", b"not a row".to_vec(), 5);
    mock.skip_answer = |_| {
        Err(crate::Error::RetryableRefusal {
            reason: "timeout".into(),
        })
    };
    assert!(session.handle_kv_push(&msg, &mock).await.is_none());
    mock.skip_answer = |_| Ok(Vec::new());
    assert!(session.handle_kv_push(&msg, &mock).await.is_none());
}
