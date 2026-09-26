// SPDX-License-Identifier: BUSL-1.1

//! Session-level KV push handler: `SyncSession::handle_kv_push`.
//!
//! A put's `{key, value…}` row decodes to the stored body through
//! `kv_row_to_body_fields` and `row_to_kv_body`, the inverse of the row every
//! KV read returns. A put whose absolute expiry already passed applies as a
//! delete: the entry is gone on the Lite that wrote it.
//!
//! Every outcome is one `KvPushAckMsg`:
//! - a gate verdict (`Applied`, `Duplicate`, `Fenced`, `Gap`) passes
//!   through;
//! - a dispatch that never got a verdict is a retryable `Gap` at the frame's
//!   own sequence;
//! - a terminal refusal is `Rejected`. A refusal decided before the Data
//!   Plane still moves the stream mark past the frame, so the producer's
//!   next frame is not a gap.

use tracing::{debug, error};

use nodedb_query::msgpack_scan::{kv_row_to_body_fields, row_to_kv_body};
use nodedb_types::Value;
use nodedb_types::sync::wire::{
    AckStatus, EngineKind, KvPushAckMsg, KvPushMsg, KvPushOp, SyncFrame, SyncMessageType,
    SyncProvenance, stream_id_for,
};

use super::kv_handler::{KvPushDispatcher, KvPushWrite, KvPushWriteOp};
use super::session::SyncSession;
use crate::bridge::envelope::ErrorCode;
use crate::types::TenantId;

/// What the session tells the sender about one push.
enum KvPushOutcome {
    /// A status at a stream mark.
    Status { status: AckStatus, applied_seq: u64 },
    /// A terminal refusal. `mark_moved` says whether the Data Plane already
    /// moved the stream mark past the frame.
    Refused { reason: String, mark_moved: bool },
}

impl SyncSession {
    /// Process a `KvPushMsg` and return its `KvPushAckMsg` frame.
    pub async fn handle_kv_push<D: KvPushDispatcher>(
        &mut self,
        msg: &KvPushMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            return kv_ack(
                msg,
                KvPushOutcome::Refused {
                    reason: "unauthenticated".to_string(),
                    mark_moved: false,
                },
            );
        }

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let provenance = SyncProvenance {
            producer_id: self.producer_id,
            epoch: self.accepted_epoch,
            stream_id: stream_id_for(EngineKind::Kv, &msg.collection),
            seq: msg.seq,
        };
        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            batch_id = msg.batch_id,
            seq = msg.seq,
            lite_id = %msg.lite_id,
            "kv push: dispatching"
        );

        let outcome = match push_write(msg, provenance.clone()) {
            Ok(write) => match dispatcher.apply(tenant_id, write).await {
                Ok(payload) => {
                    let wire = super::ack_decode::decode_sync_ack(
                        &payload,
                        "kv push",
                        &self.session_id,
                        &msg.collection,
                        msg.seq,
                    )
                    .into_wire();
                    match wire.status {
                        AckStatus::Rejected { reason } => KvPushOutcome::Refused {
                            reason,
                            mark_moved: true,
                        },
                        status => KvPushOutcome::Status {
                            status,
                            applied_seq: wire.applied_seq,
                        },
                    }
                }
                Err(e) => dispatch_outcome(&e, msg.seq),
            },
            Err(reason) => KvPushOutcome::Refused {
                reason,
                mark_moved: false,
            },
        };

        let outcome = match outcome {
            KvPushOutcome::Refused {
                reason,
                mark_moved: false,
            } => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    batch_id = msg.batch_id,
                    seq = msg.seq,
                    %reason,
                    "kv push refused before the Data Plane"
                );
                let mark_moved = match dispatcher
                    .skip(tenant_id, &msg.collection, provenance)
                    .await
                {
                    Ok(()) => true,
                    Err(e) => {
                        error!(
                            session = %self.session_id,
                            collection = %msg.collection,
                            seq = msg.seq,
                            error = %e,
                            "kv push: the stream mark could not move past a refused frame; \
                             the producer's next frame reports a gap"
                        );
                        false
                    }
                };
                KvPushOutcome::Refused { reason, mark_moved }
            }
            other => other,
        };

        match &outcome {
            KvPushOutcome::Status {
                status: AckStatus::Applied,
                ..
            } => self.mutations_applied += 1,
            KvPushOutcome::Status {
                status: AckStatus::Duplicate,
                ..
            } => self.mutations_deduplicated += 1,
            KvPushOutcome::Status { .. } => self.mutations_not_applied += 1,
            KvPushOutcome::Refused { .. } => self.mutations_rejected += 1,
        }
        self.mutations_processed += 1;
        kv_ack(msg, outcome)
    }
}

/// The write a push applies, or why Origin refuses it on its content.
fn push_write(msg: &KvPushMsg, provenance: SyncProvenance) -> Result<KvPushWrite, String> {
    let op = match &msg.op {
        KvPushOp::Delete => KvPushWriteOp::Delete,
        KvPushOp::Put { row, expire_at_ms } => {
            let now_ms = crate::engine::kv::current_ms();
            if *expire_at_ms != 0 && *expire_at_ms <= now_ms {
                KvPushWriteOp::Delete
            } else {
                let key = String::from_utf8_lossy(&msg.key);
                let (fields, shape) = kv_row_to_body_fields(&key, row)
                    .map_err(|e| format!("KV push row does not decode: {e}"))?;
                let body = row_to_kv_body(&Value::Object(fields), shape)
                    .map_err(|e| format!("KV push row does not encode as a body: {e}"))?;
                let ttl_ms = if *expire_at_ms == 0 {
                    0
                } else {
                    expire_at_ms - now_ms
                };
                KvPushWriteOp::Put { body, ttl_ms }
            }
        }
    };
    Ok(KvPushWrite {
        collection: msg.collection.clone(),
        key: msg.key.clone(),
        op,
        provenance,
    })
}

/// The outcome of a dispatch that returned an error.
fn dispatch_outcome(error: &crate::Error, seq: u64) -> KvPushOutcome {
    match error {
        crate::Error::DataPlane(ErrorCode::SyncNotApplied { hold, applied_seq }) => {
            KvPushOutcome::Status {
                status: hold.ack_status(),
                applied_seq: *applied_seq,
            }
        }
        crate::Error::DataPlane(ErrorCode::SyncRejected { violation, .. }) => {
            KvPushOutcome::Refused {
                reason: violation.to_string(),
                mark_moved: true,
            }
        }
        other => match super::refusal::ack_status_for_dispatch_error(other, seq) {
            AckStatus::Rejected { reason } => KvPushOutcome::Refused {
                reason,
                mark_moved: false,
            },
            status => KvPushOutcome::Status {
                status,
                applied_seq: seq.saturating_sub(1),
            },
        },
    }
}

/// Encode the ack frame for `outcome`.
fn kv_ack(msg: &KvPushMsg, outcome: KvPushOutcome) -> Option<SyncFrame> {
    let (status, applied_seq) = match outcome {
        KvPushOutcome::Status {
            status,
            applied_seq,
        } => (status, applied_seq),
        KvPushOutcome::Refused { reason, mark_moved } => (
            AckStatus::Rejected { reason },
            if mark_moved {
                msg.seq
            } else {
                msg.seq.saturating_sub(1)
            },
        ),
    };
    let ack = KvPushAckMsg {
        collection: msg.collection.clone(),
        key: msg.key.clone(),
        batch_id: msg.batch_id,
        accepted: !matches!(status, AckStatus::Rejected { .. }),
        reject_reason: super::refusal::reject_reason_for(&status),
        applied_seq,
        status,
    };
    SyncFrame::try_encode(SyncMessageType::KvPushAck, &ack)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use nodedb_query::msgpack_scan::kv_row_msgpack;

    use super::*;
    use crate::bridge::envelope::SyncHold;

    /// Records every dispatch and answers with a fixed result.
    struct MockDispatcher {
        applied: Arc<Mutex<Vec<KvPushWrite>>>,
        skipped: Arc<Mutex<Vec<u64>>>,
        answer: fn(&KvPushWrite) -> crate::Result<Vec<u8>>,
    }

    impl MockDispatcher {
        fn answering(answer: fn(&KvPushWrite) -> crate::Result<Vec<u8>>) -> Self {
            Self {
                applied: Arc::new(Mutex::new(Vec::new())),
                skipped: Arc::new(Mutex::new(Vec::new())),
                answer,
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
        ) -> crate::Result<()> {
            self.skipped.lock().expect("skipped").push(provenance.seq);
            Ok(())
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
        let msg = put_msg("k1", kv_row_msgpack("k1", b"v1"), 5);

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
        let msg = put_msg("k1", kv_row_msgpack("k1", b"v1"), 5);

        let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

        assert_eq!(ack.status, AckStatus::Duplicate);
        assert!(ack.accepted);
        assert_eq!(session.mutations_deduplicated, 1);
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
        let mut msg = put_msg("k1", kv_row_msgpack("k1", b"v1"), 7);
        msg.op = KvPushOp::Put {
            row: kv_row_msgpack("k1", b"v1"),
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
    async fn an_unauthenticated_push_is_rejected_without_dispatch() {
        let mut session = SyncSession::new("kv-push".to_string());
        let mock = MockDispatcher::answering(applied_at);
        let msg = put_msg("k1", kv_row_msgpack("k1", b"v1"), 1);

        let ack = ack_of(session.handle_kv_push(&msg, &mock).await);

        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
        assert!(mock.applied.lock().expect("applied").is_empty());
        assert!(mock.skipped.lock().expect("skipped").is_empty());
    }
}
