// SPDX-License-Identifier: BUSL-1.1

//! Session-level KV push handler: `SyncSession::handle_kv_push`.
//!
//! A put's `{key, value…}` row decodes to the stored body through
//! `kv_row_to_body_fields` and `row_to_kv_body`, the inverse of the row every
//! KV read returns. A put whose absolute expiry already passed applies as a
//! delete: the entry is gone on the Lite that wrote it.
//!
//! Typed core verdicts carry the durable frontier. Unknown outcomes close the session.
//! Invalid content retires only after its sequence consumption completes durably.

use tracing::{debug, error};

use nodedb_query::msgpack_scan::{kv_row_to_body_fields, row_to_kv_body};
use nodedb_types::Value;
use nodedb_types::sync::wire::{
    AckStatus, EngineKind, KvPushAckMsg, KvPushMsg, KvPushOp, SyncFrame, SyncMessageType,
    SyncProvenance, stream_id_for,
};

use super::kv_handler::{KvPushDispatcher, KvPushWrite, KvPushWriteOp};
use super::session::SyncSession;
use crate::types::TenantId;

impl SyncSession {
    /// Apply a push and preserve the Data Plane's durable frontier.
    pub async fn handle_kv_push<D: KvPushDispatcher>(
        &mut self,
        msg: &KvPushMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();
        if let Some(reason) = self.entry_refusal(&msg.collection) {
            debug!(%reason, "kv session admission fenced");
            return kv_ack(
                msg,
                super::ack_decode::EngineAckWire {
                    accepted: false,
                    reject_reason: Some(reason),
                    applied_seq: 0,
                    status: AckStatus::Fenced,
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
        let wire = match push_write(msg, provenance.clone()) {
            Ok(write) => match dispatcher.apply(tenant_id, write).await {
                Ok(payload) => super::ack_decode::decode_core_ack(&payload)?,
                Err(error) => super::ack_decode::decode_core_error(&error)?,
            },
            Err(error) => {
                let reason = error.to_string();
                error!(collection = %msg.collection, seq = msg.seq, %reason,
                    "kv content refused; dispatching durable sequence consumption");
                let mut wire = match dispatcher
                    .skip(tenant_id, &msg.collection, provenance)
                    .await
                {
                    Ok(payload) => super::ack_decode::decode_core_ack(&payload)?,
                    Err(error) => super::ack_decode::decode_core_error(&error)?,
                };
                if matches!(wire.status, AckStatus::Applied | AckStatus::Duplicate)
                    && wire.applied_seq >= msg.seq
                {
                    wire.accepted = false;
                    wire.reject_reason = Some(reason.clone());
                    wire.status = AckStatus::Rejected { reason };
                }
                wire
            }
        };
        match &wire.status {
            AckStatus::Applied => self.mutations_applied += 1,
            AckStatus::Duplicate => self.mutations_deduplicated += 1,
            AckStatus::Rejected { .. } => self.mutations_rejected += 1,
            _ => self.mutations_not_applied += 1,
        }
        self.mutations_processed += 1;
        kv_ack(msg, wire)
    }
}

/// Why Origin refuses a KV push on its content.
#[derive(Debug, thiserror::Error)]
pub enum KvPushRowError {
    #[error("KV push row does not decode: {0}")]
    RowUndecodable(nodedb_query::msgpack_scan::KvBodyError),
    #[error("KV push row does not encode as a body: {0}")]
    BodyUnencodable(nodedb_query::msgpack_scan::KvBodyError),
}

/// The write a push applies, or why Origin refuses it on its content.
fn push_write(msg: &KvPushMsg, provenance: SyncProvenance) -> Result<KvPushWrite, KvPushRowError> {
    let op = match &msg.op {
        KvPushOp::Delete => KvPushWriteOp::Delete,
        KvPushOp::Put { row, expire_at_ms } => {
            let now_ms = crate::engine::kv::current_ms();
            if *expire_at_ms != 0 && *expire_at_ms <= now_ms {
                KvPushWriteOp::Delete
            } else {
                let key = String::from_utf8_lossy(&msg.key);
                let (fields, shape) =
                    kv_row_to_body_fields(&key, row).map_err(KvPushRowError::RowUndecodable)?;
                let body = row_to_kv_body(&Value::Object(fields), shape)
                    .map_err(KvPushRowError::BodyUnencodable)?;
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

/// Encode only a checked durable verdict or a session fence.
fn kv_ack(msg: &KvPushMsg, wire: super::ack_decode::EngineAckWire) -> Option<SyncFrame> {
    let ack = KvPushAckMsg {
        collection: msg.collection.clone(),
        key: msg.key.clone(),
        batch_id: msg.batch_id,
        accepted: wire.accepted && matches!(wire.status, AckStatus::Applied | AckStatus::Duplicate),
        reject_reason: wire.reject_reason,
        applied_seq: wire.applied_seq,
        status: wire.status,
    };
    SyncFrame::try_encode(SyncMessageType::KvPushAck, &ack)
}

#[cfg(test)]
#[path = "kv_session_tests.rs"]
mod tests;
