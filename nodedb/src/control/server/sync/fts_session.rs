// SPDX-License-Identifier: BUSL-1.1

//! Session-level FTS index/delete handlers.
//!
//! Contains `SyncSession::handle_fts_index` and `SyncSession::handle_fts_delete`.

use tracing::{debug, error};

use nodedb_types::sync::wire::AckStatus;

use super::fts_handler::FtsDispatcher;
use super::session::SyncSession;
use super::wire::*;
use crate::types::TenantId;

impl SyncSession {
    /// Process a `FtsIndexMsg`: allocate surrogate, WAL-append on CP, dispatch
    /// to Data Plane through the idempotency gate, return an ACK frame.
    pub async fn handle_fts_index<D: FtsDispatcher>(
        &mut self,
        msg: &FtsIndexMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            let ack = FtsIndexAckMsg {
                collection: msg.collection.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some("unauthenticated".to_string()),
                applied_seq: 0,
                status: AckStatus::Rejected {
                    reason: "unauthenticated".to_string(),
                },
            };
            return SyncFrame::try_encode(SyncMessageType::FtsIndexAck, &ack);
        }

        // Empty fields dispatch like any other text: indexing no text removes
        // the document's prior postings from every index.
        let surrogate = match dispatcher
            .assign_surrogate(
                self.database_id(),
                self.tenant_id.unwrap_or(TenantId::new(0)),
                &msg.collection,
                &msg.doc_id,
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "fts sync: surrogate assignment failed"
                );
                let ack = FtsIndexAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate assignment failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate assignment failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::FtsIndexAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            doc_id = %msg.doc_id,
            batch_id = msg.batch_id,
            lite_id = %msg.lite_id,
            "fts index: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_index(
                tenant_id,
                vshard,
                msg.collection.clone(),
                surrogate,
                msg.fields.clone(),
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Fts,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                self.mutations_processed += 1;
                let wire = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "fts index",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                let ack = FtsIndexAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsIndexAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "fts index dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = FtsIndexAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsIndexAck, &ack)
            }
        }
    }

    /// Process a `FtsDeleteMsg`: look up surrogate, WAL-append on CP, dispatch
    /// tombstone through the idempotency gate, return an ACK frame.
    pub async fn handle_fts_delete<D: FtsDispatcher>(
        &mut self,
        msg: &FtsDeleteMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            let ack = FtsDeleteAckMsg {
                collection: msg.collection.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some("unauthenticated".to_string()),
                applied_seq: 0,
                status: AckStatus::Rejected {
                    reason: "unauthenticated".to_string(),
                },
            };
            return SyncFrame::try_encode(SyncMessageType::FtsDeleteAck, &ack);
        }

        // A delete only reads the key's binding at the collection's home. A key
        // the home never bound names no row: the delete dispatches `None`,
        // which removes nothing and still advances the producer's sequence
        // through the idempotency gate.
        let surrogate = match dispatcher
            .lookup_surrogate(
                self.database_id(),
                self.tenant_id.unwrap_or(TenantId::new(0)),
                &msg.collection,
                &msg.doc_id,
            )
            .await
        {
            Ok(found) => found,
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "fts sync: surrogate lookup failed for delete"
                );
                let ack = FtsDeleteAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate lookup failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate lookup failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::FtsDeleteAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            doc_id = %msg.doc_id,
            batch_id = msg.batch_id,
            lite_id = %msg.lite_id,
            "fts delete: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_delete(
                tenant_id,
                vshard,
                msg.collection.clone(),
                surrogate,
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Fts,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                self.mutations_processed += 1;
                let wire = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "fts delete",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                let ack = FtsDeleteAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsDeleteAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "fts delete dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = FtsDeleteAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsDeleteAck, &ack)
            }
        }
    }
}
