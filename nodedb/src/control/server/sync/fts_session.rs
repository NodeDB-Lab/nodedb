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
    /// Process a `FtsIndexMsg`: allocate its surrogate, dispatch through the
    /// idempotency gate, and return a checked ACK frame.
    pub async fn handle_fts_index<D: FtsDispatcher>(
        &mut self,
        msg: &FtsIndexMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = FtsIndexAckMsg {
                collection: msg.collection.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason),
                applied_seq: 0,
                status: AckStatus::Fenced,
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
                return None;
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
                let wire = super::ack_decode::decode_core_ack(&payload_bytes)?;
                self.mutations_processed += 1;
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
                let wire = super::ack_decode::decode_core_error(&e)?;
                let ack = FtsIndexAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsIndexAck, &ack)
            }
        }
    }

    /// Process a `FtsDeleteMsg`: look up its surrogate, dispatch through the
    /// idempotency gate, and return a checked ACK frame.
    pub async fn handle_fts_delete<D: FtsDispatcher>(
        &mut self,
        msg: &FtsDeleteMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = FtsDeleteAckMsg {
                collection: msg.collection.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason),
                applied_seq: 0,
                status: AckStatus::Fenced,
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
                return None;
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
                let wire = super::ack_decode::decode_core_ack(&payload_bytes)?;
                self.mutations_processed += 1;
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
                let wire = super::ack_decode::decode_core_error(&e)?;
                let ack = FtsDeleteAckMsg {
                    collection: msg.collection.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::FtsDeleteAck, &ack)
            }
        }
    }
}
