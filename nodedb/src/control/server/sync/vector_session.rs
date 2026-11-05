// SPDX-License-Identifier: BUSL-1.1

//! Session-level vector insert/delete handlers.
//!
//! Contains the `SyncSession::handle_vector_insert` and
//! `SyncSession::handle_vector_delete` methods.

use tracing::{debug, error, warn};

use nodedb_types::sync::wire::AckStatus;

use super::session::SyncSession;
use super::vector_handler::{VectorDispatcher, VectorInsertParams};
use super::wire::*;
use crate::types::TenantId;

impl SyncSession {
    /// Process a `VectorInsertMsg`: allocate surrogate, dispatch to Data Plane,
    /// return an ACK frame.
    ///
    /// Unauthenticated sessions receive a rejection ACK without dispatch.
    pub async fn handle_vector_insert<D: VectorDispatcher>(
        &mut self,
        msg: &VectorInsertMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = VectorInsertAckMsg {
                collection: msg.collection.clone(),
                id: msg.id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason.clone()),
                applied_seq: 0,
                status: AckStatus::Rejected { reason },
            };
            return SyncFrame::try_encode(SyncMessageType::VectorInsertAck, &ack);
        }

        if msg.vector.len() != msg.dim || msg.dim == 0 {
            warn!(
                session = %self.session_id,
                collection = %msg.collection,
                id = %msg.id,
                batch_id = msg.batch_id,
                stated_dim = msg.dim,
                actual_len = msg.vector.len(),
                "vector sync: dimension mismatch; rejecting"
            );
            let ack = VectorInsertAckMsg {
                collection: msg.collection.clone(),
                id: msg.id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(format!(
                    "dimension mismatch: stated {}, actual {}",
                    msg.dim,
                    msg.vector.len()
                )),
                applied_seq: 0,
                status: AckStatus::Rejected {
                    reason: format!(
                        "dimension mismatch: stated {}, actual {}",
                        msg.dim,
                        msg.vector.len()
                    ),
                },
            };
            return SyncFrame::try_encode(SyncMessageType::VectorInsertAck, &ack);
        }

        let surrogate = match dispatcher
            .assign_surrogate(
                self.database_id(),
                self.tenant_id.unwrap_or(TenantId::new(0)),
                &msg.collection,
                &msg.id,
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    id = %msg.id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "vector sync: surrogate assignment failed"
                );
                let ack = VectorInsertAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate assignment failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate assignment failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::VectorInsertAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            id = %msg.id,
            batch_id = msg.batch_id,
            dim = msg.dim,
            lite_id = %msg.lite_id,
            "vector insert: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_insert(
                tenant_id,
                vshard,
                VectorInsertParams {
                    collection: msg.collection.clone(),
                    vector: msg.vector.clone(),
                    dim: msg.dim,
                    field_name: msg.field_name.clone(),
                    surrogate,
                    pk_bytes: Some(msg.id.as_bytes().to_vec()),
                },
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Vector,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                let wire = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "vector insert",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                self.mutations_processed += 1;
                let ack = VectorInsertAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::VectorInsertAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    id = %msg.id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "vector insert dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = VectorInsertAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::VectorInsertAck, &ack)
            }
        }
    }

    /// Process a `VectorDeleteMsg`: look up surrogate, dispatch tombstone to
    /// Data Plane, return an ACK frame.
    pub async fn handle_vector_delete<D: VectorDispatcher>(
        &mut self,
        msg: &VectorDeleteMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = VectorDeleteAckMsg {
                collection: msg.collection.clone(),
                id: msg.id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason.clone()),
                applied_seq: 0,
                status: AckStatus::Rejected { reason },
            };
            return SyncFrame::try_encode(SyncMessageType::VectorDeleteAck, &ack);
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
                &msg.id,
            )
            .await
        {
            Ok(found) => found,
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    id = %msg.id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "vector sync: surrogate lookup failed for delete"
                );
                let ack = VectorDeleteAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate lookup failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate lookup failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::VectorDeleteAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            id = %msg.id,
            batch_id = msg.batch_id,
            lite_id = %msg.lite_id,
            "vector delete: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_delete(
                tenant_id,
                vshard,
                msg.collection.clone(),
                surrogate,
                msg.field_name.clone(),
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Vector,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                let wire = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "vector delete",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                self.mutations_processed += 1;
                let ack = VectorDeleteAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::VectorDeleteAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    id = %msg.id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "vector delete dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = VectorDeleteAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::VectorDeleteAck, &ack)
            }
        }
    }
}
