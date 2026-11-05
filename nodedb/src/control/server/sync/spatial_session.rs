// SPDX-License-Identifier: BUSL-1.1

//! Session-level spatial insert/delete handlers.
//!
//! Contains `SyncSession::handle_spatial_insert` and
//! `SyncSession::handle_spatial_delete`.

use tracing::{debug, error};

use nodedb_types::geometry::SridGeometry;
use nodedb_types::sync::wire::AckStatus;

use super::session::SyncSession;
use super::spatial_handler::{SpatialDispatcher, SpatialInsertTarget};
use super::wire::*;
use crate::types::TenantId;

impl SyncSession {
    /// Process a `SpatialInsertMsg`: deserialise geometry, allocate surrogate,
    /// WAL-append on CP, dispatch to Data Plane through the idempotency gate,
    /// return an ACK frame.
    pub async fn handle_spatial_insert<D: SpatialDispatcher>(
        &mut self,
        msg: &SpatialInsertMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = SpatialInsertAckMsg {
                collection: msg.collection.clone(),
                field: msg.field.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason.clone()),
                applied_seq: 0,
                status: AckStatus::Rejected { reason },
            };
            return SyncFrame::try_encode(SyncMessageType::SpatialInsertAck, &ack);
        }

        // Deserialise the geometry and its SRID from MessagePack bytes.
        let geometry: SridGeometry = match zerompk::from_msgpack(&msg.geometry_bytes) {
            Ok(g) => g,
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    field = %msg.field,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "spatial sync: geometry deserialisation failed"
                );
                let ack = SpatialInsertAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("geometry deserialise failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("geometry deserialise failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::SpatialInsertAck, &ack);
            }
        };

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
                    "spatial sync: surrogate assignment failed"
                );
                let ack = SpatialInsertAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate assignment failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate assignment failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::SpatialInsertAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            field = %msg.field,
            doc_id = %msg.doc_id,
            batch_id = msg.batch_id,
            lite_id = %msg.lite_id,
            "spatial insert: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_insert(
                tenant_id,
                vshard,
                SpatialInsertTarget {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    surrogate,
                    geometry,
                },
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Spatial,
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
                    "spatial insert",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                let ack = SpatialInsertAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::SpatialInsertAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    field = %msg.field,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "spatial insert dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = SpatialInsertAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::SpatialInsertAck, &ack)
            }
        }
    }

    /// Process a `SpatialDeleteMsg`: look up surrogate, WAL-append on CP,
    /// dispatch removal through the idempotency gate, return an ACK frame.
    pub async fn handle_spatial_delete<D: SpatialDispatcher>(
        &mut self,
        msg: &SpatialDeleteMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = SpatialDeleteAckMsg {
                collection: msg.collection.clone(),
                field: msg.field.clone(),
                doc_id: msg.doc_id.clone(),
                batch_id: msg.batch_id,
                accepted: false,
                reject_reason: Some(reason.clone()),
                applied_seq: 0,
                status: AckStatus::Rejected { reason },
            };
            return SyncFrame::try_encode(SyncMessageType::SpatialDeleteAck, &ack);
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
                    "spatial sync: surrogate lookup failed for delete"
                );
                let ack = SpatialDeleteAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: Some(format!("surrogate lookup failed: {e}")),
                    applied_seq: 0,
                    status: AckStatus::Rejected {
                        reason: format!("surrogate lookup failed: {e}"),
                    },
                };
                return SyncFrame::try_encode(SyncMessageType::SpatialDeleteAck, &ack);
            }
        };

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            field = %msg.field,
            doc_id = %msg.doc_id,
            batch_id = msg.batch_id,
            lite_id = %msg.lite_id,
            "spatial delete: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_delete(
                tenant_id,
                vshard,
                msg.collection.clone(),
                msg.field.clone(),
                surrogate,
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Spatial,
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
                    "spatial delete",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();
                let ack = SpatialDeleteAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: wire.accepted,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::SpatialDeleteAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    field = %msg.field,
                    doc_id = %msg.doc_id,
                    batch_id = msg.batch_id,
                    error = %e,
                    "spatial delete dispatch failed"
                );
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                let ack = SpatialDeleteAckMsg {
                    collection: msg.collection.clone(),
                    field: msg.field.clone(),
                    doc_id: msg.doc_id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: super::refusal::reject_reason_for(&status),
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::SpatialDeleteAck, &ack)
            }
        }
    }
}
