// SPDX-License-Identifier: BUSL-1.1

//! Session-level vector insert/delete handlers.
//!
//! Contains the `SyncSession::handle_vector_insert` and
//! `SyncSession::handle_vector_delete` methods.

use tracing::{debug, error};

use nodedb_types::sync::wire::AckStatus;

use super::session::SyncSession;
use super::vector_handler::{VectorDispatcher, VectorInsertParams};
use super::wire::*;
use crate::types::TenantId;

impl SyncSession {
    /// Process a `VectorInsertMsg`: allocate surrogate, dispatch to Data Plane,
    /// return an ACK frame.
    ///
    /// Unauthenticated sessions receive a fenced ACK without dispatch.
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
                reject_reason: Some(reason),
                applied_seq: 0,
                status: AckStatus::Fenced,
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
                return None;
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
                let wire = vector_ack_from_payload(&payload_bytes)?;
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
                let wire = vector_ack_from_error(&e)?;
                let ack = VectorInsertAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
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
                reject_reason: Some(reason),
                applied_seq: 0,
                status: AckStatus::Fenced,
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
                return None;
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
                let wire = vector_ack_from_payload(&payload_bytes)?;
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
                let wire = vector_ack_from_error(&e)?;
                let ack = VectorDeleteAckMsg {
                    collection: msg.collection.clone(),
                    id: msg.id.clone(),
                    batch_id: msg.batch_id,
                    accepted: false,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::VectorDeleteAck, &ack)
            }
        }
    }
}

/// Only a decoded core verdict names a durable stream frontier.
fn vector_ack_from_payload(payload: &[u8]) -> Option<super::ack_decode::EngineAckWire> {
    let result: nodedb_types::sync::wire::SyncAckResult = zerompk::from_msgpack(payload).ok()?;
    Some(super::ack_decode::EngineAck::from_result(result).into_wire())
}

/// Unknown outcomes close the connection so the same sequence is retried.
fn vector_ack_from_error(error: &crate::Error) -> Option<super::ack_decode::EngineAckWire> {
    use crate::bridge::envelope::ErrorCode;
    let crate::Error::DataPlane(code) = error else {
        return None;
    };
    let ack = match code {
        ErrorCode::SyncRejected {
            violation,
            applied_seq,
            ..
        } => super::ack_decode::EngineAck::Rejected {
            violation: violation.clone(),
            applied_seq: *applied_seq,
        },
        ErrorCode::SyncNotApplied { hold, applied_seq } => super::ack_decode::EngineAck::Status {
            status: hold.ack_status(),
            applied_seq: *applied_seq,
        },
        _ => return None,
    };
    Some(ack.into_wire())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::sync::violation::ViolationType;
    use nodedb_types::sync::wire::{SyncAckResult, SyncProvenance};

    #[test]
    fn an_unreadable_vector_outcome_closes_without_claiming_a_frontier() {
        assert!(vector_ack_from_payload(b"invalid msgpack").is_none());
        assert!(
            vector_ack_from_error(&crate::Error::Internal {
                detail: "storage unavailable".into()
            })
            .is_none()
        );
    }

    #[test]
    fn vector_terminal_payload_preserves_the_consumed_sequence() {
        let bytes =
            zerompk::to_msgpack_vec(&SyncAckResult::rejected(ViolationType::PermissionDenied, 7))
                .unwrap();
        let ack = vector_ack_from_payload(&bytes).unwrap();
        assert_eq!(ack.applied_seq, 7);
        assert!(!ack.accepted);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    }

    #[test]
    fn a_typed_gate_rejection_preserves_its_actual_frontier() {
        let error = crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::SyncRejected {
            violation: ViolationType::PermissionDenied,
            applied_seq: 7,
            provenance: SyncProvenance {
                producer_id: 2,
                epoch: 1,
                stream_id: 3,
                seq: 7,
            },
        });
        let ack = vector_ack_from_error(&error).unwrap();
        assert_eq!(ack.applied_seq, 7);
        assert!(!ack.accepted);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    }

    #[test]
    fn a_typed_gap_preserves_the_actual_expected_sequence() {
        let error = crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::SyncNotApplied {
            hold: crate::bridge::envelope::SyncHold::Gap { expected: 5 },
            applied_seq: 4,
        });
        let ack = vector_ack_from_error(&error).unwrap();
        assert_eq!(ack.applied_seq, 4);
        assert_eq!(ack.status, AckStatus::Gap { expected: 5 });
    }
}
