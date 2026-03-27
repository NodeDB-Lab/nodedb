//! Async Data Plane dispatch helpers for the sync WebSocket listener.
//!
//! Contains async functions that cross the Control Plane / Data Plane boundary
//! via the SPSC bridge: shape-subscription snapshot queries and CRDT delta
//! constraint validation.

use std::time::Duration;

use tracing::{info, warn};

use crate::control::state::SharedState;

use super::wire::{CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType};

/// Handle ShapeSubscribe with real WAL LSN and Data Plane snapshot.
pub(super) async fn handle_shape_subscribe_async(
    shared: &SharedState,
    session: &super::session::SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::DocumentOp;
    use crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async;
    use crate::types::TenantId;

    let msg: super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
    let tenant_id = session.tenant_id.map(|t| t.as_u32()).unwrap_or(0);

    // Get current WAL LSN — this is the watermark for the snapshot.
    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    // Dispatch a query to the Data Plane to get matching data for this shape.
    let snapshot_data = match &msg.shape.shape_type {
        nodedb_types::sync::shape::ShapeType::Document { collection, .. } => {
            // Query the Data Plane for all documents in this collection.
            let plan = PhysicalPlan::Document(DocumentOp::RangeScan {
                collection: collection.clone(),
                field: String::new(), // Empty = full collection scan.
                lower: None,
                upper: None,
                limit: 10_000, // Cap for safety.
            });
            match dispatch_async(
                shared,
                TenantId::new(tenant_id),
                collection,
                plan,
                Duration::from_secs(10),
            )
            .await
            {
                Ok(payload) => super::shape::handler::ShapeSnapshotData {
                    data: payload,
                    doc_count: 1, // Approximate — actual count in payload.
                },
                Err(e) => {
                    tracing::warn!(
                        shape_id = %msg.shape.shape_id,
                        error = %e,
                        "shape snapshot query failed, sending empty snapshot"
                    );
                    super::shape::handler::ShapeSnapshotData::empty()
                }
            }
        }
        nodedb_types::sync::shape::ShapeType::Vector { collection, .. } => {
            // For vector shapes, the snapshot is the collection metadata.
            // Full vector data is too large — Lite rebuilds from its own HNSW.
            super::shape::handler::ShapeSnapshotData {
                data: collection.as_bytes().to_vec(),
                doc_count: 0,
            }
        }
        nodedb_types::sync::shape::ShapeType::Graph { .. } => {
            // Graph shapes: snapshot is the subgraph from root nodes.
            // For now, return empty — full graph snapshot needs BFS dispatch.
            super::shape::handler::ShapeSnapshotData::empty()
        }
    };

    // Register the shape subscription.
    let registry = super::shape::registry::ShapeRegistry::new();
    let response = super::shape::handler::handle_subscribe(
        &session.session_id,
        tenant_id,
        &msg,
        &registry,
        current_lsn,
        |_shape, _lsn| snapshot_data,
    );

    info!(
        session = %session.session_id,
        shape_id = %msg.shape.shape_id,
        lsn = current_lsn,
        "shape subscribed with WAL LSN watermark"
    );

    Some(response)
}

/// Async constraint validation for a delta before sending DeltaAck.
///
/// Dispatches the delta to the Data Plane's CRDT engine for pre-validation
/// (UNIQUE, FK constraints). If validation fails, converts the DeltaAck
/// to a DeltaReject with a typed CompensationHint.
pub(super) async fn validate_delta_constraints(
    shared: &SharedState,
    delta_msg: &DeltaPushMsg,
    ack_frame: SyncFrame,
) -> SyncFrame {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::CrdtOp;
    use crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async;
    use crate::types::TenantId;

    // Dispatch a CrdtApply plan to the Data Plane. If the CRDT engine
    // rejects it (constraint violation), we get an error back.
    let tenant_id = TenantId::new(0); // Trust mode default tenant.
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: delta_msg.collection.clone(),
        document_id: delta_msg.document_id.clone(),
        delta: delta_msg.delta.clone(),
        peer_id: delta_msg.peer_id,
        mutation_id: delta_msg.mutation_id,
    });

    match dispatch_async(
        shared,
        tenant_id,
        &delta_msg.collection,
        plan,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(_payload) => {
            // Constraint check passed — send the original DeltaAck.
            ack_frame
        }
        Err(e) => {
            let error_detail = e.to_string();
            // Constraint check failed — convert to DeltaReject.
            warn!(
                collection = %delta_msg.collection,
                doc = %delta_msg.document_id,
                error = %error_detail,
                "sync: delta constraint violation"
            );

            let hint = if error_detail.contains("unique") || error_detail.contains("UNIQUE") {
                CompensationHint::UniqueViolation {
                    field: "unknown".into(),
                    conflicting_value: delta_msg.document_id.clone(),
                }
            } else if error_detail.contains("foreign") || error_detail.contains("FK") {
                CompensationHint::ForeignKeyMissing {
                    referenced_id: delta_msg.document_id.clone(),
                }
            } else {
                CompensationHint::Custom {
                    constraint: "constraint".into(),
                    detail: error_detail.clone(),
                }
            };

            let reject = DeltaRejectMsg {
                mutation_id: delta_msg.mutation_id,
                reason: error_detail,
                compensation: Some(hint),
            };
            SyncFrame::encode_or_empty(SyncMessageType::DeltaReject, &reject)
        }
    }
}
