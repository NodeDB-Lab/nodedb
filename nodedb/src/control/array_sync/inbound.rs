//! [`OriginArrayInbound`] — dispatcher for inbound array CRDT wire messages.
//!
//! Receives decoded wire messages from the WebSocket listener, validates
//! them (schema gating, idempotency), dispatches cell-level ops to the
//! Data Plane via `PhysicalPlan::Array`, and buffers snapshot chunks until
//! a full snapshot is assembled.
//!
//! # Data Plane dispatch
//!
//! Cell-level writes follow the same pattern as Timeseries ingest:
//!
//! 1. Decode op payload via `nodedb_array::sync::op_codec`.
//! 2. Schema-gate and idempotency-check on the Control Plane.
//! 3. Build `PhysicalPlan::Array(ArrayOp::Put | Delete)` and call
//!    `dispatch_to_data_plane_with_source` with `EventSource::CrdtSync`.
//! 4. On success: record the op in the op-log via `OriginApplyEngine::record_applied`.
//!
//! # Raft
//!
//! The existing Document/Vector delta handlers on Origin also do not wire
//! Raft for inbound sync ops (they go directly to Data Plane). This handler
//! matches that pattern. Raft-backed durability for array sync ops is deferred
//! to Phase I (multi-shard).
//!
//! # Thread safety
//!
//! `OriginArrayInbound` is `Send + Sync`. The snapshot buffer uses a `Mutex`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::sync::apply::ApplyRejection;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::array::{
    ArrayAckMsg, ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg,
    ArrayRejectReason, ArraySchemaSyncMsg,
};
use tracing::{error, warn};

use nodedb_cluster::array_routing::{vshard_for_array_coord, vshard_from_collection};

use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

use super::apply::OriginApplyEngine;
use super::outbound::ArrayApplyObserver;
use super::reject::build_reject;
use super::schema_registry::OriginSchemaRegistry;
use super::snapshot_assembly::SnapshotAssembly;

// ─── Outcome ─────────────────────────────────────────────────────────────────

/// Outcome returned by each [`OriginArrayInbound`] handler.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundOutcome {
    /// The op was applied to Data Plane engine state.
    Applied,
    /// The op was already present; no state was changed (idempotent replay).
    Idempotent,
    /// The op was rejected; the caller should send `ArrayRejectMsg` back.
    Rejected(ApplyRejection),
    /// A snapshot chunk was buffered; more chunks are expected.
    SnapshotPartial { received: u32, total: u32 },
    /// A snapshot was fully assembled and all contained ops applied.
    SnapshotApplied { ops_applied: u64 },
    /// A schema CRDT snapshot was imported into the local registry.
    SchemaImported,
    /// An ack was recorded into the ack-vector (GC frontier tracking).
    AckRecorded,
    /// A catchup request was received and logged (serving deferred to Phase H).
    CatchupRequested,
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

/// Dispatcher for inbound array CRDT wire messages from Lite peers.
///
/// Constructed once per sync session (or shared across sessions via `Arc`)
/// and called from the WebSocket listener arm for each array message type.
pub struct OriginArrayInbound {
    engine: Arc<OriginApplyEngine>,
    schemas: Arc<OriginSchemaRegistry>,
    shared: Arc<SharedState>,
    tenant_id: TenantId,
    /// Post-apply observer for fan-out to subscribed Lite peers.
    ///
    /// `None` in configurations where no Lite subscribers are expected
    /// (e.g. pure cluster-to-cluster sync without Lite edges).
    apply_observer: Option<Arc<dyn ArrayApplyObserver>>,
    /// In-flight snapshot chunk buffers keyed by `(array, snapshot_hlc_bytes)`.
    snapshots: Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>>,
}

impl OriginArrayInbound {
    /// Accessor for the snapshot assembly buffer used by the
    /// `snapshot_assembly` sibling module.
    pub(super) fn snapshots(&self) -> &Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>> {
        &self.snapshots
    }
}

impl OriginArrayInbound {
    /// Construct from shared server state and session tenant.
    pub fn new(
        engine: Arc<OriginApplyEngine>,
        schemas: Arc<OriginSchemaRegistry>,
        shared: Arc<SharedState>,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            engine,
            schemas,
            shared,
            tenant_id,
            apply_observer: None,
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    /// Attach a post-apply observer (used by `ArrayFanout` for Lite fan-out).
    pub fn with_observer(mut self, observer: Arc<dyn ArrayApplyObserver>) -> Self {
        self.apply_observer = Some(observer);
        self
    }

    // ─── Delta ───────────────────────────────────────────────────────────────

    /// Handle a single delta message from a Lite peer.
    pub async fn handle_delta(
        &self,
        msg: &ArrayDeltaMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let op = match op_codec::decode_op(&msg.op_payload) {
            Ok(op) => op,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: delta decode failed");
                return Err(Some(build_reject(
                    &msg.array,
                    Hlc::ZERO,
                    ArrayRejectReason::ShapeInvalid,
                    format!("decode error: {e}"),
                )));
            }
        };

        self.apply_op(op).await
    }

    /// Handle a batch of delta messages from a Lite peer.
    ///
    /// Returns one outcome per op. If decoding fails for an op, that op
    /// yields a reject; subsequent ops are still attempted.
    pub async fn handle_delta_batch(
        &self,
        msg: &ArrayDeltaBatchMsg,
    ) -> Vec<Result<InboundOutcome, Option<ArrayRejectMsg>>> {
        let mut outcomes = Vec::with_capacity(msg.op_payloads.len());
        for payload in &msg.op_payloads {
            let outcome = match op_codec::decode_op(payload) {
                Ok(op) => self.apply_op(op).await,
                Err(e) => {
                    warn!(array = %msg.array, error = %e, "array_inbound: batch decode failed");
                    Err(Some(build_reject(
                        &msg.array,
                        Hlc::ZERO,
                        ArrayRejectReason::ShapeInvalid,
                        format!("batch decode error: {e}"),
                    )))
                }
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    // ─── Schema ──────────────────────────────────────────────────────────────

    /// Import an array schema CRDT snapshot from a Lite peer.
    pub fn handle_schema(
        &self,
        msg: &ArraySchemaSyncMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let hlc_arr: [u8; 18] = msg.schema_hlc_bytes;
        let remote_hlc = Hlc::from_bytes(&hlc_arr);

        if let Err(e) = self
            .schemas
            .import_snapshot(&msg.array, &msg.snapshot_payload, remote_hlc)
        {
            warn!(array = %msg.array, error = %e, "array_inbound: schema import failed");
            return Err(Some(build_reject(
                &msg.array,
                remote_hlc,
                ArrayRejectReason::EngineRejected,
                format!("schema import error: {e}"),
            )));
        }

        Ok(InboundOutcome::SchemaImported)
    }

    // ─── Ack ─────────────────────────────────────────────────────────────────

    /// Record a peer ack for GC frontier tracking.
    ///
    /// Forwards the ack into the `ArrayAckRegistry` on `SharedState` so the
    /// GC task can compute the min-ack frontier for each array.
    pub fn handle_ack(&self, msg: &ArrayAckMsg) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let ack_hlc = Hlc::from_bytes(&msg.ack_hlc_bytes);
        let replica_id = nodedb_array::sync::replica_id::ReplicaId::new(msg.replica_id);
        self.shared
            .array_ack_registry
            .record(&msg.array, replica_id, ack_hlc);
        tracing::debug!(
            array = %msg.array,
            replica_id = msg.replica_id,
            ack_hlc = ?ack_hlc,
            "array_inbound: peer ack recorded"
        );
        Ok(InboundOutcome::AckRecorded)
    }

    // ─── Catchup request ─────────────────────────────────────────────────────

    /// Handle a catch-up request from a Lite peer.
    ///
    /// Delegates to [`OriginCatchupServer`] which validates the array, selects
    /// the op-stream or snapshot delivery path, and enqueues outbound frames.
    pub fn handle_catchup_request(
        &self,
        msg: &ArrayCatchupRequestMsg,
        session_id: &str,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        use super::catchup::OriginCatchupServer;

        let server = OriginCatchupServer::new(
            Arc::clone(&self.shared.array_sync_op_log),
            Arc::clone(&self.schemas),
            Arc::clone(&self.shared.array_snapshot_store),
            Arc::clone(&self.shared.array_delivery),
            Arc::clone(&self.shared.array_subscriber_cursors),
            Arc::clone(&self.shared.array_ack_registry),
        );

        if let Err(e) = server.serve(msg, session_id) {
            warn!(
                session = %session_id,
                array = %msg.array,
                error = %e,
                "array_inbound: catchup server error"
            );
        }

        Ok(InboundOutcome::CatchupRequested)
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Validate and dispatch a single decoded op to the Data Plane.
    pub(super) async fn apply_op(
        &self,
        op: ArrayOp,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        // 1. Shape validation.
        if let Err(e) = op.validate_shape() {
            return Err(Some(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ShapeInvalid,
                format!("shape validation: {e}"),
            )));
        }

        // 2. Schema HLC gating.
        match self.engine.schema_hlc(&op.header.array) {
            None => {
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::ArrayUnknown,
                    format!("array '{}' not known to this replica", op.header.array),
                )));
            }
            Some(local_schema) if op.header.schema_hlc > local_schema => {
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::SchemaTooNew,
                    format!(
                        "op schema_hlc {:?} > local {:?}; request schema sync",
                        op.header.schema_hlc, local_schema
                    ),
                )));
            }
            Some(_) => {}
        }

        // 3. Idempotency check.
        if self.engine.already_seen(&op.header.array, op.header.hlc) {
            return Ok(InboundOutcome::Idempotent);
        }

        // 4. Build Data Plane plan and dispatch.
        let data_plane_op = self.op_to_data_plane_plan(&op)?;
        let vshard = self.vshard_for_op(&op);

        let dispatch_result =
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                &self.shared,
                self.tenant_id,
                vshard,
                data_plane_op,
                0,
                crate::event::EventSource::CrdtSync,
            )
            .await;

        match dispatch_result {
            Ok(_) => {}
            Err(e) => {
                warn!(
                    array = %op.header.array,
                    error = %e,
                    "array_inbound: Data Plane dispatch failed"
                );
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("dispatch error: {e}"),
                )));
            }
        }

        // 5. Record in op-log so future replays are idempotent.
        if let Err(e) = self.engine.record_applied(&op) {
            // Non-fatal: the Data Plane has already applied the op. Log the
            // op-log failure and continue — the worst outcome is a duplicate
            // apply on the next replay, which the Data Plane handles
            // idempotently via its own seen-HLC check.
            error!(
                array = %op.header.array,
                hlc = ?op.header.hlc,
                error = %e,
                "array_inbound: op applied but op-log append failed (replay may re-apply)"
            );
        }

        // 6. Notify outbound fan-out observer so subscribed Lite peers receive
        //    this op. This runs on the Control Plane (Tokio async task) —
        //    the observer's `on_op_applied` is synchronous and fast (enqueue
        //    only; no I/O).
        if let Some(observer) = &self.apply_observer {
            observer.on_op_applied(&op);
        }

        Ok(InboundOutcome::Applied)
    }

    /// Compute the vShard that owns this op's tile.
    ///
    /// Extracts tile extents from the schema registry and casts the op's coord
    /// to `u64` for tile routing. Falls back to collection-level routing with
    /// a warning when the schema is unavailable or coord cannot be cast.
    fn vshard_for_op(&self, op: &ArrayOp) -> VShardId {
        use nodedb_array::types::coord::value::CoordValue;

        let tile_extents = self.schemas.tile_extents(&op.header.array);

        let Some(tile_extents) = tile_extents else {
            warn!(
                array = %op.header.array,
                "array_inbound: schema unavailable; routing by name only"
            );
            return VShardId::new(vshard_from_collection(&op.header.array));
        };

        let coord_u64: Vec<u64> = op
            .coord
            .iter()
            .map(|c| match c {
                CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v as u64,
                CoordValue::Float64(v) => v.to_bits(),
                CoordValue::String(_) => 0,
            })
            .collect();

        VShardId::new(vshard_for_array_coord(
            &op.header.array,
            &coord_u64,
            &tile_extents,
        ))
    }

    /// Convert a decoded `ArrayOp` (from sync) into a `PhysicalPlan::Array` variant.
    fn op_to_data_plane_plan(
        &self,
        op: &ArrayOp,
    ) -> Result<crate::bridge::envelope::PhysicalPlan, Option<ArrayRejectMsg>> {
        use crate::bridge::physical_plan::ArrayOp as DataArrayOp;
        use nodedb_array::sync::op::ArrayOpKind;
        use nodedb_types::TenantId as NdTenantId;

        let array_id = nodedb_array::types::ArrayId::new(
            NdTenantId::new(self.tenant_id.as_u32()),
            &op.header.array,
        );

        // Encode the coord and attrs as the Data Plane's msgpack payloads.
        let data_op = match op.kind {
            ArrayOpKind::Put => {
                let cells = vec![crate::engine::array::wal::ArrayPutCell {
                    coord: op.coord.clone(),
                    attrs: op.attrs.clone().unwrap_or_default(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    system_from_ms: op.header.system_from_ms,
                    valid_from_ms: op.header.valid_from_ms,
                    valid_until_ms: op.header.valid_until_ms,
                }];
                let cells_msgpack = zerompk::to_msgpack_vec(&cells).map_err(|e| {
                    Some(build_reject(
                        &op.header.array,
                        op.header.hlc,
                        ArrayRejectReason::ShapeInvalid,
                        format!("cells encode: {e}"),
                    ))
                })?;
                DataArrayOp::Put {
                    array_id,
                    cells_msgpack,
                    wal_lsn: 0,
                }
            }
            ArrayOpKind::Delete | ArrayOpKind::Erase => {
                let coords = vec![op.coord.clone()];
                let coords_msgpack = zerompk::to_msgpack_vec(&coords).map_err(|e| {
                    Some(build_reject(
                        &op.header.array,
                        op.header.hlc,
                        ArrayRejectReason::ShapeInvalid,
                        format!("coords encode: {e}"),
                    ))
                })?;
                DataArrayOp::Delete {
                    array_id,
                    coords_msgpack,
                    wal_lsn: 0,
                }
            }
        };

        Ok(crate::bridge::envelope::PhysicalPlan::Array(data_op))
    }
}
