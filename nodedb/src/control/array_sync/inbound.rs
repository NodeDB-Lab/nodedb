//! [`OriginArrayInbound`] — dispatcher for inbound array CRDT wire messages.
//!
//! Receives decoded wire messages from the WebSocket listener, validates
//! them (schema gating, idempotency), and proposes ops through Raft for
//! durable, ordered replication to all Origin replicas.
//!
//! # Write flow
//!
//! 1. Decode op payload via `nodedb_array::sync::op_codec::decode_op`.
//! 2. Fast-path idempotency check (before proposing, to avoid wasting Raft
//!    proposals for already-seen ops; this mirrors the Document handler pattern).
//! 3. Schema-gate: reject if the array is unknown or the op's schema HLC is
//!    ahead of the local registry.
//! 4. Build `ReplicatedWrite::ArrayOp` and serialize to a `ReplicatedEntry`.
//! 5. `raft_proposer(vshard_id, bytes)` — propose to the Raft group that
//!    owns the destination vShard.
//! 6. Await Raft commit via `ProposeTracker`.
//! 7. On commit: `distributed_applier` decodes the entry, dispatches it to
//!    the Data Plane, calls `record_applied`.
//!
//! # Schema sync
//!
//! `handle_schema` builds `ReplicatedWrite::ArraySchema` and follows the same
//! propose → commit flow. After commit the `distributed_applier` calls
//! `OriginSchemaRegistry::import_snapshot`.
//!
//! # Non-Raft paths
//!
//! Acks and catchup requests are advisory / read-only and never touch Raft.
//! Snapshot chunks are buffered until a full snapshot arrives, then applied
//! as a batch of ArrayOp proposals (one per contained op).
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
use tracing::warn;

use nodedb_cluster::array_routing::vshard_from_collection;

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
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

    pub(super) fn shared(&self) -> &Arc<SharedState> {
        &self.shared
    }

    pub(super) fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub(super) fn engine(&self) -> &Arc<OriginApplyEngine> {
        &self.engine
    }

    pub(super) fn schemas(&self) -> &Arc<OriginSchemaRegistry> {
        &self.schemas
    }

    pub(super) fn apply_observer(&self) -> Option<&Arc<dyn ArrayApplyObserver>> {
        self.apply_observer.as_ref()
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

        self.apply_op(op, &msg.op_payload).await
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
                Ok(op) => self.apply_op(op, payload).await,
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
    ///
    /// Proposes the schema through Raft so it is applied atomically on all
    /// replicas. Returns `SchemaImported` on successful commit.
    pub async fn handle_schema(
        &self,
        msg: &ArraySchemaSyncMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let hlc_arr: [u8; 18] = msg.schema_hlc_bytes;
        let remote_hlc = Hlc::from_bytes(&hlc_arr);

        // In single-node mode (no raft_proposer) fall back to direct import.
        if self.shared.raft_proposer.get().is_none() {
            if let Err(e) =
                self.schemas
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
            return Ok(InboundOutcome::SchemaImported);
        }

        let vshard_id = VShardId::new(vshard_from_collection(&msg.array));
        let write = ReplicatedWrite::ArraySchema {
            array: msg.array.clone(),
            snapshot_payload: msg.snapshot_payload.clone(),
            schema_hlc_bytes: hlc_arr,
        };
        let entry = ReplicatedEntry {
            tenant_id: self.tenant_id.as_u32(),
            vshard_id: vshard_id.as_u16(),
            write,
        };

        match self.propose_and_await(entry, &msg.array, remote_hlc).await {
            Ok(()) => Ok(InboundOutcome::SchemaImported),
            Err(Some(r)) => Err(Some(r)),
            Err(None) => Err(None),
        }
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

    /// Validate a decoded op, then route it through Raft (or directly to the
    /// Data Plane in single-node mode) before returning.
    ///
    /// # Fast-path idempotency
    ///
    /// The idempotency check here is a *pre-proposal fast-path*: it avoids
    /// wasting a Raft round-trip for ops the local replica already knows
    /// about. The authoritative check happens in the distributed applier
    /// (after Raft commit) so replicas that missed the original entry still
    /// accept it on re-delivery.
    pub(super) async fn apply_op(
        &self,
        op: ArrayOp,
        raw_op_bytes: &[u8],
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

        // 3. Fast-path idempotency check (before proposing).
        if self.engine.already_seen(&op.header.array, op.header.hlc) {
            return Ok(InboundOutcome::Idempotent);
        }

        // 4. In single-node mode (no raft_proposer): apply directly to the
        //    Data Plane, matching the pre-Raft behaviour. This path is only
        //    exercised when the cluster stack has not been started (development,
        //    single-node Origin, unit tests without a raft setup).
        if self.shared.raft_proposer.get().is_none() {
            return self.apply_op_direct(op).await;
        }

        // 5. Multi-node path: propose through Raft.
        let hlc_bytes = op.header.hlc.to_bytes();
        let write = ReplicatedWrite::ArrayOp {
            array: op.header.array.clone(),
            op_bytes: raw_op_bytes.to_vec(),
            schema_hlc_bytes: hlc_bytes,
        };
        let vshard = self.vshard_for_op(&op);
        let entry = ReplicatedEntry {
            tenant_id: self.tenant_id.as_u32(),
            vshard_id: vshard.as_u16(),
            write,
        };

        match self
            .propose_and_await(entry, &op.header.array, op.header.hlc)
            .await
        {
            Ok(()) => {
                // Notify outbound fan-out observer so subscribed Lite peers
                // receive this op. The observer enqueues; no I/O here.
                if let Some(observer) = &self.apply_observer {
                    observer.on_op_applied(&op);
                }
                Ok(InboundOutcome::Applied)
            }
            Err(e) => Err(e),
        }
    }

    // Raft propose / direct-dispatch helpers live in `inbound_propose.rs`
    // to keep this file under the size limit.
}
