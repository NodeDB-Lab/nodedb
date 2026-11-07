// SPDX-License-Identifier: BUSL-1.1

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
//! 5. The async Raft proposer proposes to the Raft group that owns the
//!    destination vShard, forwarding to its leader.
//! 6. Await the Raft commit and this node's apply.
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

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::sync::wire::array::{
    ArrayDeltaMsg, ArrayOpResultsMsg, ArrayRejectMsg, ArrayRejectReason, ArraySchemaSyncMsg,
};
use tracing::warn;

use nodedb_cluster::array_routing::array_vshard_for_name;

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId, VShardId};

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
    /// A snapshot chunk was buffered; more chunks are expected.
    SnapshotPartial { received: u32, total: u32 },
    /// A snapshot was fully assembled. Every contained op was attempted, and
    /// the message holds one result per op, in snapshot order.
    SnapshotApplied(ArrayOpResultsMsg),
    /// A schema CRDT snapshot was imported into the local registry.
    SchemaImported,
    /// An ack was recorded into the ack-vector (GC frontier tracking).
    AckRecorded,
    /// A catchup request was received and logged (serving is not part of this outcome).
    CatchupRequested,
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

/// What [`OriginArrayInbound::apply_op`] did with an op it did not refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpApplied {
    /// The op committed and applied.
    Applied,
    /// The op's HLC was already present. Nothing changed.
    Idempotent,
}

impl From<OpApplied> for InboundOutcome {
    fn from(applied: OpApplied) -> Self {
        match applied {
            OpApplied::Applied => Self::Applied,
            OpApplied::Idempotent => Self::Idempotent,
        }
    }
}

/// Dispatcher for inbound array CRDT wire messages from Lite peers.
///
/// Constructed once per sync session (or shared across sessions via `Arc`)
/// and called from the WebSocket listener arm for each array message type.
pub struct OriginArrayInbound {
    engine: Arc<OriginApplyEngine>,
    pub(super) schemas: Arc<OriginSchemaRegistry>,
    pub(super) shared: Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    identity: crate::control::security::identity::AuthenticatedIdentity,
    /// Post-apply observer for fan-out to subscribed Lite peers.
    ///
    /// `None` in configurations where no Lite subscribers are expected
    /// (e.g. pure cluster-to-cluster sync without Lite edges).
    apply_observer: Option<Arc<dyn ArrayApplyObserver>>,
    /// In-flight snapshot chunk buffers keyed by `(array, snapshot_hlc_bytes)`.
    snapshots: Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>>,
    /// Server-authoritative producer identity for this session, assigned at
    /// handshake and set via [`Self::set_session_identity`]. Inbound provenance
    /// is stamped from these — never from the wire message — so a client cannot
    /// spoof another producer's id or replay a fenced epoch. `0` marks an
    /// unfenced session whose handshake carried no Lite identity. The gate
    /// treats it as a no-op.
    session_producer_id: std::sync::atomic::AtomicU64,
    session_epoch: std::sync::atomic::AtomicU64,
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

    /// The tenant this inbound engine is bound to. `pub(crate)` so the sync
    /// session builder's guard test can assert the session tenant was threaded
    /// through (see `session_handler::array`), not only the sibling propose
    /// path.
    pub(crate) fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Database selected by the authenticated sync session. This is captured
    /// once at engine construction and is the sole database scope for every
    /// inbound array authorization, Raft entry, and Data-Plane task.
    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(super) fn authorize_array(
        &self,
        array: &str,
        hlc: Hlc,
        permission: crate::control::security::identity::Permission,
    ) -> Result<crate::control::server::shared::authorization::AuthorizedCollection, ArrayRejectMsg>
    {
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.shared.audit));
        crate::control::server::shared::authorization::authorize_collection_capability(
            &self.identity,
            self.database_id,
            array,
            permission,
            &self.shared.permissions,
            &self.shared.roles,
            &emitter,
        )
        .map_err(|error| {
            build_reject(
                array,
                hlc,
                ArrayRejectReason::PermissionDenied,
                error.resource().to_string(),
            )
        })
    }
}

impl OriginArrayInbound {
    /// Construct from shared server state and session tenant.
    pub fn new(
        engine: Arc<OriginApplyEngine>,
        schemas: Arc<OriginSchemaRegistry>,
        shared: Arc<SharedState>,
        identity: crate::control::security::identity::AuthenticatedIdentity,
    ) -> Self {
        let tenant_id = identity.tenant_id;
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self {
            engine,
            schemas,
            shared,
            tenant_id,
            database_id,
            identity,
            apply_observer: None,
            snapshots: Mutex::new(HashMap::new()),
            session_producer_id: std::sync::atomic::AtomicU64::new(0),
            session_epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Attach a post-apply observer (used by `ArrayFanout` for Lite fan-out).
    pub fn with_observer(mut self, observer: Arc<dyn ArrayApplyObserver>) -> Self {
        self.apply_observer = Some(observer);
        self
    }

    /// Bind this session's handshake-assigned producer identity. The session
    /// handler calls this before dispatching each array frame so inbound
    /// provenance is stamped server-authoritatively. `0` leaves the session
    /// unfenced: its handshake carried no Lite identity.
    pub fn set_session_identity(&self, producer_id: u64, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.session_producer_id
            .store(producer_id, Ordering::Relaxed);
        self.session_epoch.store(epoch, Ordering::Relaxed);
    }

    /// Build server-authoritative provenance for an op in `array` carrying the
    /// client-supplied `seq`. `producer_id`/`epoch` come from the session's
    /// handshake identity, NOT the wire message. Returns `None` for an
    /// unidentified producer (`session_producer_id == 0`) — the gate no-ops.
    fn session_provenance(&self, array: &str, seq: u64) -> Option<SyncProvenance> {
        use std::sync::atomic::Ordering;
        let producer_id = self.session_producer_id.load(Ordering::Relaxed);
        if producer_id == 0 {
            return None;
        }
        use nodedb_types::sync::wire::stream_id::{EngineKind, stream_id_for};
        Some(SyncProvenance {
            producer_id,
            epoch: self.session_epoch.load(Ordering::Relaxed),
            stream_id: stream_id_for(EngineKind::Array, array),
            seq,
        })
    }

    // ─── Delta ───────────────────────────────────────────────────────────────

    /// Handle a single delta message from a Lite peer.
    pub async fn handle_delta(
        &self,
        msg: &ArrayDeltaMsg,
    ) -> Result<InboundOutcome, ArrayRejectMsg> {
        let op = match op_codec::decode_op(&msg.op_payload) {
            Ok(op) => op,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: delta decode failed");
                return Err(build_reject(
                    &msg.array,
                    Hlc::ZERO,
                    ArrayRejectReason::ShapeInvalid,
                    format!("decode error: {e}"),
                ));
            }
        };

        let provenance = self.session_provenance(&msg.array, msg.seq);
        self.apply_op(op, &msg.op_payload, provenance)
            .await
            .map(InboundOutcome::from)
    }

    /// Server-authoritative provenance for one op of a delta batch.
    pub(super) fn batch_provenance(&self, array: &str) -> Option<SyncProvenance> {
        // A batch carries no per-op seq. Array dedups by HLC, so seq is
        // informational here. The epoch fence still applies.
        self.session_provenance(array, 0)
    }

    // ─── Schema ──────────────────────────────────────────────────────────────

    /// Import an array schema CRDT snapshot from a Lite peer.
    ///
    /// Proposes the schema through Raft so it is applied atomically on all
    /// replicas of its data group, then puts the array in the replicated
    /// catalog. Returns `SchemaImported` once both committed.
    pub async fn handle_schema(
        &self,
        msg: &ArraySchemaSyncMsg,
    ) -> Result<InboundOutcome, ArrayRejectMsg> {
        let hlc_arr: [u8; 18] = msg.schema_hlc_bytes;
        let remote_hlc = Hlc::from_bytes(&hlc_arr);
        let authorization = self.authorize_array(
            &msg.array,
            remote_hlc,
            crate::control::security::identity::Permission::Write,
        )?;

        let vshard_id = VShardId::new(array_vshard_for_name(&msg.array));
        let write = ReplicatedWrite::ArraySchema {
            array: msg.array.clone(),
            snapshot_payload: msg.snapshot_payload.clone(),
            schema_hlc_bytes: hlc_arr,
        };
        let entry = ReplicatedEntry::new(
            self.tenant_id.as_u64(),
            self.database_id.as_u64(),
            vshard_id.as_u32(),
            write,
        );

        self.propose_and_await(entry, &msg.array, remote_hlc, authorization)
            .await?;
        self.register_in_catalog(msg, remote_hlc).await?;
        Ok(InboundOutcome::SchemaImported)
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Validate a decoded op, then route it through Raft before returning.
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
        provenance: Option<SyncProvenance>,
    ) -> Result<OpApplied, ArrayRejectMsg> {
        // 1. Shape validation.
        if let Err(e) = op.validate_shape() {
            return Err(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ShapeInvalid,
                format!("shape validation: {e}"),
            ));
        }
        // The op's own bytes name its target, apart from the frame's name.
        if !nodedb_types::CollectionKey::is_bare_name(&op.header.array) {
            return Err(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ShapeInvalid,
                format!("op array name '{}' must be bare", op.header.array),
            ));
        }

        let authorization = self.authorize_array(
            &op.header.array,
            op.header.hlc,
            crate::control::security::identity::Permission::Write,
        )?;

        // 2. Schema HLC gating.
        match self.engine.schema_hlc_in_database(
            self.database_id,
            self.tenant_id.as_u64(),
            &op.header.array,
        ) {
            None => {
                return Err(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::ArrayUnknown,
                    format!("array '{}' not known to this replica", op.header.array),
                ));
            }
            Some(local_schema) if op.header.schema_hlc > local_schema => {
                return Err(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::SchemaTooNew,
                    format!(
                        "op schema_hlc {:?} > local {:?}; request schema sync",
                        op.header.schema_hlc, local_schema
                    ),
                ));
            }
            Some(_) => {}
        }

        // 2a. A put's attrs must match the schema before the op is proposed:
        //     a refusal after commit would refuse again on every replay.
        if let Some(attrs) = op.attrs.as_deref()
            && op.kind == nodedb_array::sync::op::ArrayOpKind::Put
        {
            let checked = self
                .engine
                .schemas
                .to_array_schema_in_database(
                    self.database_id,
                    self.tenant_id.as_u64(),
                    &op.header.array,
                )
                .map(|schema| schema.check_cell(attrs));
            match checked {
                Some(Ok(())) => {}
                Some(Err(e)) => {
                    return Err(build_reject(
                        &op.header.array,
                        op.header.hlc,
                        ArrayRejectReason::EngineRefused,
                        format!("schema check: {e}"),
                    ));
                }
                None => {
                    return Err(build_reject(
                        &op.header.array,
                        op.header.hlc,
                        ArrayRejectReason::ArrayUnknown,
                        format!("array '{}' has no decodable schema", op.header.array),
                    ));
                }
            }
        }

        // 3. Fast-path idempotency check (before proposing).
        if self.engine.already_seen_in_database(
            self.database_id,
            self.tenant_id.as_u64(),
            &op.header.array,
            op.header.hlc,
        ) {
            let _authorized_scope = authorization.into_scope();
            return Ok(OpApplied::Idempotent);
        }

        // 4. Propose through Raft. A put's cell surrogate is
        //    bound here, at the array's home, and every replica binds the same
        //    one when the entry applies.
        let cell_surrogate = self.cell_surrogate(&op).await?;
        let hlc_bytes = op.header.hlc.to_bytes();
        let write = ReplicatedWrite::ArrayOp {
            array: op.header.array.clone(),
            op_bytes: raw_op_bytes.to_vec(),
            cell_surrogate: cell_surrogate.map(nodedb_types::Surrogate::as_u32),
            schema_hlc_bytes: hlc_bytes,
            provenance: provenance
                .as_ref()
                .and_then(|p| zerompk::to_msgpack_vec(p).ok()),
            // Stamped with the floor, below.
            incarnation: nodedb_types::Hlc::ZERO,
        };
        let vshard = self.vshard_for_op(&op)?;
        let entry = ReplicatedEntry::new(
            self.tenant_id.as_u64(),
            self.database_id.as_u64(),
            vshard.as_u32(),
            write,
        );

        match self
            .propose_and_await(entry, &op.header.array, op.header.hlc, authorization)
            .await
        {
            Ok(()) => {
                // Notify outbound fan-out observer so subscribed Lite peers
                // receive this op. The observer enqueues; no I/O here.
                if let Some(observer) = &self.apply_observer {
                    observer.on_op_applied(&op);
                }
                Ok(OpApplied::Applied)
            }
            Err(e) => Err(e),
        }
    }

    // Raft propose / direct-dispatch helpers live in `inbound_propose.rs`
    // to keep this file under the size limit.
}
