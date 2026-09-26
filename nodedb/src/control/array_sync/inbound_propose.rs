// SPDX-License-Identifier: BUSL-1.1

//! Raft propose / single-node dispatch helpers for [`OriginArrayInbound`].
//!
//! These methods handle the multi-node Raft path (`propose_and_await`) and
//! the single-node fallback (`apply_op_direct`), plus the small helpers for
//! computing the destination vShard and converting an `ArrayOp` into a
//! Data Plane plan.

use std::time::Duration;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_cluster::array_routing::{array_vshard_for_name, vshard_for_array_coord};
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};
use tracing::{error, warn};

use crate::control::wal_replication::ReplicatedEntry;
use crate::types::{TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::inbound::{InboundOutcome, OriginArrayInbound};
use super::reject::build_reject;

impl OriginArrayInbound {
    fn consume_array_write_authorization(
        &self,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
        array: &str,
        hlc: Hlc,
    ) -> Result<(), Option<ArrayRejectMsg>> {
        let (tenant_id, database_id, collection, permission) = authorization.into_scope();
        if tenant_id != self.tenant_id()
            || database_id != self.database_id()
            || collection != array
            || permission != crate::control::security::identity::Permission::Write
        {
            return Err(Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                "array authorization scope mismatch".to_string(),
            )));
        }
        Ok(())
    }

    /// Propose a `ReplicatedEntry` to Raft and await its commit + apply.
    ///
    /// Returns `Ok(())` when the entry has been committed and executed by the
    /// distributed applier. Returns a reject on proposal or timeout failure.
    pub(super) async fn propose_and_await(
        &self,
        entry: ReplicatedEntry,
        array: &str,
        hlc: Hlc,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
    ) -> Result<(), Option<ArrayRejectMsg>> {
        self.consume_array_write_authorization(authorization, array, hlc)?;
        if entry.tenant_id != self.tenant_id().as_u64()
            || entry.database_id != self.database_id().as_u64()
        {
            return Err(Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                "array replicated entry scope mismatch".to_string(),
            )));
        }
        let vshard_id = entry.vshard_id;
        let idempotency_key = entry.idempotency_key;
        let data = entry.to_bytes();

        // Use the async proposer (with transparent leader forwarding + apply
        // wait) when available. It returns the apply payload directly.
        if let Some(async_proposer) = self.shared().async_raft_proposer().map(|a| a.as_ref()) {
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_secs(
                    self.shared().tuning.network.default_deadline_secs,
                );
            return match async_proposer(vshard_id, idempotency_key, data, deadline).await {
                Ok((_payload, _committed_version)) => Ok(()),
                Err(e) => {
                    warn!(array = %array, error = %e, "array_inbound: raft propose+apply failed");
                    Err(Some(build_reject(
                        array,
                        hlc,
                        ArrayRejectReason::EngineRejected,
                        format!("raft propose failed: {e}"),
                    )))
                }
            };
        }

        // Sync proposer fallback: register a tracker waiter after proposing.
        // Used only in single-node mode where there is no forwarding race.
        let tracker = match self.shared().propose_tracker.get().map(|a| a.as_ref()) {
            Some(t) => t,
            None => {
                return Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    "raft proposer not available".to_string(),
                )));
            }
        };

        let proposer = match self.shared().raft_proposer.get().map(|a| a.as_ref()) {
            Some(p) => p,
            None => {
                return Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    "raft proposer not available".to_string(),
                )));
            }
        };

        let (group_id, log_index) = match proposer(vshard_id, data) {
            Ok(pair) => pair,
            Err(e) => {
                warn!(array = %array, error = %e, "array_inbound: raft propose failed");
                return Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("raft propose failed: {e}"),
                )));
            }
        };

        let rx = tracker.register(group_id, log_index, idempotency_key);
        let timeout_secs = self.shared().tuning.network.default_deadline_secs;

        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(Ok(_payload))) => Ok(()),
            Ok(Ok(Err(e))) => {
                warn!(array = %array, error = %e, "array_inbound: raft commit apply error");
                Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("apply error: {e}"),
                )))
            }
            Ok(Err(_)) => Err(Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                "propose waiter channel closed".to_string(),
            ))),
            Err(_) => {
                warn!(array = %array, "array_inbound: raft commit timeout");
                Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("raft commit timeout (group={group_id} index={log_index})"),
                )))
            }
        }
    }

    /// Single-node fallback: dispatch directly to the Data Plane, bypassing
    /// Raft. Used when `raft_proposer` is absent (development / unit tests).
    pub(super) async fn apply_op_direct(
        &self,
        op: ArrayOp,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        self.consume_array_write_authorization(authorization, &op.header.array, op.header.hlc)?;
        let data_plane_op = self.op_to_data_plane_plan(&op, provenance)?;
        let vshard = self.vshard_for_op(&op);
        let task = PhysicalTask {
            tenant_id: self.tenant_id(),
            database_id: self.database_id(),
            vshard_id: vshard,
            plan: data_plane_op,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tenant_id = task.tenant_id;
        let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &self.shared().audit,
        ));
        let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: self.shared(),
                task,
                identity: self.identity(),
                tenant_id,
                permissions: &self.shared().permissions,
                roles: &self.shared().roles,
                emitter: &emitter,
            },
        )
        .await
        .map_err(|error| {
            Some(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::EngineRejected,
                error.to_string(),
            ))
        })? {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
                checked
            }
            // An array write is never a clone-write shape.
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_) => {
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::EngineRejected,
                    "unexpected clone-write interception of an array task".to_string(),
                )));
            }
        };

        // This is the only durability this op will ever get: nothing appended a
        // redo for it upstream (no Raft entry on this path), and the reply below
        // tells the peer the op is applied — after which it never re-sends. The
        // write must therefore own its record: the funnel appends it under the
        // write-admission guard, stamps the minted LSN into the plan so the tile
        // version matches what replay will stamp from the record header, and
        // holds the ack behind the durable-at-ack barrier.
        let response =
            match crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write_with_source(
                self.shared(),
                checked,
                TraceId::ZERO,
                crate::event::EventSource::CrdtSync,
            )
            .await
            {
                Ok(r) => r,
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
            };

        // Surface fenced / error responses as rejects so the caller sends
        // ArrayRejectMsg rather than a silent success.
        if response.status == crate::bridge::envelope::Status::Error {
            return Err(Some(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::EngineRejected,
                "Data Plane rejected (fenced or error)".to_string(),
            )));
        }

        if let Err(e) = self.engine().record_applied_in_database(
            self.database_id(),
            self.tenant_id().as_u64(),
            &op,
        ) {
            error!(
                array = %op.header.array,
                hlc = ?op.header.hlc,
                error = %e,
                "array_inbound: op applied but op-log append failed (replay may re-apply)"
            );
        }

        if let Some(observer) = self.apply_observer() {
            observer.on_op_applied(&op);
        }

        Ok(InboundOutcome::Applied)
    }

    /// Compute the vShard that owns this op's tile.
    ///
    /// Extracts tile extents from the schema registry and casts the op's coord
    /// to `u64` for tile routing. Falls back to collection-level routing with
    /// a warning when the schema is unavailable or coord cannot be cast.
    pub(super) fn vshard_for_op(&self, op: &ArrayOp) -> VShardId {
        use nodedb_array::types::coord::value::CoordValue;

        let tile_extents = self.schemas().tile_extents_in_database(
            self.database_id(),
            self.tenant_id().as_u64(),
            &op.header.array,
        );

        let Some(tile_extents) = tile_extents else {
            warn!(
                array = %op.header.array,
                "array_inbound: schema unavailable; routing by name only"
            );
            return VShardId::new(array_vshard_for_name(&op.header.array));
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
    ///
    /// `provenance` is `Some` on the single-node sync path; `None` for snapshot
    /// replay and any non-sync caller. The value is threaded into
    /// `DataArrayOp::Put/Delete.provenance` so the Data Plane can run epoch
    /// fencing and advance the HWM.
    pub(super) fn op_to_data_plane_plan(
        &self,
        op: &ArrayOp,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    ) -> Result<crate::bridge::envelope::PhysicalPlan, Option<ArrayRejectMsg>> {
        use nodedb_array::sync::op::ArrayOpKind;
        use nodedb_physical::physical_plan::ArrayOp as DataArrayOp;

        let array_id = nodedb_array::types::ArrayId::in_database(
            self.tenant_id(),
            self.database_id(),
            &op.header.array,
        );

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
                    provenance,
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
                    provenance,
                }
            }
        };

        Ok(crate::bridge::envelope::PhysicalPlan::Array(data_op))
    }
}
