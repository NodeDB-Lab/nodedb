//! Raft propose / single-node dispatch helpers for [`OriginArrayInbound`].
//!
//! Split out of `inbound.rs` to keep that file under the 500-line limit.
//! These methods handle the multi-node Raft path (`propose_and_await`) and
//! the single-node fallback (`apply_op_direct`), plus the small helpers for
//! computing the destination vShard and converting an `ArrayOp` into a
//! Data Plane plan.

use std::time::Duration;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_cluster::array_routing::{vshard_for_array_coord, vshard_from_collection};
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};
use tracing::{error, warn};

use crate::control::wal_replication::ReplicatedEntry;
use crate::types::VShardId;

use super::inbound::{InboundOutcome, OriginArrayInbound};
use super::reject::build_reject;

impl OriginArrayInbound {
    /// Propose a `ReplicatedEntry` to Raft and await its commit + apply.
    ///
    /// Returns `Ok(())` when the entry has been committed and executed by the
    /// distributed applier. Returns a reject on proposal or timeout failure.
    pub(super) async fn propose_and_await(
        &self,
        entry: ReplicatedEntry,
        array: &str,
        hlc: Hlc,
    ) -> Result<(), Option<ArrayRejectMsg>> {
        let vshard_id = entry.vshard_id;
        let data = entry.to_bytes();

        // Use the async proposer (with transparent leader forwarding + apply
        // wait) when available. It returns the apply payload directly.
        if let Some(async_proposer) = self.shared().async_raft_proposer.get().map(|a| a.as_ref()) {
            return match async_proposer(vshard_id, data).await {
                Ok(_payload) => Ok(()),
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

        let rx = tracker.register(group_id, log_index);
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
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let data_plane_op = self.op_to_data_plane_plan(&op)?;
        let vshard = self.vshard_for_op(&op);

        let dispatch_result =
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                self.shared(),
                self.tenant_id(),
                vshard,
                data_plane_op,
                0,
                crate::event::EventSource::CrdtSync,
            )
            .await;

        if let Err(e) = dispatch_result {
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

        if let Err(e) = self.engine().record_applied(&op) {
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

        let tile_extents = self.schemas().tile_extents(&op.header.array);

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
    pub(super) fn op_to_data_plane_plan(
        &self,
        op: &ArrayOp,
    ) -> Result<crate::bridge::envelope::PhysicalPlan, Option<ArrayRejectMsg>> {
        use crate::bridge::physical_plan::ArrayOp as DataArrayOp;
        use nodedb_array::sync::op::ArrayOpKind;
        use nodedb_types::TenantId as NdTenantId;

        let array_id = nodedb_array::types::ArrayId::new(
            NdTenantId::new(self.tenant_id().as_u32()),
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
