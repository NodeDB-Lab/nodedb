// SPDX-License-Identifier: BUSL-1.1

//! Write handlers for [`DataPlaneArrayExecutor`] — put and delete.
//!
//! Run on the shard OWNER after coordinator RPC-routing. Proposed to the
//! owning shard's data Raft group as `ReplicatedWrite::ArrayCellPut` /
//! `ArrayCellDelete`, on a one-node cluster too.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::ArrayShardWriteOutcome;
use nodedb_cluster::distributed_array::wire::{ArrayShardDeleteReq, ArrayShardPutReq};
use nodedb_cluster::error::{ClusterError, Result};

use super::cells::flatten_blob_vec;
use super::executor::DataPlaneArrayExecutor;
use super::refusal::execution_error;
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

impl DataPlaneArrayExecutor {
    pub(super) async fn put(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardPutReq,
    ) -> Result<ArrayShardWriteOutcome> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_put: {e}"),
            })?;

        // The coordinator sends a bucket of separately-encoded `ArrayPutCell`s;
        // the Data Plane decodes one flat msgpack array.
        let cells_msgpack = flatten_blob_vec::<crate::engine::array::wal::ArrayPutCell>(
            &req.cells_msgpack,
            "exec_put",
        )
        .map_err(|e| ClusterError::Codec {
            detail: e.to_string(),
        })?;

        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array_id.clone(),
            cells_msgpack,
            wal_lsn: req.wal_lsn,
            provenance: None,
            vshard_id: local_vshard_id,
        });

        self.propose(&array_id, local_vshard_id, plan, req.wal_lsn, "array put")
            .await
    }

    pub(super) async fn delete(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardDeleteReq,
    ) -> Result<ArrayShardWriteOutcome> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_delete: {e}"),
            })?;

        // Same bucket-to-flat reshape the put path performs: the Data Plane
        // decodes `coords_msgpack` as one `Vec<ArrayDeleteCell>`.
        let coords_msgpack = flatten_blob_vec::<crate::engine::array::wal::ArrayDeleteCell>(
            &req.coords_msgpack,
            "exec_delete",
        )
        .map_err(|e| ClusterError::Codec {
            detail: e.to_string(),
        })?;

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack,
            wal_lsn: req.wal_lsn,
            provenance: None,
            vshard_id: local_vshard_id,
        });

        self.propose(
            &array_id,
            local_vshard_id,
            plan,
            req.wal_lsn,
            "array delete",
        )
        .await
    }

    /// Replicate `plan` to the owning shard's data Raft group. Returns the
    /// `applied_lsn` the coordinator acks with, plus the real cell count the
    /// Data Plane handler's `{"inserted"|"deleted": n}` response reports.
    /// The entry carries the vShard from the validated RPC envelope, so every
    /// replica selects the same Data Plane core.
    async fn propose(
        &self,
        array_id: &ArrayId,
        local_vshard_id: u32,
        plan: PhysicalPlan,
        wal_lsn: u64,
        op_label: &str,
    ) -> Result<ArrayShardWriteOutcome> {
        let proposer = self
            .state
            .async_raft_proposer()
            .map_err(|e| execution_error(&format!("{op_label} raft propose"), e))?;
        let replicable =
            crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)
                .map_err(|e| ClusterError::Storage {
                    detail: format!("{op_label}: {e}"),
                })?;
        let entry = crate::control::wal_replication::to_replicated_entry(
            array_id.tenant_id,
            array_id.database_id,
            VShardId::new(local_vshard_id),
            &replicable,
        )
        .map_err(|e| ClusterError::Storage {
            detail: format!("{op_label}: {e}"),
        })?
        .ok_or_else(|| ClusterError::Storage {
            detail: format!("{op_label}: plan is not encodable as a replicated entry"),
        })?;

        let (apply_payload, _write_version) =
            crate::control::wal_replication::propose_replicated_entry(
                &self.state,
                proposer,
                entry,
                crate::control::wal_replication::statement_propose_deadline(&self.state),
            )
            .await
            .map_err(|e| execution_error(&format!("{op_label} raft propose"), e))?;
        let affected =
            require_affected_count(&apply_payload).map_err(|e| ClusterError::Storage {
                detail: format!("{op_label}: {e}"),
            })?;
        // No LSN exists to report here: each replica mints its own redo,
        // none authoritative. `wal_lsn` is echoed back verbatim — what
        // the coordinator sent, not a claim about what was recorded.
        Ok(ArrayShardWriteOutcome {
            applied_lsn: wal_lsn,
            affected,
        })
    }
}
