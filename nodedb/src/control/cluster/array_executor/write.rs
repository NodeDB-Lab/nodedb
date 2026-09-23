// SPDX-License-Identifier: BUSL-1.1

//! Write handlers for [`DataPlaneArrayExecutor`] — put and delete.
//!
//! Run on the shard OWNER after coordinator RPC-routing. Multi-node: proposed
//! to the owning shard's data Raft group as `ReplicatedWrite::ArrayCellPut` /
//! `ArrayCellDelete`. Single-node: applied via the Control-Plane write funnel,
//! whose redo record is the array engine's only durability (it's a memtable).

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::ArrayShardWriteOutcome;
use nodedb_cluster::distributed_array::wire::{ArrayShardDeleteReq, ArrayShardPutReq};
use nodedb_cluster::error::{ClusterError, Result};

use super::cells::flatten_blob_vec;
use super::executor::DataPlaneArrayExecutor;
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::types::{TraceId, VShardId};
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
        });

        self.propose_or_dispatch(&array_id, local_vshard_id, plan, req.wal_lsn, "array put")
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
        });

        self.propose_or_dispatch(
            &array_id,
            local_vshard_id,
            plan,
            req.wal_lsn,
            "array delete",
        )
        .await
    }

    /// Replicate `plan` to the owning shard's data Raft group when a proposer
    /// exists; otherwise apply it locally through the write funnel. Returns
    /// the `applied_lsn` the coordinator acks with, plus the real cell count
    /// the Data Plane handler's `{"inserted"|"deleted": n}` response reports.
    /// Both branches use the vShard from the validated RPC envelope so all
    /// paths select the same Data Plane core.
    async fn propose_or_dispatch(
        &self,
        array_id: &ArrayId,
        local_vshard_id: u32,
        plan: PhysicalPlan,
        wal_lsn: u64,
        op_label: &str,
    ) -> Result<ArrayShardWriteOutcome> {
        if let Some(proposer) = self.state.async_raft_proposer() {
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
                )
                .await
                .map_err(|e| ClusterError::Storage {
                    detail: format!("{op_label} raft propose: {e}"),
                })?;
            let affected =
                require_affected_count(&apply_payload).map_err(|e| ClusterError::Storage {
                    detail: format!("{op_label}: {e}"),
                })?;
            // No LSN exists to report here: each replica mints its own redo,
            // none authoritative. `wal_lsn` is echoed back verbatim — what
            // the coordinator sent, not a claim about what was recorded.
            return Ok(ArrayShardWriteOutcome {
                applied_lsn: wal_lsn,
                affected,
            });
        }

        // Single-node: this node's WAL is the write's only durability. The
        // funnel appends the redo, stamps the minted LSN into the plan, and
        // fsyncs before this ack returns. Ordering is decided HERE by the gate.
        let outcome: SubmitOutcome = submit_write(
            &self.state,
            single_node_submit(array_id, VShardId::new(local_vshard_id), plan),
        )
        .await
        .map_err(|e| ClusterError::Storage {
            detail: format!("{op_label}: {e}"),
        })?;

        if outcome.response.status == crate::bridge::envelope::Status::Error {
            let detail = outcome
                .response
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("{op_label} Data Plane error: {detail}"),
            });
        }

        // Ack with the LSN the funnel actually minted. `None` would mean the
        // funnel classified this as appending nothing — a wiring bug.
        let applied_lsn =
            outcome
                .wal_lsn
                .map(|lsn| lsn.as_u64())
                .ok_or_else(|| ClusterError::Storage {
                    detail: format!(
                        "{op_label}: applied with no WAL redo record — write is not durable"
                    ),
                })?;
        let affected = require_affected_count(outcome.response.payload.as_ref()).map_err(|e| {
            ClusterError::Storage {
                detail: format!("{op_label}: {e}"),
            }
        })?;
        Ok(ArrayShardWriteOutcome {
            applied_lsn,
            affected,
        })
    }
}

/// Build the single-node write-funnel request using the envelope's validated
/// vShard. The funnel carries this through to both the bridge request and the
/// WAL record, whose replay uses the same vShard-to-core mapping.
fn single_node_submit(
    array_id: &ArrayId,
    local_vshard_id: VShardId,
    plan: PhysicalPlan,
) -> SubmitWrite {
    SubmitWrite {
        tenant_id: array_id.tenant_id,
        database_id: array_id.database_id,
        vshard_id: local_vshard_id,
        plan,
        trace_id: TraceId::generate(),
        event_source: crate::event::EventSource::User,
        txn_id: None,
        user_id: None,
        durability: WalDurability::AppendHere {
            now_override: None,
            apply_key: 0,
        },
        ordering: WriteOrdering::Gate,
        change_feed: ChangeFeedOwner::Funnel,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    #[test]
    fn single_node_write_preserves_nonzero_vshard() {
        let array_id = ArrayId::new(TenantId::new(41), "measurements");
        let vshard_id = VShardId::new(19);
        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });

        let request = single_node_submit(&array_id, vshard_id, plan);

        assert_eq!(request.vshard_id, vshard_id);
    }
}
