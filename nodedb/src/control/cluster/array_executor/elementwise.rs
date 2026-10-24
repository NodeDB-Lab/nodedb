// SPDX-License-Identifier: BUSL-1.1

//! Shard-side `ARRAY_ELEMENTWISE`: run the op on this shard's Data Plane over
//! the cells of both arrays in the shard's bucket.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::ArrayElementwiseExec;
use nodedb_cluster::distributed_array::wire::ArrayShardElementwiseReq;
use nodedb_cluster::error::{ClusterError, Result};

use crate::types::{TxnId, VShardId};
use nodedb_physical::physical_plan::{ArrayBinaryOp, ArrayOp, PhysicalPlan};

use super::executor::DataPlaneArrayExecutor;
use super::read::split_msgpack_array_rows;
use super::refusal::refusal_error;

impl DataPlaneArrayExecutor {
    pub(super) async fn elementwise(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardElementwiseReq,
    ) -> Result<ArrayElementwiseExec> {
        let decode_id = |bytes: &[u8], side: &str| -> Result<ArrayId> {
            zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Codec {
                detail: format!("{side} array_id decode in exec_elementwise: {e}"),
            })
        };
        let left = decode_id(&req.left_id_msgpack, "left")?;
        let right = decode_id(&req.right_id_msgpack, "right")?;
        let op: ArrayBinaryOp =
            zerompk::from_msgpack(&req.op_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("op decode in exec_elementwise: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::Elementwise {
            left: left.clone(),
            right,
            op,
            attr_idx: req.attr_idx,
            cell_filter: None,
            shard_bucket: req.shard_bucket,
            system_time: req.system_time,
            valid_at_ms: req.valid_at_ms,
        });
        let resp = self
            .dispatch_and_await(
                &left,
                VShardId::new(local_vshard_id),
                plan,
                req.txn_id.map(TxnId::new),
            )
            .await?;
        if resp.status == crate::bridge::envelope::Status::Error {
            return Err(refusal_error("array elementwise", &resp));
        }
        let mut rows = split_msgpack_array_rows(&resp.payload)?;
        // A temporal read ends with the `{"truncated_before_horizon": bool}`
        // summary row. The coordinator ORs the flag and appends one summary
        // row for the whole fan-out.
        let truncated_before_horizon = if req.system_time.is_temporal() || req.valid_at_ms.is_some()
        {
            let summary = rows.pop().ok_or_else(|| ClusterError::Codec {
                detail: "exec_elementwise: temporal answer has no summary row".into(),
            })?;
            horizon_flag(&summary)?
        } else {
            false
        };
        Ok(ArrayElementwiseExec {
            rows,
            truncated_before_horizon,
            read_versions: resp.read_versions.to_wire(),
        })
    }
}

/// The flag of a `{"truncated_before_horizon": bool}` summary row.
fn horizon_flag(row: &[u8]) -> Result<bool> {
    let value = nodedb_types::json_from_msgpack(row).map_err(|e| ClusterError::Codec {
        detail: format!("exec_elementwise: summary row decode: {e}"),
    })?;
    value
        .get("truncated_before_horizon")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| ClusterError::Codec {
            detail: format!("exec_elementwise: last row {value} is not a horizon summary"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_row_yields_its_flag() {
        let row = nodedb_types::json_to_msgpack(&serde_json::json!({
            "truncated_before_horizon": true
        }))
        .expect("encode");
        assert!(horizon_flag(&row).expect("flag"));
        let cell = nodedb_types::json_to_msgpack(&serde_json::json!({
            "coords": [1], "attrs": [2.0]
        }))
        .expect("encode");
        assert!(horizon_flag(&cell).is_err(), "a cell row is no summary");
    }
}
