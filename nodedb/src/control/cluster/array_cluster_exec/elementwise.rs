// SPDX-License-Identifier: BUSL-1.1

//! `ClusterArrayOp::Elementwise`: fan the op out to every primary vShard.
//!
//! Each shard pairs the cells of both arrays in its own bucket on its Data
//! Plane. The coordinator concatenates the rows into the one msgpack array a
//! local `ArrayOp::Elementwise` answers with.

use std::sync::Arc;

use nodedb_cluster::distributed_array::coordinator::ArrayCoordinator;
use nodedb_cluster::distributed_array::wire::ArrayShardElementwiseReq;
use nodedb_physical::physical_plan::ArrayBinaryOp;

use crate::control::cluster::array_cluster_helpers::{
    cluster_err, coord_err, encode_err, encode_row_array,
};
use crate::types::TxnId;

use super::executor::{ARRAY_RPC_TIMEOUT_MS, ClusterArrayExecutor, note_array_reads};

pub(super) struct ElementwiseArgs<'a> {
    pub left: &'a nodedb_array::types::ArrayId,
    pub right: &'a nodedb_array::types::ArrayId,
    pub op: ArrayBinaryOp,
    pub attr_idx: u32,
    pub prefix_bits: u8,
    pub system_time: nodedb_types::SystemTimeScope,
    pub valid_at_ms: Option<i64>,
    pub txn_id: Option<TxnId>,
}

impl ClusterArrayExecutor {
    pub(super) async fn execute_elementwise(
        &self,
        args: ElementwiseArgs<'_>,
    ) -> crate::Result<Vec<u8>> {
        let ElementwiseArgs {
            left,
            right,
            op,
            attr_idx,
            prefix_bits,
            system_time,
            valid_at_ms,
            txn_id,
        } = args;
        // No tile keys: an elementwise op reads every tile of both arrays.
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            &[],
            prefix_bits,
            self.total_shards,
            Arc::clone(&self.dispatch),
            Arc::clone(&self.circuit_breaker),
        )
        .map_err(cluster_err)?;

        let req = ArrayShardElementwiseReq {
            left_id_msgpack: zerompk::to_msgpack_vec(left).map_err(encode_err)?,
            right_id_msgpack: zerompk::to_msgpack_vec(right).map_err(encode_err)?,
            op_msgpack: zerompk::to_msgpack_vec(&op).map_err(encode_err)?,
            attr_idx,
            // coord_elementwise stamps each shard's bucket.
            shard_bucket: None,
            system_time,
            valid_at_ms,
            txn_id: txn_id.map(TxnId::as_u64),
        };
        let mut result = coordinator
            .coord_elementwise(req)
            .await
            .map_err(coord_err)?;
        // Each leg read both arrays on its vShard.
        note_array_reads(left, &result.read_versions);
        note_array_reads(right, &result.read_versions);
        // A temporal read ends with one summary row, as a local elementwise
        // op answers.
        if system_time.is_temporal() || valid_at_ms.is_some() {
            let summary = nodedb_types::json_to_msgpack(&serde_json::json!({
                "truncated_before_horizon": result.truncated_before_horizon
            }))
            .map_err(encode_err)?;
            result.rows.push(summary);
        }
        Ok(encode_row_array(&result.rows))
    }
}
