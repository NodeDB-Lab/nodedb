// SPDX-License-Identifier: BUSL-1.1

//! The single `ArrayLocalExecutor` trait impl for [`DataPlaneArrayExecutor`].
//!
//! Rust requires every method of a trait impl to live in one block, so this
//! block is the sole point where the trait is satisfied. Each method delegates
//! to the concern-split inherent method (read handlers in [`super::read`], write
//! handlers in [`super::write`]).

use async_trait::async_trait;
use nodedb_cluster::distributed_array::wire::{
    ArrayShardAggReq, ArrayShardDeleteReq, ArrayShardPutReq,
};
use nodedb_cluster::distributed_array::{
    ArrayAggExec, ArrayLocalExecutor, ArrayShardWriteOutcome, ArraySliceExec,
};
use nodedb_cluster::error::Result;

use super::executor::DataPlaneArrayExecutor;

#[async_trait]
impl ArrayLocalExecutor for DataPlaneArrayExecutor {
    async fn exec_slice(
        &self,
        local_vshard_id: u32,
        req: &nodedb_cluster::distributed_array::wire::ArrayShardSliceReq,
    ) -> Result<ArraySliceExec> {
        self.slice(local_vshard_id, req).await
    }

    async fn exec_agg(&self, local_vshard_id: u32, req: &ArrayShardAggReq) -> Result<ArrayAggExec> {
        self.agg(local_vshard_id, req).await
    }

    async fn exec_put(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardPutReq,
    ) -> Result<ArrayShardWriteOutcome> {
        self.put(local_vshard_id, req).await
    }

    async fn exec_delete(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardDeleteReq,
    ) -> Result<ArrayShardWriteOutcome> {
        self.delete(local_vshard_id, req).await
    }

    async fn exec_surrogate_bitmap_scan(
        &self,
        local_vshard_id: u32,
        array_id_msgpack: &[u8],
        slice_msgpack: &[u8],
    ) -> Result<Vec<u8>> {
        self.surrogate_bitmap_scan(local_vshard_id, array_id_msgpack, slice_msgpack)
            .await
    }
}
