// SPDX-License-Identifier: BUSL-1.1

//! Local Data-Plane execution trait for array shard operations.
//!
//! `ArrayLocalExecutor` is defined here in `nodedb-cluster` and
//! implemented in the main `nodedb` binary, which has access to the
//! SPSC bridge and the Data Plane array engine. The shard-side handler
//! (`handler.rs`) holds an `Arc<dyn ArrayLocalExecutor>` and calls
//! through it to execute slice and surrogate-bitmap-scan operations
//! on the local node.
//!
//! This keeps `nodedb-cluster` free of a compile-time dependency on
//! `nodedb` while still producing real results from the Data Plane.

use crate::distributed_array::merge::ArrayAggPartial;
use crate::distributed_array::wire::{
    ArrayShardAggReq, ArrayShardDeleteReq, ArrayShardPutReq, ArrayShardSliceReq,
};
use crate::error::Result;

/// Result of a local shard slice: the per-row bytes plus the bitemporal
/// below-horizon signal.
///
/// `truncated_before_horizon` is computed by the Data Plane (it is `true`
/// when the `system_time` cutoff predates every stored tile version, so the
/// shard produced zero rows for that reason). It MUST be threaded back to the
/// coordinator so the OR-reduce across shards can surface an incomplete-result
/// signal to the client — dropping it would silently report complete results.
pub struct ArraySliceExec {
    pub rows: Vec<Vec<u8>>,
    pub truncated_before_horizon: bool,
}

/// Result of a local shard partial aggregate: the per-group partial states
/// plus the bitemporal below-horizon signal (see [`ArraySliceExec`]).
pub struct ArrayAggExec {
    pub partials: Vec<ArrayAggPartial>,
    pub truncated_before_horizon: bool,
}

/// Execute array operations against the local Data Plane.
///
/// `local_vshard_id` is the destination vShard from the validated RPC envelope.
/// Implementations must preserve it when selecting a Data Plane core and when
/// constructing write/WAL requests.
///
/// The implementor (in `nodedb`) routes the call through the SPSC bridge
/// to the appropriate TPC core and returns the serialised zerompk result
/// rows or bitmap bytes.
#[async_trait::async_trait]
pub trait ArrayLocalExecutor: Send + Sync + 'static {
    /// Execute a coord-range slice and return zerompk-encoded row bytes.
    ///
    /// `array_id_msgpack` — zerompk encoding of `nodedb_array::types::ArrayId`.
    /// `slice_msgpack` — zerompk encoding of `nodedb_array::query::Slice`.
    /// `attr_projection` — attribute index list; empty means all attributes.
    /// `limit` — maximum rows to return per shard (0 = unlimited).
    /// `cell_filter_msgpack` — zerompk encoding of `SurrogateBitmap`; empty
    ///   means no filter.
    /// `shard_hilbert_range` — optional `[lo, hi]` Hilbert-prefix range; when
    ///   set only tiles whose prefix falls in this range are returned, preventing
    ///   duplicate rows in single-node harnesses where all vShards share one
    ///   Data Plane. `None` = no Hilbert filter.
    /// `txn_id` — the reading transaction's id; the executor stamps it on the
    ///   Data Plane request so the shard folds that transaction's staged
    ///   cells into the result. `None` = autocommit read.
    ///
    /// Returns the per-row bytes (one element per matching row, each the
    /// native-msgpack encoding of that row) plus the `truncated_before_horizon`
    /// signal from the Data Plane.
    async fn exec_slice(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardSliceReq,
    ) -> Result<ArraySliceExec>;

    /// Execute a surrogate-bitmap scan and return the zerompk-encoded
    /// `SurrogateBitmap` bytes for matching cells.
    ///
    /// `array_id_msgpack` — zerompk encoding of `nodedb_array::types::ArrayId`.
    /// `slice_msgpack` — zerompk encoding of `nodedb_array::query::Slice`.
    async fn exec_surrogate_bitmap_scan(
        &self,
        local_vshard_id: u32,
        array_id_msgpack: &[u8],
        slice_msgpack: &[u8],
    ) -> Result<Vec<u8>>;

    /// Execute a partial aggregate on this shard and return the partial states.
    ///
    /// The Data Plane computes the aggregate with `return_partial = true`, so it
    /// returns partial states (plus the `truncated_before_horizon` signal)
    /// rather than finalized scalars. The coordinator merges partials from all
    /// shards before finalizing. `req.txn_id` is stamped on the Data Plane
    /// request so the partial folds in that transaction's staged cells on
    /// this shard.
    async fn exec_agg(&self, local_vshard_id: u32, req: &ArrayShardAggReq) -> Result<ArrayAggExec>;

    /// Apply a cell-batch write to the local array engine.
    ///
    /// `req.cells_msgpack` is a zerompk encoding of
    /// `Vec<nodedb::engine::array::wal::ArrayPutCell>`. All cells belong to
    /// the same Hilbert-prefix tile. The shard handler has already validated
    /// that this shard owns the tile; the executor dispatches directly to the
    /// Data Plane without further routing checks.
    async fn exec_put(&self, local_vshard_id: u32, req: &ArrayShardPutReq) -> Result<u64>;

    /// Delete cells by exact coordinates from the local array engine.
    ///
    /// Takes the full `ArrayShardDeleteReq` (not just the coord bytes) so the
    /// local executor can apply the original delete payload on the validated
    /// `local_vshard_id`, mirroring [`Self::exec_put`].
    ///
    /// Returns the `applied_lsn` (equal to `req.wal_lsn` on success).
    async fn exec_delete(&self, local_vshard_id: u32, req: &ArrayShardDeleteReq) -> Result<u64>;
}
