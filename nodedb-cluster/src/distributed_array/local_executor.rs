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
use crate::distributed_array::wire::{ArrayShardAggReq, ArrayShardPutReq, ArrayShardSliceReq};
use crate::error::Result;

/// Execute array operations against the local Data Plane.
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
    ///
    /// Returns `Vec<Vec<u8>>` — one element per matching row, each element
    /// being the zerompk encoding of that row.
    async fn exec_slice(&self, req: &ArrayShardSliceReq) -> Result<Vec<Vec<u8>>>;

    /// Execute a surrogate-bitmap scan and return the zerompk-encoded
    /// `SurrogateBitmap` bytes for matching cells.
    ///
    /// `array_id_msgpack` — zerompk encoding of `nodedb_array::types::ArrayId`.
    /// `slice_msgpack` — zerompk encoding of `nodedb_array::query::Slice`.
    async fn exec_surrogate_bitmap_scan(
        &self,
        array_id_msgpack: &[u8],
        slice_msgpack: &[u8],
    ) -> Result<Vec<u8>>;

    /// Execute a partial aggregate on this shard and return the partial states.
    ///
    /// The Data Plane computes the aggregate with `return_partial = true`, so it
    /// returns `Vec<ArrayAggPartial>` rather than finalized scalars. The
    /// coordinator merges partials from all shards before finalizing.
    async fn exec_agg(&self, req: &ArrayShardAggReq) -> Result<Vec<ArrayAggPartial>>;

    /// Apply a cell-batch write to the local array engine.
    ///
    /// `req.cells_msgpack` is a zerompk encoding of
    /// `Vec<nodedb::engine::array::wal::ArrayPutCell>`. All cells belong to
    /// the same Hilbert-prefix tile. The shard handler has already validated
    /// that this shard owns the tile; the executor dispatches directly to the
    /// Data Plane without further routing checks.
    async fn exec_put(&self, req: &ArrayShardPutReq) -> Result<u64>;

    /// Delete cells by exact coordinates from the local array engine.
    ///
    /// `array_id_msgpack` — zerompk encoding of `nodedb_array::types::ArrayId`.
    /// `coords_msgpack` — zerompk encoding of `Vec<Vec<CoordValue>>`.
    /// `wal_lsn` — WAL sequence number allocated by the Control Plane.
    ///
    /// Returns the `applied_lsn` (equal to `wal_lsn` on success).
    async fn exec_delete(
        &self,
        array_id_msgpack: &[u8],
        coords_msgpack: &[u8],
        wal_lsn: u64,
    ) -> Result<u64>;
}
