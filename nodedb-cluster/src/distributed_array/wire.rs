// SPDX-License-Identifier: BUSL-1.1

//! Wire types for array shard RPC messages.
//!
//! All request/response pairs are zerompk-serialisable. Complex sub-payloads
//! (slice predicates, cell batches) are carried as opaque msgpack bytes —
//! the same convention used by `ArrayOp` in the bridge physical plan — so
//! this crate does not need a compile-time dependency on `nodedb-array`.

/// Scatter request: coordinator asks a shard to execute a coord-range slice.
///
/// `slice_msgpack` is a zerompk encoding of `nodedb_array::query::Slice`.
/// `attr_projection` is the attribute index list; empty means all attributes.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardSliceReq {
    /// Array scoped by tenant + name (zerompk encoding of `nodedb_array::types::ArrayId`).
    pub array_id_msgpack: Vec<u8>,
    /// Zerompk encoding of `nodedb_array::query::Slice`.
    pub slice_msgpack: Vec<u8>,
    pub attr_projection: Vec<u32>,
    pub limit: u32,
    /// Optional surrogate pre-filter bitmap bytes (zerompk encoding of `SurrogateBitmap`).
    /// Empty slice means no filter.
    pub cell_filter_msgpack: Vec<u8>,
    /// The array's routing granularity (1–16). 0 skips routing validation.
    ///
    /// When non-zero the shard checks that some key in `slice_tile_keys`
    /// routes to its `local_vshard_id`. A mismatch returns
    /// `ClusterError::WrongOwner`, so the coordinator retries against a
    /// refreshed routing table.
    pub prefix_bits: u8,
    /// Tile keys covering the slice, at least one per bucket it touches,
    /// computed by the planner. Empty means unbounded and skips validation.
    pub slice_tile_keys: Vec<u64>,
    /// The bucket this shard owns. The shard returns only cells whose tile
    /// key falls in it, so vShards that share one Data Plane return no
    /// duplicate rows. `None` returns every matching cell.
    pub shard_bucket: Option<nodedb_types::ShardBucket>,
    /// Bitemporal system-time scope forwarded from `ArrayOp::Slice::system_time`.
    ///
    /// - `Current` = live read.
    /// - `AsOf(t)` = point-in-time at `t`.
    /// - `AllVersions` = audit-log fan-out — shard emits all live cell-versions,
    ///   each tagged with `ArrayCell::system_time`.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point forwarded from `ArrayOp::Slice::valid_at_ms`.
    /// `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
    /// The reading transaction's id, for read-your-own-writes against the
    /// shard's per-transaction staging overlay. `None` = autocommit read.
    ///
    /// The id is minted on the coordinator and has meaning on a shard only
    /// where that transaction staged cells. Staging resolves each shard's
    /// own leader and stages there, and this read resolves the same leader,
    /// so the overlay keyed by this id is on the node that serves the read.
    pub txn_id: Option<u64>,
    /// Zerompk encoding of `nodedb_array::query::PolygonZone`, forwarded from
    /// `ArrayOp::Slice::zone_msgpack`. Empty = no zone.
    pub zone_msgpack: Vec<u8>,
    /// Read attrs physically, forwarded from `ArrayOp::Slice::physical`.
    pub physical: bool,
}

/// Gather response: shard returns matching rows as opaque msgpack row bytes.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardSliceResp {
    pub shard_id: u32,
    /// Zerompk-encoded rows. Each element is a zerompk encoding of one result row.
    pub rows_msgpack: Vec<Vec<u8>>,
    /// True when the shard hit the `limit` and may have more rows.
    pub truncated: bool,
    /// True when the system-time cutoff is below the oldest tile version on this shard
    /// and the shard produced zero rows as a result of that horizon.
    pub truncated_before_horizon: bool,
    /// The write versions the shard read observed, one per vShard it covers,
    /// even when it matched no row. A transaction validates the read at
    /// these versions.
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// Scatter request: coordinator asks a shard to compute a partial aggregate.
///
/// Mirrors `ArrayOp::Aggregate` — attribute index + reducer + optional filter.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardAggReq {
    pub array_id_msgpack: Vec<u8>,
    pub attr_idx: u32,
    /// Zerompk encoding of `ArrayReducer`.
    pub reducer_msgpack: Vec<u8>,
    /// Negative means no group-by; non-negative is the dimension index.
    pub group_by_dim: i32,
    /// Optional surrogate pre-filter; empty = all cells.
    pub cell_filter_msgpack: Vec<u8>,
    /// The bucket this shard owns. The shard counts only cells whose tile
    /// key falls in it. `None` counts every cell.
    pub shard_bucket: Option<nodedb_types::ShardBucket>,
    /// Bitemporal system-time scope forwarded from
    /// `ArrayOp::Aggregate::system_time`. A `Pinned` scope names the cut
    /// marker the shard waits for before it reads.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point forwarded from `ArrayOp::Aggregate::valid_at_ms`.
    /// `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
    /// The reading transaction's id, for read-your-own-writes against the
    /// shard's per-transaction staging overlay. `None` = autocommit read.
    ///
    /// The id is minted on the coordinator and has meaning on a shard only
    /// where that transaction staged cells. Staging resolves each shard's
    /// own leader and stages there, and this read resolves the same leader,
    /// so the overlay keyed by this id is on the node that serves the read.
    /// Each shard folds only its own overlay cells into its partial, so the
    /// coordinator's merge counts every staged cell exactly once.
    pub txn_id: Option<u64>,
    /// Zerompk encoding of `nodedb_array::query::ArrayRegion`, forwarded from
    /// `ArrayOp::Aggregate::region_msgpack`. Empty = the whole array.
    pub region_msgpack: Vec<u8>,
    /// Reduce physical values, forwarded from `ArrayOp::Aggregate::physical`.
    pub physical: bool,
}

/// Gather response: shard returns partial aggregate(s) for merge.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardAggResp {
    pub shard_id: u32,
    /// One partial per group-by bucket (or one entry when group_by_dim < 0).
    pub partials: Vec<super::merge::ArrayAggPartial>,
    /// True when the system-time cutoff is below the oldest tile version on this shard
    /// and the shard produced zero rows as a result of that horizon.
    pub truncated_before_horizon: bool,
    /// The write versions the shard read observed (see
    /// [`ArrayShardSliceResp::read_versions`]).
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// Scatter request: coordinator asks a shard to pair the cells of two arrays
/// it owns and apply a binary op to each pair.
///
/// A cell routes by its tile key, and the tile key is a function of the
/// schema's dims and tile extents alone. Two arrays with one schema hash and
/// one `prefix_bits` therefore place every coordinate on the same shard, so
/// each shard pairs its own cells and no cell crosses shards.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardElementwiseReq {
    /// Zerompk encoding of the left `nodedb_array::types::ArrayId`.
    pub left_id_msgpack: Vec<u8>,
    /// Zerompk encoding of the right `nodedb_array::types::ArrayId`.
    pub right_id_msgpack: Vec<u8>,
    /// Zerompk encoding of `ArrayBinaryOp` (c_enum, 1 byte on wire).
    pub op_msgpack: Vec<u8>,
    pub attr_idx: u32,
    /// The bucket this shard owns. The shard pairs only cells whose tile key
    /// falls in it, so vShards that share one Data Plane emit no row twice.
    /// `None` pairs every cell.
    pub shard_bucket: Option<nodedb_types::ShardBucket>,
    /// Bitemporal system-time scope for both operands (see
    /// [`ArrayShardAggReq::system_time`]).
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point for both operands. `None` = no
    /// valid-time filter.
    pub valid_at_ms: Option<i64>,
    /// The reading transaction's id, for read-your-own-writes against the
    /// shard's staging overlay (see [`ArrayShardSliceReq::txn_id`]).
    pub txn_id: Option<u64>,
}

/// Gather response: shard returns its paired rows.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardElementwiseResp {
    pub shard_id: u32,
    /// One element per output cell, each the native-msgpack encoding of one
    /// `{"coords", "attrs"}` row.
    pub rows_msgpack: Vec<Vec<u8>>,
    /// Whether the system-time cutoff predates this shard's retained history
    /// (see [`ArrayShardSliceResp::truncated_before_horizon`]).
    pub truncated_before_horizon: bool,
    /// The write versions the shard read observed (see
    /// [`ArrayShardSliceResp::read_versions`]).
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// The maintenance a shard runs on its local copy of an array.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum ArrayShardMaintOp {
    /// Flush the memtable to a segment.
    Flush,
    /// Merge segments. `audit_retain_ms` is the array's audit-retention
    /// horizon from the catalog; `None` keeps every version.
    Compact { audit_retain_ms: Option<i64> },
}

/// Scatter request: coordinator asks a shard to flush or compact an array.
///
/// A core holds the tiles of every bucket its vShards own, so the request
/// goes to every primary vShard. A core that two vShards share runs it
/// twice; the second run finds nothing left to do.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardMaintReq {
    pub array_id_msgpack: Vec<u8>,
    pub op: ArrayShardMaintOp,
}

/// Acknowledgement: shard ran the maintenance.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardMaintResp {
    pub shard_id: u32,
    /// The count the Data Plane handler answered with: `1` for a flush, the
    /// number of segments merged for a compaction.
    pub affected: u64,
}

/// Scatter request: coordinator forwards a cell write to the owning shard.
///
/// `cells_msgpack` is a zerompk encoding of
/// `Vec<nodedb::engine::array::wal::ArrayPutCell>`.
/// All cells in this batch route to the same bucket (`prefix_bucket` of the tile key).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardPutReq {
    pub array_id_msgpack: Vec<u8>,
    pub cells_msgpack: Vec<u8>,
    pub wal_lsn: u64,
    /// Tile key of any cell in this batch. The shard handler routes it to
    /// check that it owns the batch.
    pub representative_tile_key: u64,
    /// Prefix bits used for routing (1–16). 0 means no validation.
    pub prefix_bits: u8,
}

/// Acknowledgement: shard confirms the put was applied.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardPutResp {
    pub shard_id: u32,
    pub applied_lsn: u64,
    /// Cells this shard actually wrote, read from the Data Plane handler's
    /// `{"inserted": n}` count. The coordinator sums this across shards for
    /// the client-facing `INSERT n` count — never `cells.len()`, which counts
    /// coordinates named, not cells written.
    pub affected: u64,
}

/// Scatter request: coordinator asks a shard to delete cells by exact coords.
///
/// `coords_msgpack` is a zerompk encoding of
/// `Vec<nodedb::engine::array::wal::ArrayDeleteCell>`.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardDeleteReq {
    pub array_id_msgpack: Vec<u8>,
    pub coords_msgpack: Vec<u8>,
    pub wal_lsn: u64,
    /// Tile key of any coord in this batch. The shard handler routes it to
    /// check that it still owns the batch's bucket.
    pub representative_tile_key: u64,
    /// Prefix bits used for routing (1–16). 0 means no validation.
    pub prefix_bits: u8,
}

/// Acknowledgement: shard confirms the delete was applied.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardDeleteResp {
    pub shard_id: u32,
    pub applied_lsn: u64,
    /// Cells this shard actually removed, read from the Data Plane handler's
    /// `{"deleted": n}` count (the number of coords that existed, not the
    /// number named). A delete of an absent coordinate contributes 0. The
    /// coordinator sums this across shards for the client-facing `DELETE n`
    /// count — never `coords.len()`.
    pub affected: u64,
}

/// Scatter request: coordinator asks a shard to run a surrogate bitmap scan.
///
/// Used by the cross-engine fusion path to collect surrogates for cells
/// matching a coord-range slice.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardSurrogateBitmapReq {
    pub array_id_msgpack: Vec<u8>,
    /// Zerompk encoding of `nodedb_array::query::Slice`.
    pub slice_msgpack: Vec<u8>,
}

/// Response: shard returns a surrogate bitmap for matching cells.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardSurrogateBitmapResp {
    pub shard_id: u32,
    /// Zerompk encoding of `SurrogateBitmap` for the matching cells.
    pub bitmap_msgpack: Vec<u8>,
}
