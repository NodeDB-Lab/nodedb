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
    /// Prefix bits used for Hilbert routing (1–16). 0 means no routing validation.
    ///
    /// When non-zero the shard verifies that its `local_vshard_id` is covered
    /// by the Hilbert ranges in `slice_hilbert_ranges` before executing the
    /// scan. A mismatch returns `ClusterError::WrongOwner` so the coordinator
    /// can retry against a refreshed routing table.
    pub prefix_bits: u8,
    /// Inclusive Hilbert-prefix ranges `(lo, hi)` that this slice covers,
    /// pre-computed by the coordinator from the slice predicate. Each entry
    /// is encoded as two consecutive `u64` values (little-endian). Empty
    /// means unbounded (no routing validation is performed).
    pub slice_hilbert_ranges: Vec<(u64, u64)>,
    /// Hilbert-prefix range `[lo, hi]` that this shard owns. The shard
    /// applies this as a pre-filter so it only returns cells whose Hilbert
    /// prefix falls within the range. `None` means unbounded (return all
    /// matching cells). Used by the distributed shard handler to prevent
    /// duplicate rows in single-node harnesses where all vShards share one
    /// Data Plane.
    pub shard_hilbert_range: Option<(u64, u64)>,
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
}

/// Gather response: shard returns matching rows as opaque msgpack row bytes.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardSliceResp {
    pub shard_id: u32,
    /// Zerompk-encoded rows. Each element is a zerompk encoding of one result row.
    pub rows_msgpack: Vec<Vec<u8>>,
    /// True when the shard hit the `limit` and may have more rows.
    pub truncated: bool,
    /// True when `system_as_of` is below the oldest tile version on this shard
    /// and the shard produced zero rows as a result of that horizon.
    pub truncated_before_horizon: bool,
}

/// Scatter request: coordinator asks a shard to compute a partial aggregate.
///
/// Mirrors `ArrayOp::Aggregate` — attribute index + reducer + optional filter.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardAggReq {
    pub array_id_msgpack: Vec<u8>,
    pub attr_idx: u32,
    /// Zerompk encoding of `ArrayReducer` (c_enum, 1 byte on wire).
    pub reducer_msgpack: Vec<u8>,
    /// Negative means no group-by; non-negative is the dimension index.
    pub group_by_dim: i32,
    /// Optional surrogate pre-filter; empty = all cells.
    pub cell_filter_msgpack: Vec<u8>,
    /// Hilbert-prefix range `[lo, hi]` that this shard owns. The shard
    /// applies this as a pre-filter so it only counts cells whose Hilbert
    /// prefix falls within the range. `None` means unbounded (scan all).
    pub shard_hilbert_range: Option<(u64, u64)>,
    /// Bitemporal system-time cutoff forwarded from `ArrayOp::Aggregate::system_as_of`.
    /// `None` = live read.
    pub system_as_of: Option<i64>,
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
}

/// Gather response: shard returns partial aggregate(s) for merge.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardAggResp {
    pub shard_id: u32,
    /// One partial per group-by bucket (or one entry when group_by_dim < 0).
    pub partials: Vec<super::merge::ArrayAggPartial>,
    /// True when `system_as_of` is below the oldest tile version on this shard
    /// and the shard produced zero rows as a result of that horizon.
    pub truncated_before_horizon: bool,
}

/// Scatter request: coordinator forwards a cell write to the owning shard.
///
/// `cells_msgpack` is a zerompk encoding of
/// `Vec<nodedb::engine::array::wal::ArrayPutCell>`.
/// All cells in this batch belong to the same Hilbert-prefix bucket.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardPutReq {
    pub array_id_msgpack: Vec<u8>,
    pub cells_msgpack: Vec<u8>,
    pub wal_lsn: u64,
    /// Hilbert prefix of any representative cell in this batch (used by
    /// the shard handler to validate routing). Set to 0 when routing
    /// metadata is unavailable (pre-routing clients).
    pub representative_hilbert_prefix: u64,
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
/// `coords_msgpack` is a zerompk encoding of `Vec<Vec<CoordValue>>`.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayShardDeleteReq {
    pub array_id_msgpack: Vec<u8>,
    pub coords_msgpack: Vec<u8>,
    pub wal_lsn: u64,
    /// Hilbert prefix of any representative coord in this batch. Used by the
    /// shard handler to validate that it still owns the target Hilbert bucket.
    /// Set to 0 when routing metadata is unavailable.
    pub representative_hilbert_prefix: u64,
    /// Prefix bits used for Hilbert routing (1–16). 0 means no validation.
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
