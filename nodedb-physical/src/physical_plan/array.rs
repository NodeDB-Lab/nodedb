// SPDX-License-Identifier: Apache-2.0

//! Array engine operations dispatched to the Data Plane.
//!
//! `ArrayOp` is the wire type for array query operators (slice,
//! aggregate, elementwise) plus the write-side ops (put,
//! delete) and engine maintenance (flush, compact). Complex nested
//! payloads — schemas, slice predicates, cell batches — ride as
//! opaque msgpack bytes (`*_msgpack`) decoded on the Data Plane via
//! zerompk against the canonical type from `nodedb-array`. This keeps
//! the bridge enum flat and zerompk-derivable while preserving full
//! type fidelity at the engine boundary.

use nodedb_array::types::ArrayId;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{ShardBucket, SurrogateBitmap, SystemTimeScope};

/// Reducer for [`ArrayOp::Aggregate`]. Mirrors
/// `nodedb_array::query::aggregate::Reducer`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ArrayReducer {
    Sum,
    Count,
    Min,
    Max,
    Mean,
    Median,
    /// Exact percentile `p`, `0 <= p <= 1`.
    Percentile {
        p: f64,
    },
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
    Mode,
    /// Fixed-width bin counts over `[lo, hi]`.
    Histogram {
        bins: u32,
        lo: f64,
        hi: f64,
    },
}

impl ArrayReducer {
    /// The engine reducer. A histogram's bins and range are checked.
    pub fn engine(
        self,
    ) -> Result<nodedb_array::query::aggregate::Reducer, nodedb_types::array_agg::HistogramSpecError>
    {
        use nodedb_array::query::aggregate::Reducer;
        Ok(match self {
            ArrayReducer::Sum => Reducer::Sum,
            ArrayReducer::Count => Reducer::Count,
            ArrayReducer::Min => Reducer::Min,
            ArrayReducer::Max => Reducer::Max,
            ArrayReducer::Mean => Reducer::Mean,
            ArrayReducer::Median => Reducer::Median,
            ArrayReducer::Percentile { p } => Reducer::Percentile(p),
            ArrayReducer::StdDevPop => Reducer::StdDevPop,
            ArrayReducer::StdDevSamp => Reducer::StdDevSamp,
            ArrayReducer::VarPop => Reducer::VarPop,
            ArrayReducer::VarSamp => Reducer::VarSamp,
            ArrayReducer::Mode => Reducer::Mode,
            ArrayReducer::Histogram { bins, lo, hi } => {
                Reducer::Histogram(nodedb_types::array_agg::HistogramSpec::new(bins, lo, hi)?)
            }
        })
    }
}

/// Pairwise op for [`ArrayOp::Elementwise`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum ArrayBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Array engine physical operations.
///
/// All ops scope by [`ArrayId`] (tenant + array name). LSN-bearing
/// write ops (`Put`, `Delete`) carry a Control-Plane-allocated
/// `wal_lsn` so the Data Plane handler can stamp memtable entries
/// without re-appending to the WAL — matching the existing
/// timeseries / columnar dispatch contract.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ArrayOp {
    /// Open or attach to an existing array. Schema bytes are an
    /// zerompk encoding of `nodedb_array::ArraySchema`. The DDL
    /// pathway will replace this with a catalog lookup.
    OpenArray {
        array_id: ArrayId,
        schema_msgpack: Vec<u8>,
        schema_hash: u64,
        /// Routing bucket bits for vShard routing. Immutable post-create.
        prefix_bits: u8,
        /// Array audit retention selected by CREATE ARRAY. This stays on the
        /// authorized task so planning remains read-only.
        audit_retain_ms: Option<i64>,
        /// Immutable lower bound for `audit_retain_ms`.
        minimum_audit_retain_ms: Option<u64>,
    },

    /// Insert one or more cells. `cells_msgpack` is an zerompk
    /// encoding of `Vec<nodedb::engine::array::wal::ArrayPutCell>`. The WAL
    /// record splices these bytes in without decoding them. Each cell carries a Control-Plane-allocated `Surrogate` parallel
    /// to its `coord` tuple — cross-engine bitmap joins read the
    /// surrogate column on read paths instead of translating coords
    /// back to user-visible PKs.
    ///
    /// `provenance` is `Some` only on the inbound sync path (Lite → Origin).
    /// The Data Plane uses it for epoch fencing and HWM tracking.
    /// Non-sync callers (SQL DML, Raft apply) set this to `None`.
    Put {
        array_id: ArrayId,
        cells_msgpack: Vec<u8>,
        wal_lsn: u64,
        /// Sync provenance for epoch fencing and HWM tracking.
        /// `None` for locally-originated or Raft-replicated writes.
        #[serde(default)]
        provenance: Option<SyncProvenance>,
        /// The vShard every cell of the op homes to: the vShard its tile
        /// hashes to. A Calvin transaction routes and locks the write by it.
        vshard_id: u32,
    },

    /// Delete by exact coordinates. `coords_msgpack` is a zerompk
    /// encoding of `Vec<nodedb::engine::array::wal::ArrayDeleteCell>`.
    /// The WAL record splices these bytes in without decoding them.
    ///
    /// `provenance` is `Some` only on the inbound sync path (Lite → Origin).
    Delete {
        array_id: ArrayId,
        coords_msgpack: Vec<u8>,
        wal_lsn: u64,
        /// Sync provenance for epoch fencing and HWM tracking.
        /// `None` for locally-originated or Raft-replicated writes.
        #[serde(default)]
        provenance: Option<SyncProvenance>,
        /// The vShard every coordinate of the op homes to: the vShard its
        /// tile hashes to. A Calvin transaction routes and locks the write
        /// by it.
        vshard_id: u32,
    },

    /// Coord-range slice with optional attribute projection.
    /// `slice_msgpack` is an zerompk encoding of
    /// `nodedb_array::query::Slice`. Empty `attr_projection` means
    /// "all attributes".
    ///
    /// Under `SystemTimeScope::AllVersions` (`AS OF SYSTEM TIME NULL`),
    /// the Data Plane emits one row per live cell-version, each carrying
    /// `ArrayCell::system_time = Some(system_from_ms)`. Rows are sorted
    /// ascending by system time, ties broken by coord lexicographic order.
    /// `limit` bounds the total number of versions returned (not cells).
    Slice {
        array_id: ArrayId,
        slice_msgpack: Vec<u8>,
        attr_projection: Vec<u32>,
        limit: u32,
        /// Optional surrogate prefilter restricting which cells are returned.
        /// Only cells whose surrogate is present in this bitmap are included.
        /// `None` = no restriction.
        cell_filter: Option<SurrogateBitmap>,
        /// Optional shard bucket. When set, only cells whose tile key routes
        /// to this bucket are included, so vShards that share one Data
        /// Plane return no duplicate rows. `None` includes every cell.
        shard_bucket: Option<ShardBucket>,
        /// Bitemporal system-time scope.
        ///
        /// - `Current`: live read (returns the latest committed state).
        /// - `AsOf(t)`: point-in-time snapshot at system-time `t`.
        /// - `AllVersions`: audit-log read — every live cell-version,
        ///   ordered ascending by `system_from_ms`. Each emitted row has
        ///   `ArrayCell::system_time = Some(system_from_ms)`.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point. When `Some(vt)`, a cell is only
        /// returned if `valid_from_ms <= vt < valid_until_ms`. `None` =
        /// no valid-time filter.
        valid_at_ms: Option<i64>,
        /// zerompk-encoded `nodedb_array::query::PolygonZone`. Only cells
        /// inside it are returned. Empty = no zone.
        zone_msgpack: Vec<u8>,
        /// Read attrs physically: a nodata cell as NULL, and a cell of an
        /// attr with a scale or offset as `raw * scale + offset`.
        physical: bool,
    },

    /// Reduce one attribute column. `group_by_dim < 0` means no
    /// group-by (one scalar partial per tile, merged across tiles by
    /// the executor).
    Aggregate {
        array_id: ArrayId,
        attr_idx: u32,
        reducer: ArrayReducer,
        group_by_dim: i32,
        /// Optional surrogate prefilter: only cells whose surrogate is in
        /// this bitmap contribute to the aggregate. `None` = all cells.
        cell_filter: Option<SurrogateBitmap>,
        /// When `true` the Data Plane returns a zerompk-encoded
        /// `Vec<ArrayAggPartial>` instead of the finalized scalar rows.
        /// Used by the distributed shard handler so the coordinator can
        /// merge partials across shards before finalizing.
        /// Single-node SQL callers always pass `false`.
        return_partial: bool,
        /// Optional shard bucket. When set, only cells whose tile key routes
        /// to this bucket contribute, so vShards that share one Data Plane
        /// count no cell twice. `None` includes every cell.
        shard_bucket: Option<ShardBucket>,
        /// Bitemporal system-time scope. A point-in-time scope reads the
        /// tile versions visible at its cutoff; a `Pinned` scope also names
        /// the cut marker every serving node waits for. `Current` = live
        /// read. `AllVersions` is refused: an aggregate over every version
        /// has no meaning.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point. When `Some(vt)`, only cells whose
        /// `valid_from_ms <= vt < valid_until_ms` contribute. `None` = no
        /// valid-time filter.
        valid_at_ms: Option<i64>,
        /// zerompk-encoded `nodedb_array::query::ArrayRegion`. Only cells
        /// inside it contribute. Empty = the whole array.
        region_msgpack: Vec<u8>,
        /// Reduce physical values: `raw * scale + offset` for an attr with a
        /// scale or offset. A nodata cell never contributes, either way.
        physical: bool,
    },

    /// Pairwise op between two coord-aligned arrays. Both must share
    /// schema (validated on the Data Plane).
    Elementwise {
        left: ArrayId,
        right: ArrayId,
        op: ArrayBinaryOp,
        attr_idx: u32,
        /// Optional surrogate prefilter restricting which cells participate
        /// in the pairwise op. Applied to both operands so outer-join
        /// fallthroughs from either side are also excluded.
        /// `None` = all cells.
        cell_filter: Option<SurrogateBitmap>,
        /// Optional shard bucket. When set, only cells whose tile key routes
        /// to this bucket take part on either side, so vShards that share
        /// one Data Plane emit no row twice. `None` includes every cell.
        shard_bucket: Option<ShardBucket>,
        /// Bitemporal system-time scope for both operands, as on
        /// `Aggregate`.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point for both operands. `None` = no
        /// valid-time filter.
        valid_at_ms: Option<i64>,
    },

    /// Force a memtable flush. Returns the new segment ref's id +
    /// flush_lsn in the response payload. The Control Plane allocates
    /// `wal_lsn` from the central WAL writer and the engine stamps it
    /// as the segment's flush watermark.
    Flush { array_id: ArrayId, wal_lsn: u64 },

    /// Trigger compaction if the picker selects one. Response
    /// indicates whether a merge happened.
    ///
    /// `audit_retain_ms` is the array's retention window in milliseconds
    /// (from `ArrayCatalogEntry`). `None` means retain all versions.
    Compact {
        array_id: ArrayId,
        audit_retain_ms: Option<i64>,
    },

    /// Coord-range slice that emits one document-shaped row per matching
    /// cell, where the row's `id` is the cell's bound `Surrogate` formatted
    /// as 8-char zero-padded lowercase hex (substrate row-key format).
    /// Used by the cross-engine fusion path: the vector engine runs this
    /// as an `inline_prefilter_plan` and reads `id` via
    /// `collect_surrogates` to build a `SurrogateBitmap`. Since the
    /// surrogate identity initiative binds every cell to a global
    /// `Surrogate` at write time, the cell's own surrogate is the
    /// cross-engine join key — no extra attr resolution needed.
    SurrogateBitmapScan {
        array_id: ArrayId,
        slice_msgpack: Vec<u8>,
    },

    /// Reversibly stage a per-core array drop. The store is closed and its
    /// directory atomically renamed to a deterministic tombstone. This is the
    /// externally planned `DROP ARRAY` operation and is idempotent.
    DropArray { array_id: ArrayId },

    /// Move a store from `array_id` to `target` on one core: flush, close,
    /// and rename its directory. The MOVE TENANT rekey sends it to every
    /// core. Idempotent once the store sits under `target`.
    RekeyArray { array_id: ArrayId, target: ArrayId },

    /// Permanently purge a successfully dropped array's tombstone. This is an
    /// internal all-core operation; failures must be retried before recreation.
    PurgeArrayDrop { array_id: ArrayId },
}

impl ArrayOp {
    /// The array this op targets. For `Elementwise`, returns the left
    /// operand. Both operands share one tile layout and routing, so a
    /// shard holds the matching cells of both.
    pub fn primary_array(&self) -> &ArrayId {
        match self {
            ArrayOp::OpenArray { array_id, .. }
            | ArrayOp::Put { array_id, .. }
            | ArrayOp::Delete { array_id, .. }
            | ArrayOp::Slice { array_id, .. }
            | ArrayOp::SurrogateBitmapScan { array_id, .. }
            | ArrayOp::Aggregate { array_id, .. }
            | ArrayOp::Flush { array_id, .. }
            | ArrayOp::Compact { array_id, .. }
            | ArrayOp::DropArray { array_id }
            | ArrayOp::RekeyArray { array_id, .. }
            | ArrayOp::PurgeArrayDrop { array_id } => array_id,
            ArrayOp::Elementwise { left, .. } => left,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::TenantId;

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "g")
    }

    #[test]
    fn array_op_roundtrips_through_msgpack() {
        let op = ArrayOp::Aggregate {
            array_id: aid(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: false,
            shard_bucket: None,
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
            region_msgpack: vec![1, 2, 3],
            physical: true,
        };
        let bytes = zerompk::to_msgpack_vec(&op).unwrap();
        let back: ArrayOp = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn slice_all_versions_roundtrips_through_msgpack() {
        use nodedb_types::SystemTimeScope;
        let op = ArrayOp::Slice {
            array_id: aid(),
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: SystemTimeScope::AllVersions,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        };
        let bytes = zerompk::to_msgpack_vec(&op).unwrap();
        let back: ArrayOp = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(op, back);
        assert!(
            matches!(
                back,
                ArrayOp::Slice {
                    system_time: SystemTimeScope::AllVersions,
                    ..
                }
            ),
            "round-trip must preserve AllVersions"
        );
    }

    #[test]
    fn slice_as_of_roundtrips_through_msgpack() {
        use nodedb_types::SystemTimeScope;
        let op = ArrayOp::Slice {
            array_id: aid(),
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 10,
            cell_filter: None,
            shard_bucket: None,
            system_time: SystemTimeScope::AsOf(1_700_000_000_000),
            valid_at_ms: Some(1_600_000_000_000),
            zone_msgpack: vec![4, 5, 6],
            physical: true,
        };
        let bytes = zerompk::to_msgpack_vec(&op).unwrap();
        let back: ArrayOp = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn primary_array_picks_left_for_elementwise() {
        let op = ArrayOp::Elementwise {
            left: ArrayId::new(TenantId::new(1), "L"),
            right: ArrayId::new(TenantId::new(1), "R"),
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
        };
        assert_eq!(op.primary_array().name, "L");
    }

    #[test]
    fn binary_op_and_reducer_round_trip() {
        for r in [
            ArrayReducer::Sum,
            ArrayReducer::Count,
            ArrayReducer::Min,
            ArrayReducer::Max,
            ArrayReducer::Mean,
            ArrayReducer::Median,
            ArrayReducer::Percentile { p: 0.9 },
            ArrayReducer::StdDevPop,
            ArrayReducer::StdDevSamp,
            ArrayReducer::VarPop,
            ArrayReducer::VarSamp,
            ArrayReducer::Mode,
            ArrayReducer::Histogram {
                bins: 10,
                lo: 0.0,
                hi: 1.0,
            },
        ] {
            let bytes = zerompk::to_msgpack_vec(&r).unwrap();
            let back: ArrayReducer = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(r, back);
        }
        for o in [
            ArrayBinaryOp::Add,
            ArrayBinaryOp::Sub,
            ArrayBinaryOp::Mul,
            ArrayBinaryOp::Div,
        ] {
            let bytes = zerompk::to_msgpack_vec(&o).unwrap();
            let back: ArrayBinaryOp = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(o, back);
        }
    }
}
