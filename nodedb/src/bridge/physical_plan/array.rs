//! Array engine operations dispatched to the Data Plane.
//!
//! `ArrayOp` is the wire type for array query operators (slice,
//! project, aggregate, elementwise) plus the write-side ops (put,
//! delete) and engine maintenance (flush, compact). Complex nested
//! payloads — schemas, slice predicates, cell batches — ride as
//! opaque msgpack bytes (`*_msgpack`) decoded on the Data Plane via
//! zerompk against the canonical type from `nodedb-array`. This keeps
//! the bridge enum flat and zerompk-derivable while preserving full
//! type fidelity at the engine boundary.

use nodedb_array::types::ArrayId;
use nodedb_types::SurrogateBitmap;

/// Reducer for [`ArrayOp::Aggregate`]. Numeric `c_enum` keeps the
/// wire encoding to a single byte.
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
pub enum ArrayReducer {
    Sum,
    Count,
    Min,
    Max,
    Mean,
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
        /// Hilbert-prefix bits for vShard routing. Immutable post-create.
        prefix_bits: u8,
    },

    /// Insert one or more cells. `cells_msgpack` is an zerompk
    /// encoding of `Vec<nodedb::engine::array::wal::ArrayPutCell>`.
    /// Each cell carries a Control-Plane-allocated `Surrogate` parallel
    /// to its `coord` tuple — cross-engine bitmap joins read the
    /// surrogate column on read paths instead of translating coords
    /// back to user-visible PKs.
    Put {
        array_id: ArrayId,
        cells_msgpack: Vec<u8>,
        wal_lsn: u64,
    },

    /// Delete by exact coordinates. `coords_msgpack` is an zerompk
    /// encoding of `Vec<Vec<CoordValue>>`.
    Delete {
        array_id: ArrayId,
        coords_msgpack: Vec<u8>,
        wal_lsn: u64,
    },

    /// Coord-range slice with optional attribute projection.
    /// `slice_msgpack` is an zerompk encoding of
    /// `nodedb_array::query::Slice`. Empty `attr_projection` means
    /// "all attributes".
    Slice {
        array_id: ArrayId,
        slice_msgpack: Vec<u8>,
        attr_projection: Vec<u32>,
        limit: u32,
        /// Optional surrogate prefilter restricting which cells are returned.
        /// Only cells whose surrogate is present in this bitmap are included.
        /// `None` = no restriction.
        cell_filter: Option<SurrogateBitmap>,
        /// Optional Hilbert-prefix range `[lo, hi]`. When set, only cells
        /// whose Hilbert prefix falls within this range are included.
        /// Used by distributed shard slice to prevent duplicate rows in
        /// single-node harnesses where all vShards share one Data Plane.
        /// `None` = no Hilbert filter (all cells included).
        hilbert_range: Option<(u64, u64)>,
    },

    /// Attribute projection (no coord filter).
    Project {
        array_id: ArrayId,
        attr_indices: Vec<u32>,
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
        /// Optional Hilbert-prefix range `[lo, hi]`. When set, only cells
        /// whose Hilbert prefix falls within this range are included.
        /// Used by distributed shard agg to prevent double-counting in
        /// single-node harnesses where all vShards share one Data Plane.
        /// `None` = no Hilbert filter (all cells included).
        hilbert_range: Option<(u64, u64)>,
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
    },

    /// Force a memtable flush. Returns the new segment ref's id +
    /// flush_lsn in the response payload. The Control Plane allocates
    /// `wal_lsn` from the central WAL writer and the engine stamps it
    /// as the segment's flush watermark.
    Flush { array_id: ArrayId, wal_lsn: u64 },

    /// Trigger compaction if the picker selects one. Response
    /// indicates whether a merge happened.
    Compact { array_id: ArrayId },

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

    /// Drop the per-core array store. Broadcast on `DROP ARRAY` after
    /// the Control-Plane catalog has been mutated so each Data-Plane
    /// core releases its local store; otherwise a follow-up
    /// `CREATE ARRAY` of the same name could carry stale memtable /
    /// segment state. Idempotent: silently succeeds when no store is
    /// open on the receiving core.
    DropArray { array_id: ArrayId },
}

impl ArrayOp {
    /// The array this op targets. For `Elementwise`, returns the
    /// left operand — vshard routing pins to the left array's
    /// shard and the right is fetched cross-shard.
    pub fn primary_array(&self) -> &ArrayId {
        match self {
            ArrayOp::OpenArray { array_id, .. }
            | ArrayOp::Put { array_id, .. }
            | ArrayOp::Delete { array_id, .. }
            | ArrayOp::Slice { array_id, .. }
            | ArrayOp::SurrogateBitmapScan { array_id, .. }
            | ArrayOp::Project { array_id, .. }
            | ArrayOp::Aggregate { array_id, .. }
            | ArrayOp::Flush { array_id, .. }
            | ArrayOp::Compact { array_id }
            | ArrayOp::DropArray { array_id } => array_id,
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
            hilbert_range: None,
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
        };
        assert_eq!(op.primary_array().name, "L");
    }

    #[test]
    fn binary_op_and_reducer_are_c_enum_one_byte() {
        // c_enum encodes as a single u8 — confirm round-trip preserves identity.
        for r in [
            ArrayReducer::Sum,
            ArrayReducer::Count,
            ArrayReducer::Min,
            ArrayReducer::Max,
            ArrayReducer::Mean,
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
