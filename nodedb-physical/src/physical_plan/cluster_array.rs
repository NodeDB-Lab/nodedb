// SPDX-License-Identifier: Apache-2.0

//! Control-Plane cluster array operations.
//!
//! These variants are emitted by the SQL converter when cluster mode is
//! active and an array operation spans multiple vShards. They are handled
//! exclusively by the Control-Plane dispatch loop (pgwire routing handler)
//! which calls the `ArrayCoordinator` before anything touches the SPSC
//! bridge. They are **never sent to the Data Plane**.
//!
//! The converter emits local `ArrayOp` variants in single-node mode and
//! `ClusterArrayOp` variants in cluster mode. Downstream code that sees
//! a `PhysicalPlan::ClusterArray(_)` knows it must call the coordinator,
//! not the SPSC bridge.

use nodedb_array::types::ArrayId;
use nodedb_types::SystemTimeScope;

use super::array::ArrayBinaryOp;

/// Cluster-mode array operations executed by the coordinator on the
/// Control Plane.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ClusterArrayOp {
    /// Fan-out a slice query across all target vShards and merge rows.
    ///
    /// `slice_tile_keys` — pre-computed tile keys, at least one per routing
    /// bucket the slice touches. They pick the shards to contact. An empty
    /// vec scans every shard.
    ///
    /// `prefix_bits` — routing granularity from the array catalog entry.
    Slice {
        array_id: ArrayId,
        /// zerompk-encoded `Slice` predicate.
        slice_msgpack: Vec<u8>,
        /// Projected attribute indices (empty = all).
        attr_projection: Vec<u32>,
        /// Maximum rows returned. 0 = unlimited.
        limit: u32,
        /// Pre-computed tile keys for shard selection.
        slice_tile_keys: Vec<u64>,
        /// Routing granularity from the array catalog entry.
        prefix_bits: u8,
        /// Bitemporal system-time scope for this slice fan-out.
        ///
        /// - `Current`: live read.
        /// - `AsOf(t)`: point-in-time at `t`.
        /// - `AllVersions`: audit-log fan-out — each shard emits all live
        ///   cell-versions; the coordinator merge-sorts by
        ///   `ArrayCell::system_time` ascending before applying the limit.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point. `None` = no valid-time filter.
        valid_at_ms: Option<i64>,
        /// zerompk-encoded `nodedb_array::query::PolygonZone`, forwarded to
        /// every shard. Empty = no zone.
        zone_msgpack: Vec<u8>,
        /// Read attrs physically. See `ArrayOp::Slice::physical`.
        physical: bool,
    },

    /// Fan-out an aggregate across all target vShards and reduce partials.
    Agg {
        array_id: ArrayId,
        /// Index of the attribute to aggregate.
        attr_idx: u32,
        /// zerompk-encoded `ArrayReducer`.
        reducer_msgpack: Vec<u8>,
        /// Dimension index to group by (-1 = no grouping).
        group_by_dim: i32,
        /// Pre-computed tile keys for shard selection (empty = all).
        slice_tile_keys: Vec<u64>,
        /// Routing granularity.
        prefix_bits: u8,
        /// Bitemporal system-time scope. See `ArrayOp::Aggregate::system_time`.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point. `None` = no valid-time filter.
        valid_at_ms: Option<i64>,
        /// zerompk-encoded `nodedb_array::query::ArrayRegion`, forwarded to
        /// every shard. Empty = the whole array.
        region_msgpack: Vec<u8>,
        /// Reduce physical values. See `ArrayOp::Aggregate::physical`.
        physical: bool,
    },

    /// Fan-out an elementwise op to every primary vShard and concatenate the
    /// rows.
    ///
    /// The planner admits only two arrays with one schema hash and one
    /// `prefix_bits`. Their tile keys then match per coordinate, so each
    /// shard holds both sides of every pair it owns and pairs them on its
    /// Data Plane.
    Elementwise {
        left: ArrayId,
        right: ArrayId,
        op: ArrayBinaryOp,
        /// Index of the attribute the op reads.
        attr_idx: u32,
        /// Routing granularity, shared by both arrays.
        prefix_bits: u8,
        /// Bitemporal system-time scope for both operands. See
        /// `ArrayOp::Aggregate::system_time`.
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point for both operands. `None` = no
        /// valid-time filter.
        valid_at_ms: Option<i64>,
    },

    /// Fan-out a cell write batch, partitioned by routing bucket.
    ///
    /// `cells` — list of `(tile_key, zerompk-encoded single-cell bytes)`.
    /// The coordinator partitions these by the tile key's vShard and sends
    /// each shard's subset via `coord_put`.
    Put {
        array_id: ArrayId,
        /// zerompk-encoded `ArrayId` bytes for the wire request.
        array_id_msgpack: Vec<u8>,
        /// Cells as `(tile_key, cell_msgpack)` pairs.
        cells: Vec<(u64, Vec<u8>)>,
        /// WAL LSN allocated by the Control Plane for this batch.
        wal_lsn: u64,
        /// Routing granularity.
        prefix_bits: u8,
    },

    /// Fan-out a coord delete batch, partitioned by routing bucket.
    ///
    /// `coords` — `(tile_key, zerompk-encoded single-coord bytes)`.
    Delete {
        array_id: ArrayId,
        /// zerompk-encoded `ArrayId` bytes for the wire request.
        array_id_msgpack: Vec<u8>,
        /// Coords as `(tile_key, coord_msgpack)` pairs.
        coords: Vec<(u64, Vec<u8>)>,
        /// WAL LSN.
        wal_lsn: u64,
        /// Routing granularity.
        prefix_bits: u8,
    },

    /// Fan-out a memtable flush to every primary vShard. Each shard's core
    /// stamps the flush with its own WAL frontier.
    Flush {
        array_id: ArrayId,
        /// Routing granularity.
        prefix_bits: u8,
    },

    /// Fan-out a compaction to every primary vShard.
    ///
    /// `audit_retain_ms` is the array's retention window from the catalog.
    /// `None` keeps every version.
    Compact {
        array_id: ArrayId,
        audit_retain_ms: Option<i64>,
        /// Routing granularity.
        prefix_bits: u8,
    },
}

impl ClusterArrayOp {
    /// The array this op targets. Total over every variant, so read-set
    /// tracking and the commit validator key on the same name a shard-local
    /// `ArrayOp` reports. For `Elementwise`, the left operand.
    pub fn array_id(&self) -> &ArrayId {
        match self {
            ClusterArrayOp::Slice { array_id, .. }
            | ClusterArrayOp::Agg { array_id, .. }
            | ClusterArrayOp::Put { array_id, .. }
            | ClusterArrayOp::Delete { array_id, .. }
            | ClusterArrayOp::Flush { array_id, .. }
            | ClusterArrayOp::Compact { array_id, .. } => array_id,
            ClusterArrayOp::Elementwise { left, .. } => left,
        }
    }
}
