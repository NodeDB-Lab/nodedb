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
    /// `slice_hilbert_ranges` — pre-computed `(lo, hi)` Hilbert-prefix
    /// ranges that determine which shards to contact. An empty vec means
    /// "scan all shards".
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
        /// Pre-computed Hilbert-prefix ranges for shard selection.
        slice_hilbert_ranges: Vec<(u64, u64)>,
        /// Routing granularity from the array catalog entry.
        prefix_bits: u8,
        /// Bitemporal system-time cutoff. `None` = live read.
        system_as_of: Option<i64>,
        /// Bitemporal valid-time point. `None` = no valid-time filter.
        valid_at_ms: Option<i64>,
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
        /// Pre-computed Hilbert ranges for shard selection (empty = all).
        slice_hilbert_ranges: Vec<(u64, u64)>,
        /// Routing granularity.
        prefix_bits: u8,
        /// Bitemporal system-time cutoff. `None` = live read.
        system_as_of: Option<i64>,
        /// Bitemporal valid-time point. `None` = no valid-time filter.
        valid_at_ms: Option<i64>,
    },

    /// Fan-out a cell write batch, partitioned by Hilbert tile.
    ///
    /// `cells` — list of `(hilbert_prefix, zerompk-encoded single-cell
    /// bytes)`. The coordinator partitions these by prefix and sends each
    /// shard's subset via `coord_put`.
    Put {
        array_id: ArrayId,
        /// zerompk-encoded `ArrayId` bytes for the wire request.
        array_id_msgpack: Vec<u8>,
        /// Cells as `(hilbert_prefix, cell_msgpack)` pairs.
        cells: Vec<(u64, Vec<u8>)>,
        /// WAL LSN allocated by the Control Plane for this batch.
        wal_lsn: u64,
        /// Routing granularity.
        prefix_bits: u8,
    },

    /// Fan-out a coord delete batch, partitioned by Hilbert tile.
    ///
    /// `coords` — `(hilbert_prefix, zerompk-encoded single-coord bytes)`.
    Delete {
        array_id: ArrayId,
        /// zerompk-encoded `ArrayId` bytes for the wire request.
        array_id_msgpack: Vec<u8>,
        /// Coords as `(hilbert_prefix, coord_msgpack)` pairs.
        coords: Vec<(u64, Vec<u8>)>,
        /// WAL LSN.
        wal_lsn: u64,
        /// Routing granularity.
        prefix_bits: u8,
    },
}
