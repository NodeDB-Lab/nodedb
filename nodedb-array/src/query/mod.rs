// SPDX-License-Identifier: Apache-2.0

//! Query operators over tiles.
//!
//! Pure, tile-local primitives: slice (coord-range filter), project
//! (attr subset), region (coordinate box plus polygon zone), aggregate
//! (exact reducers with optional group-by + cross-tile merge),
//! elementwise (binary ops between two aligned tiles) and rechunk
//! (re-bucket cells into a new tile-extent layout). No engine state, no plane crossings — the executor wires these
//! into the physical plan.

pub mod agg_number;
pub mod aggregate;
pub mod ceiling;
pub mod elementwise;
pub mod physical;
pub mod project;
pub mod rechunk;
pub mod region;
pub mod retention;
pub mod slice;
pub mod zone_frame;

pub use agg_number::{AggNumber, AggScalar};
pub use aggregate::{
    AggOutput, AggRead, AggregateResult, GroupAggregate, Reducer, aggregate_attr, group_by_dim,
};
pub use ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
pub use elementwise::{BinaryOp, elementwise};
pub use physical::{physical_cells, physical_sparse};
pub use project::{Projection, project_sparse};
pub use rechunk::rechunk_sparse;
pub use region::{
    ArrayRegion, PolygonZone, PreparedRegion, PreparedZone, ZonePolygon, filter_region_owned,
};
pub use retention::{RetentionMergeResult, merge_for_retention};
pub use slice::{
    DimRange, Slice, filter_sparse, slice_sparse, slice_sparse_owned, tile_overlaps_slice,
    tile_within_slice,
};
pub use zone_frame::{
    MAX_BISECTION_DEPTH, MAX_FRAME_VERTICES, ZoneFrame, ZoneFrameError, build_zone_frame,
};
