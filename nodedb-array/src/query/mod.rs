//! Tier 4 query operators over tiles.
//!
//! Pure, tile-local primitives: slice (coord-range filter), project
//! (attr subset), aggregate (sum/count/min/max/mean with optional
//! group-by + cross-tile merge), elementwise (binary ops between two
//! aligned tiles) and rechunk (re-bucket cells into a new tile-extent
//! layout). No engine state, no plane crossings — Tier 5 wires these
//! into the physical plan.

pub mod aggregate;
pub mod elementwise;
pub mod project;
pub mod rechunk;
pub mod slice;

pub use aggregate::{AggregateResult, GroupAggregate, Reducer, aggregate_attr, group_by_dim};
pub use elementwise::{BinaryOp, elementwise};
pub use project::{Projection, project_sparse};
pub use rechunk::rechunk_sparse;
pub use slice::{DimRange, Slice, slice_sparse, tile_overlaps_slice};
