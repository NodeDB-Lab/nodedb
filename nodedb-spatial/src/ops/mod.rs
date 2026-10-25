// SPDX-License-Identifier: Apache-2.0

//! Geometry operations: boolean overlay, buffer, DE-9IM, simplification,
//! hulls, repair, linear referencing, subdivision, splitting and tiles.

pub mod boolean;
pub mod buffer;
pub mod closest;
pub(crate) mod convert;
pub mod hull;
pub mod linear;
pub(crate) mod noding;
pub(crate) mod pieces;
pub mod relate;
pub mod repair;
pub mod simplify;
pub mod split;
pub mod subdivide;
pub mod tile;

pub use boolean::{difference, intersection, sym_difference, union};
pub use buffer::buffer;
pub use closest::closest_point;
pub use hull::{Hull, hull};
pub use linear::{
    LinearError, interpolate_point, line_interpolate_point, locate_along, locate_between,
    segmentize,
};
pub use relate::{Relation, relate, relate_pattern, relates};
pub use repair::make_valid;
pub use simplify::{Simplifier, simplify};
pub use split::split;
pub use subdivide::{MIN_SUBDIVIDE_VERTICES, subdivide};
pub use tile::{
    MAX_TILE_ZOOM, TileError, WEB_MERCATOR_HALF_SIDE, shift_longitude, tile_envelope,
    web_mercator_bounds,
};
