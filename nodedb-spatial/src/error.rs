// SPDX-License-Identifier: Apache-2.0

//! The error every fallible spatial operation returns.

use nodedb_crs::{CrsError, UnknownSrid};
use nodedb_types::geometry::{GeoJsonError, OrdinateError};

use crate::ops::linear::LinearError;
use crate::ops::tile::TileError;
use crate::wkb::WkbError;
use crate::wkt::WktError;

/// Why a geometry does not parse, or an operation on it fails.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SpatialError {
    #[error(transparent)]
    Wkt(#[from] WktError),
    #[error(transparent)]
    Wkb(#[from] WkbError),
    #[error(transparent)]
    GeoJson(#[from] GeoJsonError),
    #[error("invalid ordinates: {0}")]
    Ordinates(#[from] OrdinateError),
    /// An SRID code no CRS defines, or outside the code range.
    #[error(transparent)]
    Srid(#[from] UnknownSrid),
    /// A local projection or a transform fails.
    #[error(transparent)]
    Crs(#[from] CrsError),
    /// `ST_MakeValid` cannot repair the input.
    #[error("cannot repair geometry: {reason}")]
    Repair { reason: Box<str> },
    /// A DE-9IM pattern is not 9 characters of `0 1 2 T F *`.
    #[error("invalid DE-9IM pattern '{pattern}': {reason}")]
    Pattern { pattern: Box<str>, reason: Box<str> },
    #[error(transparent)]
    Linear(#[from] LinearError),
    #[error(transparent)]
    Tile(#[from] TileError),
    /// A value of a type no geometry form takes.
    #[error("a {found} value is not a geometry")]
    NotGeometryValue { found: &'static str },
    /// The operation has no meaning for these geometry kinds.
    #[error("{operation} does not take {kinds}")]
    Unsupported {
        operation: &'static str,
        kinds: Box<str>,
    },
}
