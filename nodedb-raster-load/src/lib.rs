// SPDX-License-Identifier: Apache-2.0

//! Raster-to-array load planning for NodeDB.
//!
//! A load opens one or more raster sources with `nodedb-raster`, describes
//! each with a [`SourceHeader`], and checks that every source shares one
//! grid and block layout ([`check_same_layout`]). [`RasterPlan::new`] runs
//! every open-time check against the array: CRS, band-to-attr types, grid
//! alignment, the dim domains, `TIME`, nodata and the pixel window. For
//! each block index the caller reads that block from every source, and
//! [`BlockRows`] turns the blocks into array rows. [`adopted_metadata`]
//! names the attr metadata `ADOPT_METADATA` takes from the files.
//!
//! The crate does no I/O, so a server and an embedded engine share it.

pub mod bands;
pub mod coords;
pub mod error;
pub mod grid;
pub mod metadata;
pub mod nodata;
pub mod options;
pub mod plan;
pub mod rows;
pub mod source;
pub mod view;
pub mod window;

pub use bands::{AttrSource, BandMap, map_bands};
pub use coords::{CoordSource, plan_coords};
pub use error::RasterLoadError;
pub use grid::{CellGrid, GridInput, plan_grid};
pub use metadata::adopted_metadata;
pub use nodata::{Nodata, typed_nodata};
pub use options::{BandTarget, LoadOptions};
pub use plan::{PlanBand, RasterPlan};
pub use rows::BlockRows;
pub use source::{LayoutError, SourceHeader, check_same_layout};
pub use view::{
    AdoptedMeta, ArrayView, AttrLiteral, AttrMeta, AttrType, CoordLiteral, DimType, DomainBound,
    LoadRow, ViewAttr, ViewDim, ViewGeoref,
};
pub use window::PixelWindow;
