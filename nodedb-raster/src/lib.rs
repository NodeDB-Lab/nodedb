// SPDX-License-Identifier: Apache-2.0

//! Sans-IO raster decoding for NodeDB array bulk loads.
//!
//! A [`RasterOpener`] reads a raster header from a [`RangeCache`] and
//! names the byte ranges it needs next, so any transport (a local file,
//! HTTP range requests, an object store) can feed it. The resulting
//! [`RasterReader`] names each block's ranges and decodes the block from
//! the fetched bytes, under the caller's [`RasterLimits`]. The synchronous
//! driver in [`reader::driver`] answers both from a blocking source.
//!
//! [`TiffOpener`] reads classic TIFF and BigTIFF, cloud-optimised or not:
//! strips or tiles, chunky or planar bands, overviews, internal masks,
//! GeoKeys, `GDAL_NODATA` and `GDAL_METADATA`. Its codecs are pure Rust:
//! none, LZW, deflate, PackBits, ZSTD, JPEG and LERC, with the horizontal
//! and floating-point predictors.

pub mod block;
pub mod codec;
pub mod crs;
pub mod error;
pub mod header;
pub mod layout;
pub mod limits;
pub mod nodata;
pub mod reader;
pub mod sample;
pub mod source;
pub mod tiff;

pub use block::{BandSamples, RasterBlock};
pub use crs::srid_from_geokeys;
pub use error::RasterError;
pub use header::{BandInfo, CrsRef, OuterDim, RasterHeader, UserDefinedCrs};
pub use layout::{BlockKind, BlockWindow, Interleave, RasterLayout};
pub use limits::{CapacityExceeded, DEFAULT_HEADER_BYTES, MAX_DECODE_CAPACITY, RasterLimits};
pub use nodata::parse_gdal_nodata;
pub use nodedb_crs::GeoTransform;
pub use reader::{
    Opened, RangeRead, RasterOpener, RasterReader, SliceSource, open_all, open_with,
    read_block_all, read_block_with,
};
pub use sample::{SampleFormatCode, SampleType};
pub use source::{ByteRange, COALESCE_GAP, Lookup, ObjectId, RangeCache, coalesce};
pub use tiff::geokeys::{GeoKeyDirectory, GeoKeyError, GeoKeyValue};
pub use tiff::transform::{ModelTags, transform_from_tags};
pub use tiff::{TiffOpener, TiffReader};
