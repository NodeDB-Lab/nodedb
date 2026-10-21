// SPDX-License-Identifier: Apache-2.0

//! The TIFF and BigTIFF container: header, IFD chain, tags, block index,
//! internal masks, and the GeoTIFF and GDAL tags.

pub mod byte_order;
pub mod chain;
pub mod decode;
pub mod error;
pub mod fetch;
pub mod file_header;
pub mod gdal_metadata;
pub mod geo;
pub mod geokeys;
pub mod ifd;
pub mod image;
pub mod mask;
pub mod opener;
pub mod reader;
pub mod samples;
pub mod tags;
pub mod tile_index;
pub mod transform;
pub mod values;

pub use opener::TiffOpener;
pub use reader::TiffReader;
