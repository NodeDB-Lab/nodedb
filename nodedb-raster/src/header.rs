// SPDX-License-Identifier: Apache-2.0

//! What a raster's header says: grid size, outer dims, bands, georeference.

use nodedb_crs::{CrsContext, GeoTransform};
use nodedb_types::geometry::Srid;

use crate::crs::srid::resolve_crs;
use crate::error::RasterError;
use crate::sample::SampleType;
use crate::tiff::geokeys::GeoKeyDirectory;

/// A dimension outside the 2D grid, such as time or a band axis of a
/// multi-dimensional format. A TIFF has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterDim {
    pub name: String,
    pub len: u64,
}

/// One band: its sample type and the value metadata the file declares.
#[derive(Debug, Clone, PartialEq)]
pub struct BandInfo {
    /// The band name or description, when the file gives one.
    pub name: Option<String>,
    pub sample_type: SampleType,
    /// The sample value that means "no data".
    pub nodata: Option<f64>,
    /// Physical value = `sample * scale + offset`.
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    /// Inclusive valid sample range `(min, max)`.
    pub valid_range: Option<(f64, f64)>,
    /// The unit of the physical value.
    pub units: Option<String>,
}

impl BandInfo {
    /// A band of `sample_type` with no metadata.
    pub const fn plain(sample_type: SampleType) -> Self {
        Self {
            name: None,
            sample_type,
            nodata: None,
            scale: None,
            offset: None,
            valid_range: None,
            units: None,
        }
    }
}

/// A user-defined GeoTIFF CRS (GeoKey value 32767): the directory its
/// parameters decode from.
#[derive(Debug, Clone, PartialEq)]
pub struct UserDefinedCrs {
    pub keys: GeoKeyDirectory,
    /// True for a projected CRS, false for a geographic one.
    pub projected: bool,
}

/// The CRS a raster names, before it resolves against a CRS context.
#[derive(Debug, Clone, PartialEq)]
pub enum CrsRef {
    /// The file names no CRS.
    None,
    /// An EPSG code.
    Epsg(u32),
    /// A CRS defined by parameters.
    UserDefined(Box<UserDefinedCrs>),
}

impl CrsRef {
    /// The SRID this CRS resolves to in `ctx`, or `None` when the file
    /// names no CRS.
    pub fn resolve(&self, ctx: &CrsContext<'_>) -> Result<Option<Srid>, RasterError> {
        resolve_crs(self, ctx)
    }
}

/// The header of one raster image.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterHeader {
    pub width: u32,
    pub height: u32,
    /// Dims outside the 2D grid, outermost first.
    pub outer: Vec<OuterDim>,
    pub bands: Vec<BandInfo>,
    /// The GDAL-order pixel-corner geotransform. `None` when the file has
    /// none.
    pub transform: Option<GeoTransform>,
    pub crs: CrsRef,
}

impl RasterHeader {
    /// The band count.
    pub fn band_count(&self) -> usize {
        self.bands.len()
    }
}
