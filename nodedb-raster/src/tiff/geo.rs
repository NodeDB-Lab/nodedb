// SPDX-License-Identifier: Apache-2.0

//! The georeference and band metadata of the first image: model tags,
//! GeoKeys, `GDAL_NODATA` and `GDAL_METADATA`.

use std::collections::BTreeMap;

use nodedb_crs::GeoTransform;

use crate::crs::srid::crs_ref_from_geokeys;
use crate::error::RasterError;
use crate::header::{BandInfo, CrsRef};
use crate::nodata::parse_gdal_nodata;
use crate::sample::SampleType;
use crate::tiff::gdal_metadata::{BandMetadata, parse_gdal_metadata};
use crate::tiff::geokeys::{GT_RASTER_TYPE, GeoKeyDirectory};
use crate::tiff::tags::{
    GDAL_METADATA, GDAL_NODATA, GEO_ASCII_PARAMS, GEO_DOUBLE_PARAMS, GEO_KEY_DIRECTORY,
    MODEL_PIXEL_SCALE, MODEL_TIEPOINT, MODEL_TRANSFORMATION,
};
use crate::tiff::transform::{ModelTags, transform_from_tags};
use crate::tiff::values::TagValues;

/// What the first image says about georeference and bands.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoInfo {
    pub transform: Option<GeoTransform>,
    pub geokeys: Option<GeoKeyDirectory>,
    pub crs: CrsRef,
    /// `GDAL_NODATA`, shared by every band.
    pub nodata: Option<f64>,
    pub metadata: BTreeMap<u32, BandMetadata>,
}

impl GeoInfo {
    /// Read the georeference tags of `values`, the first image's IFD.
    pub fn read(values: &TagValues<'_>) -> Result<Self, RasterError> {
        let geokeys = read_geokeys(values)?;
        let pixel_scale = values.f64s(MODEL_PIXEL_SCALE)?;
        let tiepoints = values.f64s(MODEL_TIEPOINT)?;
        let transformation = values.f64s(MODEL_TRANSFORMATION)?;
        let transform = transform_from_tags(ModelTags {
            pixel_scale: pixel_scale.as_deref(),
            tiepoints: tiepoints.as_deref(),
            transformation: transformation.as_deref(),
            raster_type: geokeys.as_ref().and_then(|keys| keys.short(GT_RASTER_TYPE)),
        })?;
        let crs = crs_ref_from_geokeys(geokeys.as_ref())?;
        let nodata = values
            .ascii(GDAL_NODATA)?
            .map(|text| parse_gdal_nodata(&text))
            .transpose()?;
        let metadata = match values.ascii(GDAL_METADATA)? {
            Some(xml) => parse_gdal_metadata(&xml)?,
            None => BTreeMap::new(),
        };
        Ok(Self {
            transform,
            geokeys,
            crs,
            nodata,
            metadata,
        })
    }

    /// The band list of an image with `bands` bands of `sample_type`.
    pub fn bands(&self, bands: u16, sample_type: SampleType) -> Vec<BandInfo> {
        (0..u32::from(bands))
            .map(|band| {
                let meta = self.metadata.get(&band);
                BandInfo {
                    name: meta.and_then(|m| m.name.clone()),
                    sample_type,
                    nodata: meta.and_then(|m| m.nodata).or(self.nodata),
                    scale: meta.and_then(|m| m.scale),
                    offset: meta.and_then(|m| m.offset),
                    valid_range: meta.and_then(BandMetadata::valid_range),
                    units: meta.and_then(|m| m.units.clone()),
                }
            })
            .collect()
    }
}

/// The GeoKey directory with its parameter arrays, when the file has one.
fn read_geokeys(values: &TagValues<'_>) -> Result<Option<GeoKeyDirectory>, RasterError> {
    let Some(directory) = values.u16s(GEO_KEY_DIRECTORY)? else {
        return Ok(None);
    };
    let doubles = values.f64s(GEO_DOUBLE_PARAMS)?;
    let ascii = values.ascii(GEO_ASCII_PARAMS)?;
    let keys = GeoKeyDirectory::parse(&directory, doubles.as_deref(), ascii.as_deref())?;
    Ok(Some(keys))
}
