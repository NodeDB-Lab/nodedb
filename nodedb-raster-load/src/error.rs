// SPDX-License-Identifier: Apache-2.0

//! `RasterLoadError`: why a raster load refuses its input, with the
//! SQLSTATE each refusal reports.

use nodedb_raster::SampleType;

/// A refused raster load. Every variant but the per-pixel ones refuses the
/// whole input before any row loads.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RasterLoadError {
    #[error(
        "the file CRS is SRID {file}, array '{array}' GEOREF is SRID {georef}; \
         reproject outside the engine"
    )]
    CrsMismatch {
        array: String,
        file: u32,
        georef: u32,
    },
    #[error("the file GeoKeys name no CRS, array '{array}' GEOREF is SRID {georef}")]
    NoCrs { array: String, georef: u32 },
    #[error("the file has no geotransform, so no pixel has a world position")]
    NoTransform,
    #[error(
        "band {band} holds {sample} samples, which attr '{attr}' ({attr_type}) cannot hold \
         exactly; declare the attr as a type that widens {sample}"
    )]
    BandType {
        band: usize,
        sample: SampleType,
        attr: String,
        attr_type: &'static str,
    },
    #[error("no file band loads into an attr")]
    NoBandLoads,
    #[error(
        "attr '{attr}' is NOT NULL, and no file band loads into it; map a band to it with \
         BANDS (...)"
    )]
    UnmappedNotNull { attr: String },
    #[error(
        "the sources have {bands} bands, array '{array}' has {attrs} attrs; name each band's \
         attr or SKIP with BANDS (...)"
    )]
    BandCount {
        array: String,
        bands: usize,
        attrs: usize,
    },
    #[error(
        "BANDS lists {listed} entries, the sources have {bands} bands; list one entry per band"
    )]
    BandListLength { listed: usize, bands: usize },
    #[error("BANDS names '{name}', which array '{array}' has no attr")]
    UnknownAttr { array: String, name: String },
    #[error("array '{array}' GEOREF names dim '{dim}', which it lacks")]
    GeorefDim { array: String, dim: String },
    #[error(
        "dim '{dim}' has no source in a GeoTIFF; a raster fills the GEOREF axes and one \
         TIMESTAMP dim from TIME"
    )]
    DimWithoutSource { dim: String },
    #[error("TIME is given, and array '{array}' has no TIMESTAMP dim")]
    TimeWithoutDim { array: String },
    #[error("dim '{dim}' is a TIMESTAMP; give the scene instant with WITH (TIME '<iso>')")]
    TimeMissing { dim: String },
    #[error("TIME {time} is outside dim '{dim}' domain [{lo}..{hi}]")]
    TimeOutside {
        time: i64,
        dim: String,
        lo: String,
        hi: String,
    },
    #[error("nodata {value} is not a {sample} value, so no sample can match it")]
    Nodata { value: f64, sample: SampleType },
    #[error("raster grid does not align with array '{array}': {why}; resample outside the engine")]
    Misaligned { array: String, why: String },
    #[error(
        "the raster footprint spans [{lo}..{hi}] on dim '{dim}', outside its domain \
         [{domain_lo}..{domain_hi}]"
    )]
    FootprintOutside {
        dim: String,
        lo: String,
        hi: String,
        domain_lo: String,
        domain_hi: String,
    },
    #[error("GEOREF axis '{dim}' cannot hold world coordinates")]
    AxisNotWorld { dim: String },
    #[error(
        "pixel centres on INT64 axis '{dim}' are not whole numbers; declare it FLOAT64 or \
         resample outside the engine"
    )]
    AxisNotWhole { dim: String },
    #[error(
        "source {index} ('{source_name}') {what} differs from source 1 ('{first}'); every \
         source of one COPY shares its grid and block layout"
    )]
    SourceMismatch {
        index: usize,
        source_name: String,
        first: String,
        what: String,
    },
    #[error(
        "WINDOW ({x0}, {y0}, {x1}, {y1}) lies outside the {width}x{height} image; give a \
         window inside it"
    )]
    WindowOutside {
        x0: u32,
        y0: u32,
        x1: u32,
        y1: u32,
        width: u32,
        height: u32,
    },
    #[error("raster block {index} of source {source_index} {what}")]
    BlockShape {
        index: u32,
        source_index: usize,
        what: String,
    },
    #[error("band {band} has no sample for this pixel")]
    MissingSample { row: usize, band: usize },
    #[error(
        "attr '{attr}' is NOT NULL, and band {band} holds no data at (pixel row {pixel_row}, \
         col {pixel_col})"
    )]
    NodataInNotNull {
        row: usize,
        attr: String,
        band: usize,
        pixel_row: u32,
        pixel_col: u32,
    },
}

impl RasterLoadError {
    /// The SQLSTATE the refusal reports.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::CrsMismatch { .. }
            | Self::NoCrs { .. }
            | Self::NoTransform
            | Self::NoBandLoads
            | Self::BandCount { .. }
            | Self::BandListLength { .. }
            | Self::Nodata { .. }
            | Self::Misaligned { .. }
            | Self::AxisNotWorld { .. }
            | Self::AxisNotWhole { .. }
            | Self::SourceMismatch { .. }
            | Self::WindowOutside { .. } => "22023",
            Self::BandType { .. } => "42804",
            Self::UnmappedNotNull { .. } | Self::NodataInNotNull { .. } => "23502",
            Self::UnknownAttr { .. } => "42703",
            Self::GeorefDim { .. } => "XX000",
            Self::DimWithoutSource { .. } => "0A000",
            Self::TimeWithoutDim { .. } | Self::TimeMissing { .. } => "42601",
            Self::TimeOutside { .. } | Self::FootprintOutside { .. } => "22003",
            Self::BlockShape { .. } | Self::MissingSample { .. } => "22P04",
        }
    }

    /// The 1-based row the refusal names, or 0 when it refuses the whole
    /// input.
    pub fn row(&self) -> usize {
        match self {
            Self::MissingSample { row, .. } | Self::NodataInNotNull { row, .. } => *row,
            Self::CrsMismatch { .. }
            | Self::NoCrs { .. }
            | Self::NoTransform
            | Self::BandType { .. }
            | Self::NoBandLoads
            | Self::UnmappedNotNull { .. }
            | Self::BandCount { .. }
            | Self::BandListLength { .. }
            | Self::UnknownAttr { .. }
            | Self::GeorefDim { .. }
            | Self::DimWithoutSource { .. }
            | Self::TimeWithoutDim { .. }
            | Self::TimeMissing { .. }
            | Self::TimeOutside { .. }
            | Self::Nodata { .. }
            | Self::Misaligned { .. }
            | Self::FootprintOutside { .. }
            | Self::AxisNotWorld { .. }
            | Self::AxisNotWhole { .. }
            | Self::SourceMismatch { .. }
            | Self::WindowOutside { .. }
            | Self::BlockShape { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_pixel_refusals_name_their_row() {
        let e = RasterLoadError::NodataInNotNull {
            row: 7,
            attr: "b1".into(),
            band: 1,
            pixel_row: 1,
            pixel_col: 2,
        };
        assert_eq!((e.row(), e.sqlstate()), (7, "23502"));
        assert!(e.to_string().contains("(pixel row 1, col 2)"));
        assert_eq!(RasterLoadError::NoTransform.row(), 0);
        assert_eq!(RasterLoadError::NoTransform.sqlstate(), "22023");
    }
}
