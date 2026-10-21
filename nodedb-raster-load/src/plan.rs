// SPDX-License-Identifier: Apache-2.0

//! The open-time checks of a raster load. Every check runs before the
//! first block decodes, so a refused input commits no row.
//!
//! - The file CRS of every source must be the array GEOREF SRID (`22023`).
//!   The server refuses an array with no GEOREF (`55000`) before planning.
//! - Bands map onto attrs by the lossless type rule (`42804`), and an attr
//!   no band fills must be nullable (`23502`).
//! - The file grid must align with the array (`22023`), and the loaded
//!   footprint must fit the dim domains (`22003`).
//! - `TIME` fills the one `TIMESTAMP` dim. Any other dim has no source.
//! - The nodata value must be a value of the band's sample type (`22023`).
//!
//! Nodata precedence: the COPY `NODATA` option, then the file's nodata of
//! the band (`GDAL_NODATA`, or a `GDAL_METADATA` band item), then the
//! `NODATA` metadata of the attr the band loads into. The COPY option
//! applies to every band. An attr nodata no sample can equal matches no
//! pixel.

use nodedb_raster::{BlockWindow, SampleType};

use crate::bands::{AttrSource, map_bands};
use crate::coords::{CoordSource, plan_coords};
use crate::error::RasterLoadError;
use crate::grid::{CellGrid, GridInput, plan_grid};
use crate::nodata::{Nodata, typed_nodata};
use crate::options::LoadOptions;
use crate::source::SourceHeader;
use crate::view::{ArrayView, ViewDim, ViewGeoref};
use crate::window::PixelWindow;

/// Where one band of the load comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanBand {
    /// The source index, in statement order.
    pub source: usize,
    /// The band index within the source.
    pub band: usize,
    pub sample: SampleType,
}

/// How every pixel of an accepted input becomes a row.
#[derive(Debug)]
pub struct RasterPlan {
    /// The image width, for row numbers.
    pub(crate) width: u32,
    /// The pixels that load.
    pub(crate) window: PixelWindow,
    /// Every source's bands in source order.
    pub(crate) bands: Vec<PlanBand>,
    /// Bands per source.
    pub(crate) source_bands: Vec<usize>,
    /// One source per dim, in schema order.
    pub(crate) coords: Vec<CoordSource>,
    /// One source per attr, in schema order.
    pub(crate) attrs: Vec<AttrSource>,
    /// The bands that load. A pixel is skipped when none of them holds a
    /// value.
    pub(crate) loaded: Vec<usize>,
    pub(crate) grid: CellGrid,
    /// The nodata of each band, by band index. `None` matches no sample.
    pub(crate) band_nodata: Vec<Option<Nodata>>,
}

impl RasterPlan {
    /// Run every open-time check of `sources` against `view`. `sources`
    /// share one grid, which [`crate::source::check_same_layout`] checks.
    pub fn new(
        view: &ArrayView,
        georef: &ViewGeoref,
        sources: &[SourceHeader],
        options: &LoadOptions,
    ) -> Result<Self, RasterLoadError> {
        let first = sources.first().ok_or(RasterLoadError::NoBandLoads)?;
        for source in sources {
            check_srid(view, georef, source)?;
        }
        let bands: Vec<PlanBand> = sources
            .iter()
            .enumerate()
            .flat_map(|(source, header)| {
                header
                    .bands
                    .iter()
                    .enumerate()
                    .map(move |(band, info)| PlanBand {
                        source,
                        band,
                        sample: info.sample_type,
                    })
            })
            .collect();
        let samples: Vec<SampleType> = bands.iter().map(|band| band.sample).collect();
        let map = map_bands(view, &samples, options.bands.as_deref())?;
        let (x_index, x_dim) = georef_axis(view, &georef.x_dim)?;
        let (y_index, y_dim) = georef_axis(view, &georef.y_dim)?;
        let file_gt = first.transform.ok_or(RasterLoadError::NoTransform)?;
        let window = PixelWindow::of(options.window, first.width, first.height)?;
        let grid = plan_grid(&GridInput {
            array: &view.name,
            x_dim,
            y_dim,
            array_gt: georef.transform,
            file_gt,
            window,
        })?;
        let coords = plan_coords(view, x_index, y_index, options.time_ms)?;
        let band_nodata = band_nodata(view, sources, &bands, options, &map.attrs)?;
        Ok(Self {
            width: first.width,
            window,
            source_bands: sources.iter().map(|s| s.bands.len()).collect(),
            bands,
            coords,
            attrs: map.attrs,
            loaded: map.loaded,
            grid,
            band_nodata,
        })
    }

    /// The pixels that load.
    pub fn window(&self) -> PixelWindow {
        self.window
    }

    /// Whether block `window` holds a pixel that loads. A load reads only
    /// those blocks.
    pub fn wants_block(&self, window: &BlockWindow) -> bool {
        self.window.overlaps(window)
    }

    /// The sources the plan reads, in statement order.
    pub fn source_count(&self) -> usize {
        self.source_bands.len()
    }

    /// Where band `band` of the load comes from.
    pub fn band(&self, band: usize) -> Option<PlanBand> {
        self.bands.get(band).copied()
    }

    /// The attr sources, in schema order.
    pub fn attrs(&self) -> &[AttrSource] {
        &self.attrs
    }
}

/// The nodata of each band. The COPY option applies to every band. Without
/// it, a band takes the file's nodata of the band, else the `NODATA` of the
/// attr it loads into. The COPY option and the file nodata must be sample
/// values.
fn band_nodata(
    view: &ArrayView,
    sources: &[SourceHeader],
    bands: &[PlanBand],
    options: &LoadOptions,
    attrs: &[AttrSource],
) -> Result<Vec<Option<Nodata>>, RasterLoadError> {
    if let Some(value) = options.nodata {
        return bands
            .iter()
            .map(|band| typed_nodata(value, band.sample).map(Some))
            .collect();
    }
    let mut out = vec![None; bands.len()];
    for (attr, source) in view.attrs.iter().zip(attrs) {
        if let (AttrSource::Band { band, .. }, Some(value)) = (source, attr.meta.nodata)
            && let (Some(slot), Some(plan)) = (out.get_mut(*band), bands.get(*band))
        {
            *slot = typed_nodata(value, plan.sample).ok();
        }
    }
    for (slot, plan) in out.iter_mut().zip(bands) {
        let file = sources
            .get(plan.source)
            .and_then(|source| source.bands.get(plan.band))
            .and_then(|info| info.nodata);
        if let Some(value) = file {
            *slot = Some(typed_nodata(value, plan.sample)?);
        }
    }
    Ok(out)
}

fn check_srid(
    view: &ArrayView,
    georef: &ViewGeoref,
    source: &SourceHeader,
) -> Result<(), RasterLoadError> {
    match source.srid {
        Some(code) if code == georef.srid => Ok(()),
        Some(code) => Err(RasterLoadError::CrsMismatch {
            array: view.name.clone(),
            file: code,
            georef: georef.srid,
        }),
        None => Err(RasterLoadError::NoCrs {
            array: view.name.clone(),
            georef: georef.srid,
        }),
    }
}

/// The index and spec of the dim a GEOREF axis names.
fn georef_axis<'v>(
    view: &'v ArrayView,
    name: &str,
) -> Result<(usize, &'v ViewDim), RasterLoadError> {
    view.dims
        .iter()
        .enumerate()
        .find(|(_, dim)| dim.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| RasterLoadError::GeorefDim {
            array: view.name.clone(),
            dim: name.to_string(),
        })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::options::BandTarget;
    use crate::view::{AttrType, DimType, DomainBound, ViewAttr};
    use nodedb_raster::GeoTransform;
    use nodedb_raster::{BandInfo, CrsRef, RasterHeader};

    pub(crate) const GT: [f64; 6] = [600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];

    fn dim(name: &str) -> ViewDim {
        ViewDim {
            name: name.into(),
            dtype: DimType::Int64,
            lo: DomainBound::Int64(0),
            hi: DomainBound::Int64(1_000),
        }
    }

    pub(crate) fn view() -> ArrayView {
        let attr = |name: &str, dtype| ViewAttr {
            name: name.into(),
            dtype,
            nullable: true,
            meta: Default::default(),
        };
        ArrayView {
            name: "scene".into(),
            dims: vec![dim("row"), dim("col")],
            attrs: vec![
                attr("red", AttrType::UInt16),
                attr("nir", AttrType::UInt16),
                attr("qa", AttrType::UInt16),
            ],
            georef: None,
        }
    }

    pub(crate) fn georef() -> ViewGeoref {
        ViewGeoref {
            srid: 32_647,
            x_dim: "col".into(),
            y_dim: "row".into(),
            transform: Some(GeoTransform(GT)),
        }
    }

    pub(crate) fn source(name: &str, info: BandInfo) -> SourceHeader {
        let header = RasterHeader {
            width: 256,
            height: 192,
            outer: Vec::new(),
            bands: vec![info],
            transform: Some(GeoTransform(GT)),
            crs: CrsRef::Epsg(32_647),
        };
        SourceHeader::new(name, &header, Some(32_647))
    }

    fn u16_band() -> BandInfo {
        BandInfo::plain(SampleType::U16)
    }

    #[test]
    fn three_single_band_sources_fill_three_attrs() {
        let sources = [
            source("/B4.TIF", u16_band()),
            source("/B5.TIF", u16_band()),
            source("/QA.TIF", u16_band()),
        ];
        let options = LoadOptions {
            bands: Some(vec![
                BandTarget::Attr("red".into()),
                BandTarget::Attr("nir".into()),
                BandTarget::Attr("qa".into()),
            ]),
            ..Default::default()
        };
        let plan = RasterPlan::new(&view(), &georef(), &sources, &options).expect("plans");
        assert_eq!(plan.source_count(), 3);
        assert_eq!(plan.loaded, vec![0, 1, 2]);
        assert_eq!(
            plan.band(2),
            Some(PlanBand {
                source: 2,
                band: 0,
                sample: SampleType::U16
            })
        );
    }

    #[test]
    fn a_source_in_another_crs_is_refused() {
        let mut other = source("/B5.TIF", u16_band());
        other.srid = Some(4326);
        let sources = [source("/B4.TIF", u16_band()), other];
        let err = RasterPlan::new(&view(), &georef(), &sources, &Default::default())
            .expect_err("4326 against 32647");
        assert_eq!(err.sqlstate(), "22023");
    }

    #[test]
    fn file_nodata_and_the_window_shape_the_plan() {
        let mut info = u16_band();
        info.nodata = Some(0.0);
        let sources = [source("/B4.TIF", info)];
        let options = LoadOptions {
            window: Some(PixelWindow {
                col0: 64,
                row0: 0,
                col1: 128,
                row1: 64,
            }),
            ..Default::default()
        };
        let plan = RasterPlan::new(&view(), &georef(), &sources, &options).expect("plans");
        assert_eq!(plan.band_nodata, vec![Some(Nodata::Int(0))]);
        let block = |col_off| BlockWindow {
            outer: Vec::new(),
            col_off,
            row_off: 0,
            width: 64,
            height: 64,
        };
        assert!(!plan.wants_block(&block(0)));
        assert!(plan.wants_block(&block(64)));
        assert!(!plan.wants_block(&block(128)));
        let past = LoadOptions {
            window: Some(PixelWindow {
                col0: 0,
                row0: 0,
                col1: 300,
                row1: 10,
            }),
            ..Default::default()
        };
        let err = RasterPlan::new(&view(), &georef(), &sources, &past).expect_err("past");
        assert_eq!(err.sqlstate(), "22023");
    }
}
