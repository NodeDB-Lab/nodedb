// SPDX-License-Identifier: Apache-2.0

//! The rows of one block index, read one pixel at a time across every
//! source.
//!
//! A load reads the same block index from every source, so one row carries
//! every band's attr. A row's number is the pixel's row-major ordinal in
//! the image plus one. A pixel outside the loaded window is skipped. A band
//! holds no data at a pixel its mask marks invalid (an internal mask, its
//! LERC mask, or a block the file omits) or where it holds its nodata. A
//! pixel where no loaded band holds data is skipped. A kept pixel writes
//! NULL for a band that holds no data, or is refused with `23502` when the
//! attr is NOT NULL. Open checked the types and the footprint, so no other
//! check runs per pixel.
//!
//! Each kept pixel allocates its coordinate and attr vectors, sized
//! exactly. A skipped pixel allocates nothing.

use nodedb_raster::{BandSamples, BlockWindow, RasterBlock};

use crate::bands::AttrSource;
use crate::coords::CoordSource;
use crate::error::RasterLoadError;
use crate::plan::RasterPlan;
use crate::view::{AttrLiteral, CoordLiteral, LoadRow};

/// The rows of one block index in pixel order within the block.
pub struct BlockRows<'a> {
    plan: &'a RasterPlan,
    /// One block per source, in source order, all of one window.
    blocks: &'a [RasterBlock],
    window: &'a BlockWindow,
    next: usize,
    pixels: usize,
}

impl<'a> BlockRows<'a> {
    /// Read `blocks`, one per source, under `plan`. A block with the wrong
    /// band count, a short band, or a window unlike the first source's is a
    /// malformed input (`22P04`).
    pub fn new(plan: &'a RasterPlan, blocks: &'a [RasterBlock]) -> Result<Self, RasterLoadError> {
        let shape = |index: u32, source: usize, what: String| RasterLoadError::BlockShape {
            index,
            source_index: source + 1,
            what,
        };
        let Some(first) = blocks.first() else {
            return Err(shape(0, 0, "is missing".into()));
        };
        if blocks.len() != plan.source_count() {
            return Err(shape(
                first.index,
                0,
                format!(
                    "came from {} sources, the load reads {}",
                    blocks.len(),
                    plan.source_count()
                ),
            ));
        }
        let pixels = first.window.pixels();
        for (source, block) in blocks.iter().enumerate() {
            if block.window != first.window {
                return Err(shape(
                    block.index,
                    source,
                    "covers another window than source 1".into(),
                ));
            }
            let declared = plan.source_bands.get(source).copied().unwrap_or(0);
            if block.bands.len() != declared {
                return Err(shape(
                    block.index,
                    source,
                    format!(
                        "decoded {} bands, the file declares {declared}",
                        block.bands.len()
                    ),
                ));
            }
            if let Some(short) = block.bands.iter().find(|band| band.len() < pixels) {
                return Err(shape(
                    block.index,
                    source,
                    format!("decoded {} samples, its window holds {pixels}", short.len()),
                ));
            }
        }
        Ok(Self {
            plan,
            blocks,
            window: &first.window,
            next: 0,
            pixels,
        })
    }

    /// The samples of load band `band`, and the block that holds them.
    fn band_block(&self, band: usize) -> Option<(&'a RasterBlock, usize)> {
        let at = self.plan.band(band)?;
        let block = self.blocks.get(at.source)?;
        Some((block, at.band))
    }

    /// Whether load band `band` holds data at pixel `index`: its mask
    /// marks the pixel valid and the sample is not its nodata.
    fn holds_data(&self, band: usize, index: usize) -> bool {
        let Some((block, local)) = self.band_block(band) else {
            return false;
        };
        if !block.band_valid(local, index) {
            return false;
        }
        match (
            self.plan.band_nodata.get(band).copied().flatten(),
            block.bands.get(local),
        ) {
            (Some(nodata), Some(samples)) => !nodata.matches(samples, index),
            (None, Some(_)) => true,
            (_, None) => false,
        }
    }

    /// The image pixel `(col, row)` of window pixel `index`.
    fn pixel(&self, index: usize) -> (u32, u32) {
        let window = self.window;
        let local_row = index / window.width as usize;
        let local_col = index % window.width as usize;
        // The window lies inside the image, so both fit `u32`.
        (
            window.col_off + local_col as u32,
            window.row_off + local_row as u32,
        )
    }

    /// The row of pixel `index` of the block.
    fn row(&self, index: usize) -> Result<(usize, LoadRow), RasterLoadError> {
        let (col, row) = self.pixel(index);
        let row_no = row as usize * self.plan.width as usize + col as usize + 1;
        let (x, y) = self.plan.grid.cell(col, row);
        let mut coords = Vec::with_capacity(self.plan.coords.len());
        for source in &self.plan.coords {
            coords.push(match source {
                CoordSource::X => x.clone(),
                CoordSource::Y => y.clone(),
                CoordSource::Time(ms) => CoordLiteral::Int64(*ms),
            });
        }
        let mut attrs = Vec::with_capacity(self.plan.attrs.len());
        for source in &self.plan.attrs {
            let AttrSource::Band {
                band,
                float,
                nullable,
                name,
            } = source
            else {
                attrs.push(AttrLiteral::Null);
                continue;
            };
            let literal = match (self.holds_data(*band, index), *nullable) {
                (false, true) => AttrLiteral::Null,
                (false, false) => {
                    return Err(RasterLoadError::NodataInNotNull {
                        row: row_no,
                        attr: name.clone(),
                        band: band + 1,
                        pixel_row: row,
                        pixel_col: col,
                    });
                }
                (true, _) => self
                    .band_block(*band)
                    .and_then(|(block, local)| block.bands.get(local))
                    .and_then(|samples| sample_literal(samples, index, *float))
                    .ok_or(RasterLoadError::MissingSample {
                        row: row_no,
                        band: band + 1,
                    })?,
            };
            attrs.push(literal);
        }
        Ok((row_no, LoadRow { coords, attrs }))
    }

    /// Whether pixel `index` loads: inside the window, and some loaded band
    /// holds data there.
    fn keeps(&self, index: usize) -> bool {
        let (col, row) = self.pixel(index);
        self.plan.window.contains(col, row)
            && self
                .plan
                .loaded
                .iter()
                .any(|&band| self.holds_data(band, index))
    }
}

impl Iterator for BlockRows<'_> {
    type Item = Result<(usize, LoadRow), RasterLoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.pixels {
            let index = self.next;
            self.next += 1;
            if self.keeps(index) {
                return Some(self.row(index));
            }
        }
        None
    }
}

/// Sample `index` as an attr literal. Open admitted only lossless pairs,
/// so an integer sample widens to `f64` exactly when the attr is a float.
fn sample_literal(samples: &BandSamples, index: usize, float: bool) -> Option<AttrLiteral> {
    let int = |n: i64| {
        if float {
            AttrLiteral::Float64(n as f64)
        } else {
            AttrLiteral::Int64(n)
        }
    };
    match samples {
        BandSamples::U8(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::I8(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::U16(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::I16(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::U32(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::I32(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::F32(v) => v.get(index).map(|s| AttrLiteral::Float64(f64::from(*s))),
        BandSamples::F64(v) => v.get(index).map(|s| AttrLiteral::Float64(*s)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grid::CellGrid;
    use crate::nodata::Nodata;
    use crate::plan::PlanBand;
    use crate::window::PixelWindow;
    use nodedb_raster::SampleType;

    fn band(source: usize, band: usize) -> PlanBand {
        PlanBand {
            source,
            band,
            sample: SampleType::U16,
        }
    }

    fn plan(nullable: bool) -> RasterPlan {
        RasterPlan {
            width: 4,
            window: PixelWindow::whole(4, 2),
            bands: vec![band(0, 0), band(0, 1)],
            source_bands: vec![2],
            coords: vec![CoordSource::Y, CoordSource::X],
            attrs: vec![
                AttrSource::Band {
                    band: 0,
                    float: false,
                    nullable,
                    name: "b1".into(),
                },
                AttrSource::Band {
                    band: 1,
                    float: true,
                    nullable: true,
                    name: "b2".into(),
                },
                AttrSource::Null,
            ],
            loaded: vec![0, 1],
            grid: CellGrid::Pixel { col0: 10, row0: 20 },
            band_nodata: vec![Some(Nodata::Int(0)), Some(Nodata::Int(0))],
        }
    }

    fn window() -> BlockWindow {
        BlockWindow {
            outer: Vec::new(),
            col_off: 2,
            row_off: 0,
            width: 2,
            height: 2,
        }
    }

    fn block() -> RasterBlock {
        RasterBlock {
            index: 1,
            window: window(),
            bands: vec![
                BandSamples::U16(vec![5, 0, 0, 7]),
                BandSamples::U16(vec![6, 0, 9, 8]),
            ],
            masks: vec![None, None],
        }
    }

    fn rows(plan: &RasterPlan, blocks: &[RasterBlock]) -> Vec<(usize, LoadRow)> {
        BlockRows::new(plan, blocks)
            .expect("block fits")
            .collect::<Result<_, _>>()
            .expect("all rows load")
    }

    /// Attr metadata gives each band its own nodata: a band with none never
    /// holds nodata, so no pixel is skipped and only band 1 writes NULL.
    #[test]
    fn per_band_nodata_applies_to_its_band_only() {
        let mut plan = plan(true);
        plan.band_nodata = vec![Some(Nodata::Int(0)), None];
        let rows = rows(&plan, &[block()]);
        assert_eq!(rows.len(), 4);
        let (_, both_zero) = &rows[1];
        assert_eq!(both_zero.attrs[0], AttrLiteral::Null);
        assert_eq!(both_zero.attrs[1], AttrLiteral::Float64(0.0));
    }

    #[test]
    fn rows_skip_all_nodata_pixels_and_null_partial_ones() {
        let rows = rows(&plan(true), &[block()]);
        let numbers: Vec<usize> = rows.iter().map(|(n, _)| *n).collect();
        // Pixel (0, 3) is nodata in both bands. Row-major ordinals: 2, 6, 7.
        assert_eq!(numbers, vec![3, 7, 8]);
        let (_, first) = &rows[0];
        assert_eq!(
            first.coords,
            vec![CoordLiteral::Int64(20), CoordLiteral::Int64(12)]
        );
        assert_eq!(
            first.attrs,
            vec![
                AttrLiteral::Int64(5),
                AttrLiteral::Float64(6.0),
                AttrLiteral::Null
            ]
        );
        let (_, partial) = &rows[1];
        assert_eq!(partial.attrs[0], AttrLiteral::Null);
        assert_eq!(partial.attrs[1], AttrLiteral::Float64(9.0));
    }

    #[test]
    fn a_nodata_band_in_a_not_null_attr_names_the_pixel() {
        let (plan, blocks) = (plan(false), [block()]);
        let err = BlockRows::new(&plan, &blocks)
            .expect("block fits")
            .find_map(Result::err)
            .expect("pixel (1, 2) is refused");
        assert_eq!(err.sqlstate(), "23502");
        assert_eq!(err.row(), 7);
        assert!(err.to_string().contains("(pixel row 1, col 2)"), "{err}");
    }

    #[test]
    fn a_band_mask_nulls_its_band_and_an_all_masked_pixel_is_skipped() {
        let plan = plan(true);
        let mut block = block();
        block.masks = vec![
            Some(vec![false, true, true, false]),
            Some(vec![true, true, true, false]),
        ];
        let rows = rows(&plan, &[block]);
        let numbers: Vec<usize> = rows.iter().map(|(n, _)| *n).collect();
        // Pixel 1 is nodata in both bands; pixel 3 is masked in both.
        assert_eq!(numbers, vec![3, 7]);
        let (_, masked_first) = &rows[0];
        assert_eq!(masked_first.attrs[0], AttrLiteral::Null);
        assert_eq!(masked_first.attrs[1], AttrLiteral::Float64(6.0));
    }

    #[test]
    fn sources_load_in_lockstep_into_one_row() {
        let mut plan = plan(true);
        plan.bands = vec![band(0, 0), band(1, 0)];
        plan.source_bands = vec![1, 1];
        let red = RasterBlock {
            index: 1,
            window: window(),
            bands: vec![BandSamples::U16(vec![5, 0, 0, 7])],
            masks: vec![None],
        };
        let nir = RasterBlock {
            index: 1,
            window: window(),
            bands: vec![BandSamples::U16(vec![6, 0, 9, 8])],
            masks: vec![None],
        };
        let rows = rows(&plan, &[red.clone(), nir.clone()]);
        let (_, first) = &rows[0];
        assert_eq!(first.attrs[0], AttrLiteral::Int64(5));
        assert_eq!(first.attrs[1], AttrLiteral::Float64(6.0));
        assert_eq!(rows.len(), 3);
        let mut shifted = nir;
        shifted.window.col_off = 0;
        let err = BlockRows::new(&plan, &[red, shifted])
            .err()
            .expect("windows differ");
        assert_eq!(err.sqlstate(), "22P04");
    }

    #[test]
    fn pixels_outside_the_window_are_skipped() {
        let mut plan = plan(true);
        plan.window = PixelWindow {
            col0: 3,
            row0: 0,
            col1: 4,
            row1: 1,
        };
        let numbers: Vec<usize> = rows(&plan, &[block()]).iter().map(|(n, _)| *n).collect();
        assert_eq!(numbers, Vec::<usize>::new());
        plan.window.col0 = 2;
        let numbers: Vec<usize> = rows(&plan, &[block()]).iter().map(|(n, _)| *n).collect();
        assert_eq!(numbers, vec![3]);
    }

    #[test]
    fn a_short_block_is_malformed() {
        let plan = plan(true);
        let mut block = block();
        block.bands[1] = BandSamples::U16(vec![1]);
        let err = BlockRows::new(&plan, &[block]).err().expect("short band");
        assert_eq!(err.sqlstate(), "22P04");
    }
}
