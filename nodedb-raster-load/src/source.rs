// SPDX-License-Identifier: Apache-2.0

//! What each source's header says, and the check that every source of one
//! load shares a grid and a block layout.
//!
//! A multi-source load reads the same block index from every source in
//! lockstep, so each cell write carries the bands of every source. That
//! needs one image size, one geotransform, one CRS and one block grid.

use nodedb_raster::{BandInfo, BlockWindow, GeoTransform, RasterError, RasterHeader, RasterReader};

use crate::error::RasterLoadError;

/// The header facts of one source a load plans from.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceHeader {
    /// The source as the statement wrote it, for messages.
    pub name: String,
    pub width: u32,
    pub height: u32,
    /// One entry per band of the source, in file order.
    pub bands: Vec<BandInfo>,
    /// The GDAL-order pixel-corner geotransform. `None` when the file has
    /// none.
    pub transform: Option<GeoTransform>,
    /// The SRID the file CRS resolves to. `None` when it names none.
    pub srid: Option<u32>,
}

impl SourceHeader {
    /// The facts of `header`, with its CRS resolved to `srid`.
    pub fn new(name: impl Into<String>, header: &RasterHeader, srid: Option<u32>) -> Self {
        Self {
            name: name.into(),
            width: header.width,
            height: header.height,
            bands: header.bands.clone(),
            transform: header.transform,
            srid,
        }
    }
}

/// Why two sources do not share a layout: a header mismatch, or a block
/// window the reader cannot name.
#[derive(Debug, thiserror::Error)]
pub enum LayoutError {
    #[error(transparent)]
    Mismatch(#[from] RasterLoadError),
    #[error(transparent)]
    Reader(#[from] RasterError),
}

/// Check that every source shares the first source's image size,
/// geotransform, CRS and block grid. `headers[i]` describes `readers[i]`.
pub fn check_same_layout<R: RasterReader>(
    headers: &[SourceHeader],
    readers: &[R],
) -> Result<(), LayoutError> {
    let (Some(first), Some(first_reader)) = (headers.first(), readers.first()) else {
        return Ok(());
    };
    for (offset, (header, reader)) in headers.iter().zip(readers).enumerate().skip(1) {
        let mismatch = |what: String| RasterLoadError::SourceMismatch {
            index: offset + 1,
            source_name: header.name.clone(),
            first: first.name.clone(),
            what,
        };
        if (header.width, header.height) != (first.width, first.height) {
            return Err(mismatch(format!(
                "grid {}x{} (source 1 is {}x{})",
                header.width, header.height, first.width, first.height
            ))
            .into());
        }
        let transform = header.transform.map(|gt| gt.0);
        let first_transform = first.transform.map(|gt| gt.0);
        if transform != first_transform {
            return Err(mismatch(format!(
                "geotransform {transform:?} (source 1 is {first_transform:?})"
            ))
            .into());
        }
        if header.srid != first.srid {
            return Err(mismatch(format!(
                "CRS {} (source 1 is {})",
                srid_text(header.srid),
                srid_text(first.srid)
            ))
            .into());
        }
        let count = reader.block_count();
        if count != first_reader.block_count() {
            return Err(mismatch(format!(
                "block count {count} (source 1 has {})",
                first_reader.block_count()
            ))
            .into());
        }
        for index in 0..count {
            let window = reader.block_window(index)?;
            let expected = first_reader.block_window(index)?;
            if window != expected {
                return Err(mismatch(format!(
                    "block {index} window {} (source 1 is {})",
                    window_text(&window),
                    window_text(&expected)
                ))
                .into());
            }
        }
    }
    Ok(())
}

fn srid_text(srid: Option<u32>) -> String {
    srid.map_or_else(|| "none".to_string(), |code| format!("SRID {code}"))
}

fn window_text(window: &BlockWindow) -> String {
    format!(
        "{}x{} at ({}, {})",
        window.width, window.height, window.col_off, window.row_off
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raster::{CrsRef, RasterBlock, SampleType};

    /// A reader of one `width x height` image in `tile`-sized blocks.
    struct Grid {
        header: RasterHeader,
        tile: u32,
    }

    impl RasterReader for Grid {
        fn header(&self) -> &RasterHeader {
            &self.header
        }

        fn block_count(&self) -> u32 {
            self.header.width.div_ceil(self.tile) * self.header.height.div_ceil(self.tile)
        }

        fn block_window(&self, index: u32) -> Result<BlockWindow, RasterError> {
            let across = self.header.width.div_ceil(self.tile);
            let (col_off, row_off) = ((index % across) * self.tile, (index / across) * self.tile);
            Ok(BlockWindow {
                outer: Vec::new(),
                col_off,
                row_off,
                width: self.tile.min(self.header.width - col_off),
                height: self.tile.min(self.header.height - row_off),
            })
        }

        fn block_ranges(&self, _: u32) -> Result<Vec<nodedb_raster::ByteRange>, RasterError> {
            Ok(Vec::new())
        }

        fn decode_block(
            &self,
            index: u32,
            _: &[Option<&[u8]>],
        ) -> Result<RasterBlock, RasterError> {
            Err(RasterError::BlockOutOfRange { index, count: 0 })
        }
    }

    fn grid(width: u32, tile: u32) -> Grid {
        Grid {
            header: RasterHeader {
                width,
                height: 64,
                outer: Vec::new(),
                bands: vec![BandInfo::plain(SampleType::U16)],
                transform: Some(GeoTransform([0.0, 1.0, 0.0, 0.0, 0.0, -1.0])),
                crs: CrsRef::Epsg(4326),
            },
            tile,
        }
    }

    fn headers(readers: &[Grid]) -> Vec<SourceHeader> {
        readers
            .iter()
            .enumerate()
            .map(|(i, r)| SourceHeader::new(format!("/b{i}.tif"), &r.header, Some(4326)))
            .collect()
    }

    #[test]
    fn sources_of_one_grid_and_tiling_pass() {
        let readers = [grid(128, 32), grid(128, 32)];
        assert!(check_same_layout(&headers(&readers), &readers).is_ok());
    }

    #[test]
    fn a_mismatched_grid_or_tiling_names_the_source() {
        for readers in [
            [grid(128, 32), grid(96, 32)],
            [grid(128, 32), grid(128, 64)],
        ] {
            let Err(LayoutError::Mismatch(err)) = check_same_layout(&headers(&readers), &readers)
            else {
                panic!("refused");
            };
            assert_eq!(err.sqlstate(), "22023");
            assert!(err.to_string().contains("/b1.tif"), "{err}");
        }
    }
}
