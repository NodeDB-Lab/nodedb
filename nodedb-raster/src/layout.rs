// SPDX-License-Identifier: Apache-2.0

//! The block grid of one image: size, band layout, and strip or tile
//! geometry.

use crate::error::RasterError;
use crate::sample::SampleType;

/// How a raster stores its blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    /// Full-width strips of rows.
    Strips,
    /// Rectangular tiles.
    Tiles,
}

/// How a raster stores its bands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interleave {
    /// All bands of a pixel together (`PlanarConfiguration` 1).
    Chunky,
    /// One plane per band (`PlanarConfiguration` 2).
    Planar,
}

/// The pixel rectangle one block covers. Edge blocks are cropped to the
/// image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockWindow {
    /// The block's index along each outer dim, outermost first. Empty for
    /// a 2D raster.
    pub outer: Vec<u64>,
    pub col_off: u32,
    pub row_off: u32,
    pub width: u32,
    pub height: u32,
}

impl BlockWindow {
    /// Pixels in the window.
    pub fn pixels(&self) -> usize {
        self.width as usize * self.height as usize
    }
}

/// The geometry every block read derives from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RasterLayout {
    pub width: u32,
    pub height: u32,
    pub bands: u16,
    pub sample_type: SampleType,
    pub kind: BlockKind,
    pub interleave: Interleave,
    /// Block width as stored. A tile can extend past the image edge.
    pub block_width: u32,
    /// Block height as stored. A strip height is clamped to the image.
    pub block_height: u32,
    pub blocks_across: u32,
    pub blocks_down: u32,
}

impl RasterLayout {
    /// Blocks per band plane.
    pub fn block_count(&self) -> u32 {
        // The TIFF layer checks at open that this product fits a u32.
        self.blocks_across.saturating_mul(self.blocks_down)
    }

    /// Decoded bytes of the largest block, across all bands.
    pub fn block_bytes(&self) -> u64 {
        u64::from(self.block_width)
            .saturating_mul(u64::from(self.block_height))
            .saturating_mul(u64::from(self.bands))
            .saturating_mul(self.sample_type.byte_len() as u64)
    }

    /// The window of block `index`, cropped to the image.
    pub fn window(&self, index: u32) -> Result<BlockWindow, RasterError> {
        let count = self.block_count();
        if index >= count {
            return Err(RasterError::BlockOutOfRange { index, count });
        }
        let col_off = (index % self.blocks_across) * self.block_width;
        let row_off = (index / self.blocks_across) * self.block_height;
        Ok(BlockWindow {
            outer: Vec::new(),
            col_off,
            row_off,
            width: self.block_width.min(self.width - col_off),
            height: self.block_height.min(self.height - row_off),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(kind: BlockKind, block: (u32, u32), image: (u32, u32)) -> RasterLayout {
        RasterLayout {
            width: image.0,
            height: image.1,
            bands: 2,
            sample_type: SampleType::U16,
            kind,
            interleave: Interleave::Chunky,
            block_width: block.0,
            block_height: block.1,
            blocks_across: image.0.div_ceil(block.0),
            blocks_down: image.1.div_ceil(block.1),
        }
    }

    #[test]
    fn edge_tiles_are_cropped() {
        let grid = layout(BlockKind::Tiles, (16, 16), (40, 24));
        assert_eq!(grid.block_count(), 6);
        let last = grid.window(5).expect("in range");
        assert_eq!(
            last,
            BlockWindow {
                outer: Vec::new(),
                col_off: 32,
                row_off: 16,
                width: 8,
                height: 8
            }
        );
        let middle = grid.window(1).expect("in range");
        assert_eq!((middle.col_off, middle.width, middle.height), (16, 16, 16));
        assert_eq!(grid.block_bytes(), 16 * 16 * 2 * 2);
    }

    #[test]
    fn strips_span_the_width() {
        let grid = layout(BlockKind::Strips, (10, 4), (10, 9));
        assert_eq!(grid.block_count(), 3);
        let last = grid.window(2).expect("in range");
        assert_eq!((last.row_off, last.width, last.height), (8, 10, 1));
    }

    #[test]
    fn out_of_range_block_is_a_typed_error() {
        let grid = layout(BlockKind::Strips, (10, 4), (10, 9));
        assert!(matches!(
            grid.window(3),
            Err(RasterError::BlockOutOfRange { index: 3, count: 3 })
        ));
    }
}
