// SPDX-License-Identifier: Apache-2.0

//! The pixel window a load reads: the whole image, or `WINDOW (...)`.

use nodedb_raster::BlockWindow;

use crate::error::RasterLoadError;

/// The half-open pixel window `cols x rows` of an image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PixelWindow {
    pub col0: u32,
    pub row0: u32,
    /// One past the last column.
    pub col1: u32,
    /// One past the last row.
    pub row1: u32,
}

impl PixelWindow {
    /// The whole `width x height` image.
    pub const fn whole(width: u32, height: u32) -> Self {
        Self {
            col0: 0,
            row0: 0,
            col1: width,
            row1: height,
        }
    }

    /// The requested `window` of a `width x height` image, or the whole
    /// image when no window is given. An empty window or one past the image
    /// is refused (`22023`).
    pub fn of(
        window: Option<PixelWindow>,
        width: u32,
        height: u32,
    ) -> Result<Self, RasterLoadError> {
        let Some(w) = window else {
            return Ok(Self::whole(width, height));
        };
        if w.col0 >= w.col1 || w.row0 >= w.row1 || w.col1 > width || w.row1 > height {
            return Err(RasterLoadError::WindowOutside {
                x0: w.col0,
                y0: w.row0,
                x1: w.col1,
                y1: w.row1,
                width,
                height,
            });
        }
        Ok(w)
    }

    /// Whether pixel `(col, row)` lies inside.
    pub fn contains(&self, col: u32, row: u32) -> bool {
        (self.col0..self.col1).contains(&col) && (self.row0..self.row1).contains(&row)
    }

    /// Whether `block` holds any pixel of the window.
    pub fn overlaps(&self, block: &BlockWindow) -> bool {
        let block_col1 = block.col_off.saturating_add(block.width);
        let block_row1 = block.row_off.saturating_add(block.height);
        block.width > 0
            && block.height > 0
            && block.col_off < self.col1
            && self.col0 < block_col1
            && block.row_off < self.row1
            && self.row0 < block_row1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(col_off: u32, row_off: u32, width: u32, height: u32) -> BlockWindow {
        BlockWindow {
            outer: Vec::new(),
            col_off,
            row_off,
            width,
            height,
        }
    }

    #[test]
    fn a_window_selects_the_blocks_it_touches() {
        let w = PixelWindow::of(
            Some(PixelWindow {
                col0: 300,
                row0: 0,
                col1: 600,
                row1: 256,
            }),
            1024,
            1024,
        )
        .expect("inside");
        assert!(!w.overlaps(&block(0, 0, 256, 256)));
        assert!(w.overlaps(&block(256, 0, 256, 256)));
        assert!(w.overlaps(&block(512, 0, 256, 256)));
        assert!(!w.overlaps(&block(768, 0, 256, 256)));
        assert!(!w.overlaps(&block(256, 256, 256, 256)));
        assert!(w.contains(300, 255));
        assert!(!w.contains(600, 0));
    }

    #[test]
    fn a_window_past_the_image_is_refused() {
        let past = PixelWindow {
            col0: 0,
            row0: 0,
            col1: 1025,
            row1: 10,
        };
        let err = PixelWindow::of(Some(past), 1024, 1024).expect_err("past the edge");
        assert_eq!(err.sqlstate(), "22023");
        assert_eq!(PixelWindow::of(None, 4, 3), Ok(PixelWindow::whole(4, 3)));
    }
}
