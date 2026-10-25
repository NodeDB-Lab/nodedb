// SPDX-License-Identifier: Apache-2.0

//! Where each pixel lands in an array: the cell coordinates of its GEOREF
//! axes, checked against the dim domains once at open.
//!
//! A pixel-mode array stores pixel indexes. The file grid must be the
//! array grid shifted by whole pixels, so file pixel `(col, row)` is cell
//! `(col + col0, row + row0)`. A world-mode array stores pixel-centre
//! world coordinates, so the cell is the file geotransform at
//! `(col + 0.5, row + 0.5)`.
//!
//! The pixel-to-cell map is affine and each coordinate is monotone in the
//! pixel index. So the four corner pixels of the loaded window bound every
//! cell, and a footprint inside the dim domains puts every cell inside them.

use nodedb_raster::GeoTransform;

use crate::coords::bound_text;
use crate::error::RasterLoadError;
use crate::view::{CoordLiteral, DimType, DomainBound, ViewDim};
use crate::window::PixelWindow;

/// Largest relative difference between the file and array linear terms.
const LINEAR_TOLERANCE: f64 = 1e-9;
/// Largest distance from a whole number, in pixels or INT64 units.
const INTEGRAL_TOLERANCE: f64 = 1e-6;
/// Largest pixel offset kept as `i64`, so `offset + u32` never overflows.
const MAX_OFFSET: f64 = (1u64 << 62) as f64;

/// The cell coordinates of one pixel on the array's GEOREF axes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CellGrid {
    /// File pixel `(col, row)` is cell `(col + col0, row + row0)`.
    Pixel { col0: i64, row0: i64 },
    /// The cell is the pixel centre in world coordinates. An `INT64` axis
    /// holds the rounded centre, which open checks to be whole.
    World {
        gt: GeoTransform,
        x_int: bool,
        y_int: bool,
    },
}

impl CellGrid {
    /// The `(x, y)` cell coordinates of file pixel `(col, row)`.
    pub fn cell(&self, col: u32, row: u32) -> (CoordLiteral, CoordLiteral) {
        match *self {
            CellGrid::Pixel { col0, row0 } => (
                CoordLiteral::Int64(i64::from(col) + col0),
                CoordLiteral::Int64(i64::from(row) + row0),
            ),
            CellGrid::World { gt, x_int, y_int } => {
                let (x, y) = gt.pixel_to_world(f64::from(col) + 0.5, f64::from(row) + 0.5);
                (world_coord(x, x_int), world_coord(y, y_int))
            }
        }
    }
}

fn world_coord(value: f64, int: bool) -> CoordLiteral {
    if int {
        // Open proved every centre on this axis whole and inside the domain.
        CoordLiteral::Int64(value.round() as i64)
    } else {
        CoordLiteral::Float64(value)
    }
}

/// The file facts the grid checks read.
pub struct GridInput<'a> {
    /// The array name, for messages.
    pub array: &'a str,
    pub x_dim: &'a ViewDim,
    pub y_dim: &'a ViewDim,
    /// The array GEOREF transform. `None` is a world-mode array.
    pub array_gt: Option<GeoTransform>,
    pub file_gt: GeoTransform,
    /// The pixels that load.
    pub window: PixelWindow,
}

/// Check the file grid against the array and the loaded footprint against
/// the dim domains. Misalignment is `22023`, a footprint outside a domain
/// `22003`.
pub fn plan_grid(input: &GridInput<'_>) -> Result<CellGrid, RasterLoadError> {
    match input.array_gt {
        Some(array_gt) => pixel_grid(input, array_gt),
        None => world_grid(input),
    }
}

fn pixel_grid(input: &GridInput<'_>, array: GeoTransform) -> Result<CellGrid, RasterLoadError> {
    let file = input.file_gt.0;
    let array_gt = array.0;
    let scale = [1, 2, 4, 5]
        .iter()
        .map(|&k| array_gt[k].abs())
        .fold(0.0_f64, f64::max);
    let linear_equal = [1, 2, 4, 5]
        .iter()
        .all(|&k| (file[k] - array_gt[k]).abs() <= LINEAR_TOLERANCE * scale);
    if !linear_equal {
        return Err(misaligned(
            input,
            format!(
                "the file pixel size and rotation {:?} differ from the array TRANSFORM {:?}",
                [file[1], file[2], file[4], file[5]],
                [array_gt[1], array_gt[2], array_gt[4], array_gt[5]]
            ),
        ));
    }
    let Some((u0, v0)) = array.world_to_pixel(file[0], file[3]) else {
        return Err(misaligned(
            input,
            "the array TRANSFORM has no inverse".into(),
        ));
    };
    let (Some(col0), Some(row0)) = (whole_offset(u0), whole_offset(v0)) else {
        return Err(misaligned(
            input,
            format!("the file origin falls at array pixel ({u0}, {v0}), not on a whole pixel"),
        ));
    };
    let w = input.window;
    check_int_span(
        input.x_dim,
        col0 + i64::from(w.col0),
        col0 + i64::from(w.col1) - 1,
    )?;
    check_int_span(
        input.y_dim,
        row0 + i64::from(w.row0),
        row0 + i64::from(w.row1) - 1,
    )?;
    Ok(CellGrid::Pixel { col0, row0 })
}

/// `value` rounded to `i64` when it is within the tolerance of a whole
/// number and inside the offset range.
fn whole_offset(value: f64) -> Option<i64> {
    let rounded = value.round();
    let whole = (value - rounded).abs() <= INTEGRAL_TOLERANCE;
    (whole && rounded.abs() <= MAX_OFFSET).then_some(rounded as i64)
}

fn world_grid(input: &GridInput<'_>) -> Result<CellGrid, RasterLoadError> {
    let gt = input.file_gt;
    let w = input.window;
    let (c0, c1) = (f64::from(w.col0) + 0.5, f64::from(w.col1) - 0.5);
    let (r0, r1) = (f64::from(w.row0) + 0.5, f64::from(w.row1) - 0.5);
    let corners = [(c0, r0), (c1, r0), (c0, r1), (c1, r1)].map(|(u, v)| gt.pixel_to_world(u, v));
    let xs = corners.map(|(x, _)| x);
    let ys = corners.map(|(_, y)| y);
    let x_int = int_axis(input.x_dim)?;
    let y_int = int_axis(input.y_dim)?;
    let file = input.file_gt.0;
    if x_int {
        check_whole_axis(input.x_dim, &xs, [file[1], file[2]])?;
    }
    if y_int {
        check_whole_axis(input.y_dim, &ys, [file[4], file[5]])?;
    }
    check_world_span(input.x_dim, &xs)?;
    check_world_span(input.y_dim, &ys)?;
    Ok(CellGrid::World { gt, x_int, y_int })
}

/// Whether a world-mode axis is `INT64`. GEOREF admits `INT64` and
/// `FLOAT64` axes only.
fn int_axis(dim: &ViewDim) -> Result<bool, RasterLoadError> {
    match dim.dtype {
        DimType::Int64 => Ok(true),
        DimType::Float64 => Ok(false),
        DimType::TimestampMs | DimType::String => Err(RasterLoadError::AxisNotWorld {
            dim: dim.name.clone(),
        }),
    }
}

/// Every pixel centre on an `INT64` world axis must be whole: the corner
/// centres and the per-pixel steps.
fn check_whole_axis(
    dim: &ViewDim,
    corners: &[f64; 4],
    steps: [f64; 2],
) -> Result<(), RasterLoadError> {
    let whole = |v: &f64| (v - v.round()).abs() <= INTEGRAL_TOLERANCE;
    if corners.iter().all(whole) && steps.iter().all(whole) {
        return Ok(());
    }
    Err(RasterLoadError::AxisNotWhole {
        dim: dim.name.clone(),
    })
}

fn check_world_span(dim: &ViewDim, corners: &[f64; 4]) -> Result<(), RasterLoadError> {
    let lo = corners.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = corners.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    match dim.dtype {
        DimType::Int64 => check_int_span(dim, lo.round() as i64, hi.round() as i64),
        DimType::Float64 | DimType::TimestampMs | DimType::String => {
            let inside = match (&dim.lo, &dim.hi) {
                (DomainBound::Float64(d_lo), DomainBound::Float64(d_hi)) => {
                    *d_lo <= lo && hi <= *d_hi
                }
                _ => false,
            };
            if inside {
                Ok(())
            } else {
                Err(outside(dim, lo.to_string(), hi.to_string()))
            }
        }
    }
}

/// Check the integer span `[lo, hi]` of an axis against its domain.
fn check_int_span(dim: &ViewDim, lo: i64, hi: i64) -> Result<(), RasterLoadError> {
    let inside = match (&dim.lo, &dim.hi) {
        (DomainBound::Int64(d_lo), DomainBound::Int64(d_hi)) => *d_lo <= lo && hi <= *d_hi,
        _ => false,
    };
    if inside {
        return Ok(());
    }
    Err(outside(dim, lo.to_string(), hi.to_string()))
}

fn outside(dim: &ViewDim, lo: String, hi: String) -> RasterLoadError {
    RasterLoadError::FootprintOutside {
        dim: dim.name.clone(),
        lo,
        hi,
        domain_lo: bound_text(&dim.lo),
        domain_hi: bound_text(&dim.hi),
    }
}

fn misaligned(input: &GridInput<'_>, why: String) -> RasterLoadError {
    RasterLoadError::Misaligned {
        array: input.array.to_string(),
        why,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_dim(name: &str, lo: i64, hi: i64) -> ViewDim {
        ViewDim {
            name: name.into(),
            dtype: DimType::Int64,
            lo: DomainBound::Int64(lo),
            hi: DomainBound::Int64(hi),
        }
    }

    fn float_dim(name: &str, lo: f64, hi: f64) -> ViewDim {
        ViewDim {
            name: name.into(),
            dtype: DimType::Float64,
            lo: DomainBound::Float64(lo),
            hi: DomainBound::Float64(hi),
        }
    }

    const GT: [f64; 6] = [600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];

    fn input<'a>(
        x: &'a ViewDim,
        y: &'a ViewDim,
        array_gt: Option<[f64; 6]>,
        file_gt: [f64; 6],
    ) -> GridInput<'a> {
        GridInput {
            array: "scene",
            x_dim: x,
            y_dim: y,
            array_gt: array_gt.map(GeoTransform),
            file_gt: GeoTransform(file_gt),
            window: PixelWindow::whole(256, 192),
        }
    }

    #[test]
    fn a_shifted_file_lands_at_its_pixel_offset() {
        let (x, y) = (int_dim("col", 0, 1000), int_dim("row", 0, 1000));
        let file = [
            600_000.0 + 30.0 * 10.0,
            30.0,
            0.0,
            700_000.0 - 30.0 * 4.0,
            0.0,
            -30.0,
        ];
        let grid = plan_grid(&input(&x, &y, Some(GT), file)).expect("aligned");
        let (cx, cy) = grid.cell(0, 0);
        assert_eq!(cx, CoordLiteral::Int64(10));
        assert_eq!(cy, CoordLiteral::Int64(4));
    }

    #[test]
    fn a_half_pixel_shift_is_misaligned() {
        let (x, y) = (int_dim("col", 0, 1000), int_dim("row", 0, 1000));
        let file = [600_015.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];
        let err = plan_grid(&input(&x, &y, Some(GT), file)).expect_err("half pixel");
        assert_eq!(err.sqlstate(), "22023");
        assert!(
            err.to_string().contains("resample outside the engine"),
            "{err}"
        );
        let scaled = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];
        let err = plan_grid(&input(&x, &y, Some(GT), scaled)).expect_err("pixel size");
        assert_eq!(err.sqlstate(), "22023");
    }

    #[test]
    fn a_footprint_past_the_domain_is_22003() {
        let (x, y) = (int_dim("col", 0, 255), int_dim("row", 0, 99));
        let err = plan_grid(&input(&x, &y, Some(GT), GT)).expect_err("rows past 99");
        assert_eq!(err.sqlstate(), "22003");
    }

    #[test]
    fn a_window_inside_the_domain_loads_where_the_file_does_not_fit() {
        let (x, y) = (int_dim("col", 0, 255), int_dim("row", 0, 99));
        let mut windowed = input(&x, &y, Some(GT), GT);
        windowed.window = PixelWindow {
            col0: 0,
            row0: 0,
            col1: 256,
            row1: 100,
        };
        assert!(plan_grid(&windowed).is_ok());
    }

    #[test]
    fn world_cells_are_pixel_centres() {
        let x = float_dim("x", 600_000.0, 607_680.0);
        let y = float_dim("y", 694_240.0, 700_000.0);
        let grid = plan_grid(&input(&x, &y, None, GT)).expect("inside");
        let (cx, cy) = grid.cell(2, 1);
        assert_eq!(cx, CoordLiteral::Float64(600_075.0));
        assert_eq!(cy, CoordLiteral::Float64(699_955.0));
    }

    #[test]
    fn int_world_axes_need_whole_centres() {
        let x = int_dim("x", 0, 10_000_000);
        let y = int_dim("y", 0, 10_000_000);
        let quarter = [600_000.0, 25.0, 0.0, 700_000.0, 0.0, -25.0];
        let err = plan_grid(&input(&x, &y, None, quarter)).expect_err("centres at 12.5 m");
        assert_eq!(err.sqlstate(), "22023");
        let shifted = [599_985.0, 30.0, 0.0, 700_015.0, 0.0, -30.0];
        let grid = plan_grid(&input(&x, &y, None, shifted)).expect("whole centres");
        let (cx, cy) = grid.cell(1, 1);
        assert_eq!(cx, CoordLiteral::Int64(600_030));
        assert_eq!(cy, CoordLiteral::Int64(699_970));
    }
}
