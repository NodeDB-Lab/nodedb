// SPDX-License-Identifier: Apache-2.0

//! The GDAL-order affine geotransform between pixel and world positions.
//! Arrays, raster readers and raster loads share this one type.

use serde::{Deserialize, Serialize};

/// Affine pixel-to-world transform in GDAL coefficient order.
///
/// For a continuous pixel position `(u, v)`, `u` along the column axis and
/// `v` along the row axis:
///
/// - `X = GT0 + u·GT1 + v·GT2`
/// - `Y = GT3 + u·GT4 + v·GT5`
///
/// Pixel `(i, j)` covers `[i, i+1) × [j, j+1)`. Its centre is `(i+0.5, j+0.5)`.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct GeoTransform(pub [f64; 6]);

impl PartialEq for GeoTransform {
    /// Bitwise equality. Validation admits finite coefficients only, so
    /// this is equality of the stored values.
    fn eq(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(a, b)| a.to_bits() == b.to_bits())
    }
}

impl Eq for GeoTransform {}

impl GeoTransform {
    /// `GT1·GT5 − GT2·GT4`. Zero means the transform has no inverse.
    pub fn determinant(&self) -> f64 {
        let [_, gt1, gt2, _, gt4, gt5] = self.0;
        gt1 * gt5 - gt2 * gt4
    }

    /// Whether every coefficient is finite and the determinant is finite
    /// and non-zero.
    pub fn is_invertible(&self) -> bool {
        let det = self.determinant();
        self.0.iter().all(|c| c.is_finite()) && det != 0.0 && det.is_finite()
    }

    /// World position of the continuous pixel position `(u, v)`.
    pub fn pixel_to_world(&self, u: f64, v: f64) -> (f64, f64) {
        let [gt0, gt1, gt2, gt3, gt4, gt5] = self.0;
        (gt0 + u * gt1 + v * gt2, gt3 + u * gt4 + v * gt5)
    }

    /// World position of the centre of pixel `(i, j)`.
    pub fn cell_centre(&self, i: i64, j: i64) -> (f64, f64) {
        self.pixel_to_world(i as f64 + 0.5, j as f64 + 0.5)
    }

    /// Continuous pixel position of the world position `(x, y)`.
    ///
    /// Returns `None` when the determinant is zero or not finite.
    pub fn world_to_pixel(&self, x: f64, y: f64) -> Option<(f64, f64)> {
        let det = self.determinant();
        if det == 0.0 || !det.is_finite() {
            return None;
        }
        let [gt0, gt1, gt2, gt3, gt4, gt5] = self.0;
        let dx = x - gt0;
        let dy = y - gt3;
        Some(((gt5 * dx - gt2 * dy) / det, (gt1 * dy - gt4 * dx) / det))
    }

    /// Index of the pixel that holds the world position `(x, y)`.
    ///
    /// Returns `None` when the transform has no inverse or the index falls
    /// outside `i64`.
    pub fn world_to_cell(&self, x: f64, y: f64) -> Option<(i64, i64)> {
        let (u, v) = self.world_to_pixel(x, y)?;
        Some((floor_to_i64(u)?, floor_to_i64(v)?))
    }
}

/// `floor(value)` as `i64`. `None` for a non-finite or out-of-range value.
fn floor_to_i64(value: f64) -> Option<i64> {
    let floored = value.floor();
    // `i64::MAX as f64` rounds up to 2^63, which is outside `i64`.
    if floored.is_finite() && floored >= i64::MIN as f64 && floored < i64::MAX as f64 {
        Some(floored as i64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn assert_close(a: (f64, f64), b: (f64, f64), eps: f64) {
        assert!(
            (a.0 - b.0).abs() <= eps && (a.1 - b.1).abs() <= eps,
            "{a:?} != {b:?}"
        );
    }

    #[test]
    fn north_up_centre_is_index_plus_half() {
        let gt = GeoTransform([100_000.0, 10.0, 0.0, 200_000.0, 0.0, -10.0]);
        assert_close(gt.cell_centre(0, 0), (100_005.0, 199_995.0), EPS);
        assert_close(gt.cell_centre(3, 2), (100_035.0, 199_975.0), EPS);
    }

    #[test]
    fn centre_inverse_round_trip_north_up() {
        let gt = GeoTransform([100_000.0, 10.0, 0.0, 200_000.0, 0.0, -10.0]);
        for (i, j) in [(0, 0), (7, 3), (255, 191), (-4, 9)] {
            let (x, y) = gt.cell_centre(i, j);
            let (u, v) = gt.world_to_pixel(x, y).unwrap();
            assert_close((u, v), (i as f64 + 0.5, j as f64 + 0.5), EPS);
            assert_eq!(gt.world_to_cell(x, y), Some((i, j)));
        }
    }

    #[test]
    fn centre_inverse_round_trip_rotated() {
        let gt = GeoTransform([500_000.0, 8.660254, 5.0, 4_000_000.0, 5.0, -8.660254]);
        assert!(gt.determinant().abs() > 0.0);
        for (i, j) in [(0, 0), (12, 40), (1023, 511)] {
            let (x, y) = gt.cell_centre(i, j);
            let (u, v) = gt.world_to_pixel(x, y).unwrap();
            assert_close((u, v), (i as f64 + 0.5, j as f64 + 0.5), 1e-6);
            assert_eq!(gt.world_to_cell(x, y), Some((i, j)));
        }
    }

    #[test]
    fn centre_inverse_round_trip_geographic_exponent() {
        let res = 8.983152841195214e-05;
        let gt = GeoTransform([100.5, res, 0.0, 6.5, 0.0, -res]);
        let (x, y) = gt.cell_centre(0, 0);
        assert_close((x, y), (100.5 + res / 2.0, 6.5 - res / 2.0), 1e-15);
        for (i, j) in [(0, 0), (100, 200), (11_131, 5_000)] {
            let (x, y) = gt.cell_centre(i, j);
            let (u, v) = gt.world_to_pixel(x, y).unwrap();
            assert_close((u, v), (i as f64 + 0.5, j as f64 + 0.5), 1e-6);
            assert_eq!(gt.world_to_cell(x, y), Some((i, j)));
        }
    }

    #[test]
    fn singular_transform_has_no_inverse() {
        let gt = GeoTransform([0.0, 1.0, 2.0, 0.0, 2.0, 4.0]);
        assert_eq!(gt.determinant(), 0.0);
        assert!(gt.world_to_pixel(1.0, 1.0).is_none());
        assert!(gt.world_to_cell(1.0, 1.0).is_none());
    }

    #[test]
    fn out_of_range_index_is_none() {
        let gt = GeoTransform([0.0, 1e-10, 0.0, 0.0, 0.0, -1e-10]);
        assert!(gt.world_to_pixel(1e10, -1.0).is_some());
        assert!(gt.world_to_cell(1e10, -1.0).is_none());
    }

    #[test]
    fn invertible_needs_finite_terms_and_a_non_zero_determinant() {
        assert!(GeoTransform([0.0, 1.0, 0.0, 0.0, 0.0, -1.0]).is_invertible());
        assert!(!GeoTransform([0.0, 1.0, 2.0, 0.0, 2.0, 4.0]).is_invertible());
        assert!(!GeoTransform([f64::NAN, 1.0, 0.0, 0.0, 0.0, -1.0]).is_invertible());
        assert!(!GeoTransform([0.0; 6]).is_invertible());
    }
}
