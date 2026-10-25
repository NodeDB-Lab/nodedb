// SPDX-License-Identifier: Apache-2.0

//! The pixel-to-world [`GeoTransform`], from the GeoTIFF model tags.
//!
//! The transform maps the pixel corner `(col, row)` to world coordinates.
//! Tag precedence follows GDAL: a tiepoint with a pixel scale wins over
//! `ModelTransformationTag`.

use nodedb_crs::GeoTransform;

use crate::error::RasterError;

/// `GTRasterTypeGeoKey` value of a raster whose tags name pixel centres.
const PIXEL_IS_POINT: u16 = 2;

/// The georeferencing tags of IFD 0, as read from the file.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelTags<'a> {
    /// `ModelPixelScaleTag`: `(Sx, Sy, Sz)`.
    pub pixel_scale: Option<&'a [f64]>,
    /// `ModelTiepointTag`: `(I, J, K, X, Y, Z)` per tiepoint.
    pub tiepoints: Option<&'a [f64]>,
    /// `ModelTransformationTag`: a row-major 4x4 matrix.
    pub transformation: Option<&'a [f64]>,
    /// `GTRasterTypeGeoKey`, when the GeoKey directory holds it.
    pub raster_type: Option<u16>,
}

/// The transform the tags define, or `None` when the file carries no model
/// tags.
pub fn transform_from_tags(tags: ModelTags<'_>) -> Result<Option<GeoTransform>, RasterError> {
    let base = match (tags.pixel_scale, tags.tiepoints, tags.transformation) {
        (Some(scale), Some(tiepoints), _) => from_tiepoint(scale, tiepoints)?,
        (Some(_), None, None) => return Err(RasterError::PixelScaleWithoutTiepoint),
        (_, _, Some(matrix)) => from_matrix(matrix)?,
        (None, Some(_), None) => return Err(RasterError::GroundControlPoints),
        (None, None, None) => return Ok(None),
    };
    let transform = if tags.raster_type == Some(PIXEL_IS_POINT) {
        shifted_to_corner(base)
    } else {
        base
    };
    if !transform.is_invertible() {
        return Err(RasterError::DegenerateTransform {
            transform: transform.0,
        });
    }
    Ok(Some(transform))
}

/// Move a PixelIsPoint origin from the first pixel's centre to its corner,
/// half a pixel up and left.
fn shifted_to_corner(gt: GeoTransform) -> GeoTransform {
    let mut terms = gt.0;
    terms[0] -= 0.5 * terms[1] + 0.5 * terms[2];
    terms[3] -= 0.5 * terms[4] + 0.5 * terms[5];
    GeoTransform(terms)
}

/// `GT = [X − I·Sx, Sx, 0, Y + J·Sy, 0, −Sy]` from the first tiepoint.
fn from_tiepoint(scale: &[f64], tiepoints: &[f64]) -> Result<GeoTransform, RasterError> {
    let [sx, sy, ..] = *scale else {
        return Err(RasterError::GeoTagLength {
            tag: "ModelPixelScaleTag",
            found: scale.len(),
            expected: "at least 2",
        });
    };
    let [i, j, _k, x, y, ..] = *tiepoints else {
        return Err(RasterError::GeoTagLength {
            tag: "ModelTiepointTag",
            found: tiepoints.len(),
            expected: "at least 6",
        });
    };
    if !tiepoints.len().is_multiple_of(6) {
        return Err(RasterError::GeoTagLength {
            tag: "ModelTiepointTag",
            found: tiepoints.len(),
            expected: "a multiple of 6",
        });
    }
    Ok(GeoTransform([x - i * sx, sx, 0.0, y + j * sy, 0.0, -sy]))
}

/// `GT = [M3, M0, M1, M7, M4, M5]` from a 2D affine 4x4 matrix.
fn from_matrix(matrix: &[f64]) -> Result<GeoTransform, RasterError> {
    let Ok(m) = <[f64; 16]>::try_from(matrix) else {
        return Err(RasterError::GeoTagLength {
            tag: "ModelTransformationTag",
            found: matrix.len(),
            expected: "16",
        });
    };
    let z_terms_zero = m[2] == 0.0 && m[6] == 0.0;
    let last_row_identity = m[12] == 0.0 && m[13] == 0.0 && m[14] == 0.0 && m[15] == 1.0;
    if !(z_terms_zero && last_row_identity) {
        return Err(RasterError::NonAffineTransformation);
    }
    Ok(GeoTransform([m[3], m[0], m[1], m[7], m[4], m[5]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags<'a>() -> ModelTags<'a> {
        ModelTags::default()
    }

    #[test]
    fn no_model_tags_is_no_transform() {
        assert_eq!(transform_from_tags(tags()).ok(), Some(None));
    }

    #[test]
    fn tiepoint_and_scale() {
        let scale = [30.0, 30.0, 0.0];
        let tie = [0.0, 0.0, 0.0, 600_000.0, 450_000.0, 0.0];
        let gt = transform_from_tags(ModelTags {
            pixel_scale: Some(&scale),
            tiepoints: Some(&tie),
            ..tags()
        })
        .expect("valid")
        .expect("present");
        assert_eq!(gt.0, [600_000.0, 30.0, 0.0, 450_000.0, 0.0, -30.0]);
    }

    #[test]
    fn tiepoint_at_a_nonzero_pixel_moves_the_origin() {
        let scale = [10.0, 5.0, 0.0];
        let tie = [2.0, 3.0, 0.0, 1000.0, 2000.0, 0.0];
        let gt = transform_from_tags(ModelTags {
            pixel_scale: Some(&scale),
            tiepoints: Some(&tie),
            ..tags()
        })
        .expect("valid")
        .expect("present");
        assert_eq!(gt.0, [980.0, 10.0, 0.0, 2015.0, 0.0, -5.0]);
        assert_eq!(gt.pixel_to_world(2.0, 3.0), (1000.0, 2000.0));
    }

    #[test]
    fn pixel_is_point_shifts_half_a_pixel() {
        let scale = [10.0, 10.0, 0.0];
        let tie = [0.0, 0.0, 0.0, 100.0, 200.0, 0.0];
        let gt = transform_from_tags(ModelTags {
            pixel_scale: Some(&scale),
            tiepoints: Some(&tie),
            raster_type: Some(PIXEL_IS_POINT),
            ..tags()
        })
        .expect("valid")
        .expect("present");
        assert_eq!(gt.0, [95.0, 10.0, 0.0, 205.0, 0.0, -10.0]);
    }

    #[test]
    fn model_transformation_maps_to_gdal_order() {
        let m = [
            2.0, 0.5, 0.0, 100.0, //
            0.25, -3.0, 0.0, 200.0, //
            0.0, 0.0, 0.0, 0.0, //
            0.0, 0.0, 0.0, 1.0,
        ];
        let gt = transform_from_tags(ModelTags {
            transformation: Some(&m),
            ..tags()
        })
        .expect("valid")
        .expect("present");
        assert_eq!(gt.0, [100.0, 2.0, 0.5, 200.0, 0.25, -3.0]);
    }

    #[test]
    fn rotated_pixel_is_point_shift_uses_both_terms() {
        let m = [
            2.0, 0.5, 0.0, 100.0, //
            0.25, -3.0, 0.0, 200.0, //
            0.0, 0.0, 0.0, 0.0, //
            0.0, 0.0, 0.0, 1.0,
        ];
        let gt = transform_from_tags(ModelTags {
            transformation: Some(&m),
            raster_type: Some(PIXEL_IS_POINT),
            ..tags()
        })
        .expect("valid")
        .expect("present");
        assert_eq!(gt.0, [98.75, 2.0, 0.5, 201.375, 0.25, -3.0]);
    }

    #[test]
    fn non_affine_matrix_is_refused() {
        let mut m = [0.0; 16];
        m[0] = 1.0;
        m[5] = -1.0;
        m[2] = 4.0;
        m[15] = 1.0;
        assert!(matches!(
            transform_from_tags(ModelTags {
                transformation: Some(&m),
                ..tags()
            }),
            Err(RasterError::NonAffineTransformation)
        ));
        m[2] = 0.0;
        m[15] = 2.0;
        assert!(matches!(
            transform_from_tags(ModelTags {
                transformation: Some(&m),
                ..tags()
            }),
            Err(RasterError::NonAffineTransformation)
        ));
    }

    #[test]
    fn tiepoints_without_scale_are_gcps() {
        let tie = [0.0, 0.0, 0.0, 1.0, 2.0, 0.0, 10.0, 10.0, 0.0, 5.0, 6.0, 0.0];
        assert!(matches!(
            transform_from_tags(ModelTags {
                tiepoints: Some(&tie),
                ..tags()
            }),
            Err(RasterError::GroundControlPoints)
        ));
    }

    #[test]
    fn scale_without_tiepoint_is_refused() {
        let scale = [1.0, 1.0, 0.0];
        assert!(matches!(
            transform_from_tags(ModelTags {
                pixel_scale: Some(&scale),
                ..tags()
            }),
            Err(RasterError::PixelScaleWithoutTiepoint)
        ));
    }

    #[test]
    fn short_or_degenerate_tags_are_refused() {
        let scale = [1.0];
        let tie = [0.0; 6];
        assert!(matches!(
            transform_from_tags(ModelTags {
                pixel_scale: Some(&scale),
                tiepoints: Some(&tie),
                ..tags()
            }),
            Err(RasterError::GeoTagLength { .. })
        ));
        let zero_scale = [0.0, 1.0, 0.0];
        assert!(matches!(
            transform_from_tags(ModelTags {
                pixel_scale: Some(&zero_scale),
                tiepoints: Some(&tie),
                ..tags()
            }),
            Err(RasterError::DegenerateTransform { .. })
        ));
        let nan_scale = [f64::NAN, 1.0, 0.0];
        assert!(matches!(
            transform_from_tags(ModelTags {
                pixel_scale: Some(&nan_scale),
                tiepoints: Some(&tie),
                ..tags()
            }),
            Err(RasterError::DegenerateTransform { .. })
        ));
        let short_matrix = [1.0; 9];
        assert!(matches!(
            transform_from_tags(ModelTags {
                transformation: Some(&short_matrix),
                ..tags()
            }),
            Err(RasterError::GeoTagLength { .. })
        ));
    }
}
