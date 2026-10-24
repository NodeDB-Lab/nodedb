// SPDX-License-Identifier: Apache-2.0

//! Georeference from the GeoTIFF tags: geotransform, CRS, and nodata.

use nodedb_crs::CrsContext;
use nodedb_raster::RasterError;
use nodedb_raster::tiff::geokeys::{
    GEOGRAPHIC_TYPE, GT_MODEL_TYPE, GT_RASTER_TYPE, PROJECTED_CS_TYPE,
};
use nodedb_types::geometry::Srid;
use tiff::encoder::colortype::Gray16;
use tiff::tags::Tag;

use crate::fixtures::{Extra, StripSpec, encode_strips, geokeys, planes, strip_spec};
use crate::image::open;

const W: u32 = 6;
const H: u32 = 4;

fn with_tags(extras: &[Extra]) -> Vec<u8> {
    let data = planes::<u16>(W, H, 1);
    let spec = StripSpec {
        extras,
        ..strip_spec(W, H)
    };
    encode_strips::<Gray16>(&spec, &data[0])
}

fn utm_tiepoint(raster_type: u16) -> Vec<Extra> {
    vec![
        Extra::Doubles(Tag::ModelPixelScaleTag, vec![30.0, 30.0, 0.0]),
        Extra::Doubles(
            Tag::ModelTiepointTag,
            vec![0.0, 0.0, 0.0, 600_000.0, 450_000.0, 0.0],
        ),
        Extra::Shorts(
            Tag::GeoKeyDirectoryTag,
            geokeys(&[
                (GT_MODEL_TYPE, 1),
                (GT_RASTER_TYPE, raster_type),
                (PROJECTED_CS_TYPE, 32647),
            ]),
        ),
    ]
}

#[test]
fn tiepoint_and_scale_give_a_north_up_transform() {
    let reader = open(&with_tags(&utm_tiepoint(1))).expect("opens");
    let gt = reader.transform().expect("georeferenced");
    assert_eq!(gt.0, [600_000.0, 30.0, 0.0, 450_000.0, 0.0, -30.0]);
    assert_eq!(
        reader.srid(&CrsContext::builtin_only()).expect("CRS maps"),
        Some(Srid::new(32647).unwrap())
    );
}

#[test]
fn pixel_is_point_shifts_the_origin_half_a_pixel() {
    let reader = open(&with_tags(&utm_tiepoint(2))).expect("opens");
    let gt = reader.transform().expect("georeferenced");
    assert_eq!(gt.0, [599_985.0, 30.0, 0.0, 450_015.0, 0.0, -30.0]);
}

#[test]
fn model_transformation_gives_a_rotated_transform() {
    let matrix = vec![
        0.5, 0.1, 0.0, 101.0, //
        -0.1, -0.5, 0.0, 3.5, //
        0.0, 0.0, 0.0, 0.0, //
        0.0, 0.0, 0.0, 1.0,
    ];
    let extras = [
        Extra::Doubles(Tag::ModelTransformationTag, matrix),
        Extra::Shorts(
            Tag::GeoKeyDirectoryTag,
            geokeys(&[(GT_MODEL_TYPE, 2), (GEOGRAPHIC_TYPE, 4326)]),
        ),
    ];
    let reader = open(&with_tags(&extras)).expect("opens");
    let gt = reader.transform().expect("georeferenced");
    assert_eq!(gt.0, [101.0, 0.5, 0.1, 3.5, -0.1, -0.5]);
    assert_eq!(
        reader.srid(&CrsContext::builtin_only()).expect("CRS maps"),
        Some(Srid::WGS84)
    );
}

#[test]
fn gdal_nodata_is_read() {
    let reader = open(&with_tags(&[Extra::Ascii(
        Tag::GdalNodata,
        "-9999".to_owned(),
    )]))
    .expect("opens");
    assert_eq!(reader.nodata(), Some(-9999.0));

    let reader = open(&with_tags(&[Extra::Ascii(
        Tag::GdalNodata,
        "nan ".to_owned(),
    )]))
    .expect("opens");
    assert!(reader.nodata().is_some_and(f64::is_nan));

    let malformed = open(&with_tags(&[Extra::Ascii(
        Tag::GdalNodata,
        "none".to_owned(),
    )]));
    assert!(matches!(malformed, Err(RasterError::InvalidNodata { .. })));
}

#[test]
fn a_plain_tiff_has_no_georeference() {
    let reader = open(&with_tags(&[])).expect("opens");
    assert_eq!(reader.transform(), None);
    assert_eq!(
        reader.srid(&CrsContext::builtin_only()).expect("no CRS"),
        None
    );
    assert_eq!(reader.nodata(), None);
    assert!(reader.geokeys().is_none());
}

#[test]
fn ground_control_points_are_refused() {
    let extras = [Extra::Doubles(
        Tag::ModelTiepointTag,
        vec![
            0.0, 0.0, 0.0, 100.0, 200.0, 0.0, //
            5.0, 3.0, 0.0, 150.0, 170.0, 0.0,
        ],
    )];
    assert!(matches!(
        open(&with_tags(&extras)),
        Err(RasterError::GroundControlPoints)
    ));
}

#[test]
fn an_undefined_crs_is_a_typed_error() {
    let extras = [Extra::Shorts(
        Tag::GeoKeyDirectoryTag,
        geokeys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 999)]),
    )];
    let reader = open(&with_tags(&extras)).expect("opens");
    assert!(matches!(
        reader.srid(&CrsContext::builtin_only()),
        Err(RasterError::UnknownCrs(_))
    ));
}

/// Any EPSG projected CRS maps, such as UTM 33N.
#[test]
fn an_epsg_projected_crs_maps() {
    let extras = [Extra::Shorts(
        Tag::GeoKeyDirectoryTag,
        geokeys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32633)]),
    )];
    let reader = open(&with_tags(&extras)).expect("opens");
    assert_eq!(
        reader.srid(&CrsContext::builtin_only()).expect("CRS maps"),
        Some(Srid::new(32633).unwrap())
    );
}

#[test]
fn a_malformed_geokey_directory_fails_open() {
    let extras = [Extra::Shorts(
        Tag::GeoKeyDirectoryTag,
        vec![1, 1, 0, 9, 1024],
    )];
    assert!(matches!(
        open(&with_tags(&extras)),
        Err(RasterError::GeoKeys(_))
    ));
}
