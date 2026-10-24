// SPDX-License-Identifier: BUSL-1.1

//! A user-defined CRS in array GEOREFs and GeoTIFF loads: a GEOREF in a
//! user-defined code answers a WGS 84 zone, and a GeoTIFF whose user-defined
//! GeoKeys spell the same parameters loads into it.

use std::collections::BTreeSet;
use std::io::{Cursor, Write};

use nodedb_crs::{CrsContext, CustomCrsSet, Srid, Transformer, parse_proj_string};
use nodedb_types::geometry::point_in_polygon;
use tiff::encoder::{TiffEncoder, colortype};
use tiff::tags::Tag;
use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

const TEST_TM: &str =
    "+proj=tmerc +lat_0=0 +lon_0=101.25 +k=0.99991 +x_0=123456 +y_0=0 +ellps=GRS80 +units=m";

/// The same projection with another central meridian.
const OTHER_TM: &str =
    "+proj=tmerc +lat_0=0 +lon_0=101.5 +k=0.99991 +x_0=123456 +y_0=0 +ellps=GRS80 +units=m";

const SIZE: u32 = 8;
const PIXEL_M: f64 = 30.0;
const EAST0: f64 = 124_456.0;
const NORTH0: f64 = 50_000.0;

/// GeoKey ids.
const GT_MODEL_TYPE: u16 = 1024;
const GT_RASTER_TYPE: u16 = 1025;
const GEOGRAPHIC_TYPE: u16 = 2048;
const GEODETIC_DATUM: u16 = 2050;
const ELLIPSOID: u16 = 2056;
const PROJECTED_CS_TYPE: u16 = 3072;
const PROJECTION: u16 = 3074;
const PROJ_METHOD: u16 = 3075;
const PROJ_LINEAR_UNITS: u16 = 3076;
const PROJ_NAT_ORIGIN_LONG: u16 = 3080;
const PROJ_NAT_ORIGIN_LAT: u16 = 3081;
const PROJ_FALSE_EASTING: u16 = 3082;
const PROJ_FALSE_NORTHING: u16 = 3083;
const PROJ_SCALE_AT_NAT_ORIGIN: u16 = 3092;
const USER_DEFINED: u16 = 32767;
const DOUBLE_PARAMS: u16 = 34736;

async fn define(srv: &TestServer, code: u32, proj: &str) {
    let sql = format!("CREATE SPATIAL_REF_SYS {code} (NAME 'grid {code}', PROJ '{proj}')");
    srv.exec(&sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

async fn create_array(srv: &TestServer, name: &str, code: u32) {
    let sql = format!(
        "CREATE ARRAY {name} DIMS (row INT64 [0..{last}], col INT64 [0..{last}]) \
         ATTRS (b1 UINT16) TILE_EXTENTS (8, 8) \
         GEOREF (SRID {code}, X col, Y row, TRANSFORM ({EAST0}, {PIXEL_M}, 0, {NORTH0}, 0, \
         -{PIXEL_M}))",
        last = SIZE - 1
    );
    srv.exec(&sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

fn to_wgs84(code: u32) -> Transformer {
    let srid = Srid::new(code).expect("srid");
    let mut set = CustomCrsSet::new();
    set.define(parse_proj_string(srid, "grid", TEST_TM).expect("parses"))
        .expect("defines");
    CrsContext::new(&set)
        .transformer(srid, Srid::WGS84)
        .expect("defined")
}

fn lon_lat(t: &Transformer, east: f64, north: f64) -> [f64; 2] {
    let [lon, lat, _] = t.apply([east, north, 0.0]).expect("inside");
    [lon, lat]
}

/// World centre of pixel `(row, col)`.
fn centre(row: u32, col: u32) -> (f64, f64) {
    (
        EAST0 + (f64::from(col) + 0.5) * PIXEL_M,
        NORTH0 - (f64::from(row) + 0.5) * PIXEL_M,
    )
}

/// A GEOREF in a user-defined code answers a WGS 84 zone with the cells
/// whose centre lies inside it.
#[tokio::test]
async fn a_user_defined_georef_answers_a_wgs84_zone() {
    let srv = TestServer::start().await;
    define(&srv, 910_161, TEST_TM).await;
    create_array(&srv, "srs_zone", 910_161).await;
    for row in 0..SIZE {
        let cells: Vec<String> = (0..SIZE)
            .map(|col| format!("COORDS ({row}, {col}) VALUES (1)"))
            .collect();
        srv.exec(&format!("INSERT INTO ARRAY srs_zone {}", cells.join(", ")))
            .await
            .expect("INSERT");
    }
    srv.exec("SELECT ARRAY_FLUSH('srs_zone')")
        .await
        .expect("ARRAY_FLUSH");

    let t = to_wgs84(910_161);
    // The left half of the grid, as a WGS 84 ring from projected corners.
    let mut ring: Vec<[f64; 2]> = [
        (EAST0 + 5.0, NORTH0 - 5.0),
        (EAST0 + 125.0, NORTH0 - 5.0),
        (EAST0 + 125.0, NORTH0 - 235.0),
        (EAST0 + 5.0, NORTH0 - 235.0),
    ]
    .iter()
    .map(|(e, n)| lon_lat(&t, *e, *n))
    .collect();
    ring.push(ring[0]);
    let mut expected = BTreeSet::new();
    for row in 0..SIZE {
        for col in 0..SIZE {
            let (e, n) = centre(row, col);
            let [lon, lat] = lon_lat(&t, e, n);
            if point_in_polygon(lon, lat, &ring) {
                expected.insert((row, col));
            }
        }
    }
    assert_eq!(expected.len(), 32, "the zone holds the left half");

    let points: Vec<String> = ring
        .iter()
        .map(|p| format!("{:?} {:?}", p[0], p[1]))
        .collect();
    let zone = format!("'SRID=4326;POLYGON(({}))'", points.join(", "));
    let sql =
        format!("SELECT * FROM ARRAY_AGG('srs_zone', 'b1', 'count', NULL, NULL, {zone}, NULL)");
    let rows = srv
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let count: f64 = rows
        .first()
        .and_then(|r| r.get("result"))
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("{sql}: no numeric result in {rows:?}"));
    assert_eq!(count, expected.len() as f64);
}

/// An 8 x 8 u16 GeoTIFF on the grid of [`create_array`] whose user-defined
/// GeoKeys spell [`TEST_TM`]: Transverse Mercator over the GRS 1980
/// ellipsoid, no datum code.
fn user_defined_tiff() -> tempfile::NamedTempFile {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut encoder = TiffEncoder::new(&mut cursor).expect("encoder");
        let mut image = encoder
            .new_image::<colortype::Gray16>(SIZE, SIZE)
            .expect("image");
        let dir = image.encoder();
        dir.write_tag(Tag::ModelPixelScaleTag, &[PIXEL_M, PIXEL_M, 0.0][..])
            .expect("tag");
        dir.write_tag(
            Tag::ModelTiepointTag,
            &[0.0, 0.0, 0.0, EAST0, NORTH0, 0.0][..],
        )
        .expect("tag");
        let short = |key: u16, value: u16| [key, 0, 1, value];
        let double = |key: u16, index: u16| [key, DOUBLE_PARAMS, 1, index];
        let entries = [
            short(GT_MODEL_TYPE, 1),
            short(GT_RASTER_TYPE, 1),
            short(GEOGRAPHIC_TYPE, USER_DEFINED),
            short(GEODETIC_DATUM, USER_DEFINED),
            short(ELLIPSOID, 7019),
            short(PROJECTED_CS_TYPE, USER_DEFINED),
            short(PROJECTION, USER_DEFINED),
            short(PROJ_METHOD, 1),
            short(PROJ_LINEAR_UNITS, 9001),
            double(PROJ_NAT_ORIGIN_LONG, 0),
            double(PROJ_NAT_ORIGIN_LAT, 1),
            double(PROJ_FALSE_EASTING, 2),
            double(PROJ_FALSE_NORTHING, 3),
            double(PROJ_SCALE_AT_NAT_ORIGIN, 4),
        ];
        let count = u16::try_from(entries.len()).expect("few keys");
        let mut keys = vec![1u16, 1, 0, count];
        keys.extend(entries.iter().flatten());
        dir.write_tag(Tag::GeoKeyDirectoryTag, &keys[..])
            .expect("tag");
        dir.write_tag(
            Tag::GeoDoubleParamsTag,
            &[101.25, 0.0, 123_456.0, 0.0, 0.99991][..],
        )
        .expect("tag");
        let samples: Vec<u16> = (0..SIZE * SIZE)
            .map(|i| u16::try_from(i + 1).expect("small"))
            .collect();
        image.write_data(&samples).expect("pixels");
    }
    let mut file = tempfile::Builder::new()
        .suffix(".tif")
        .tempfile()
        .expect("temp file");
    file.write_all(&cursor.into_inner()).expect("writes");
    file.flush().expect("flushes");
    file
}

/// Run a GeoTIFF COPY: the `COPY <n>` count, or the SQLSTATE it refuses.
async fn copy(srv: &TestServer, array: &str, path: &str) -> Result<u64, String> {
    let sql = format!("COPY {array} FROM '{path}' WITH (FORMAT geotiff)");
    let messages = srv
        .client
        .simple_query(&sql)
        .await
        .map_err(|e| match e.as_db_error() {
            Some(db) => format!("{}: {}", db.code().code(), db.message()),
            None => e.to_string(),
        })?;
    Ok(messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(rows) => Some(*rows),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no command tag")))
}

/// A GeoTIFF whose user-defined GeoKeys equal a user-defined CRS loads
/// into an array in that code. The same file is refused where the code
/// holds other parameters.
#[tokio::test]
async fn user_defined_geokeys_match_a_user_defined_crs() {
    let srv = TestServer::start().await;
    define(&srv, 910_171, TEST_TM).await;
    define(&srv, 910_172, OTHER_TM).await;
    create_array(&srv, "srs_load", 910_171).await;
    create_array(&srv, "srs_other", 910_172).await;
    let file = user_defined_tiff();
    let path = file.path().to_string_lossy().into_owned();

    let loaded = copy(&srv, "srs_load", &path)
        .await
        .unwrap_or_else(|e| panic!("COPY srs_load: {e}"));
    assert_eq!(loaded, u64::from(SIZE * SIZE));

    let refused = copy(&srv, "srs_other", &path)
        .await
        .expect_err("the file CRS is SRID 910171, not 910172");
    assert!(refused.starts_with("22023"), "{refused}");
    assert!(refused.contains("SRID 910171"), "{refused}");
}
