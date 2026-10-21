// SPDX-License-Identifier: BUSL-1.1

//! The open CRS set over pgwire: `ST_Transform` to EPSG CRSs past the
//! Malaysian five, the NodeDB-defined MODIS sinusoidal, a GEOREF in UTM 50N
//! with a WGS 84 zone, the refusal of an undefined SRID, and the GeoJSON
//! `crs` member of a geographic CRS other than WGS 84.
//!
//! Reference coordinates come from PROJ 9.8.1 via pyproj 3.8.0 over EPSG
//! v12.029, always_xy, as recorded in the `nodedb-crs` reference fixtures.

use std::collections::BTreeSet;

use nodedb_crs::{CrsContext, Srid, Transformer};
use nodedb_types::geometry::point_in_polygon;

use crate::harness::TestServer;

/// Largest error accepted against a projected reference value, metres.
const MM: f64 = 1e-3;

/// Largest error accepted against a geographic reference value, degrees.
const DEG: f64 = 1e-8;

async fn scalar(srv: &TestServer, sql: &str) -> String {
    let rows = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    assert_eq!(rows.len(), 1, "`{sql}` must return one row, got {rows:?}");
    rows[0][0].clone()
}

async fn number(srv: &TestServer, sql: &str) -> f64 {
    let text = scalar(srv, sql).await;
    text.parse()
        .unwrap_or_else(|_| panic!("`{sql}` must return a number, got {text:?}"))
}

/// The `(x, y)` and SRID of `geom`, a geometry expression.
async fn position(srv: &TestServer, geom: &str) -> (f64, f64, String) {
    let x = number(srv, &format!("SELECT ST_X({geom})")).await;
    let y = number(srv, &format!("SELECT ST_Y({geom})")).await;
    let srid = scalar(srv, &format!("SELECT ST_SRID({geom})")).await;
    (x, y, srid)
}

#[tokio::test]
async fn transform_from_wgs84_matches_proj() {
    let srv = TestServer::start().await;
    // (target SRID, lon, lat, x, y)
    let cases = [
        (
            32649,
            110.018,
            2.95,
            390_866.472_639_102_1,
            326_114.662_299_186_05,
        ),
        (
            32650,
            114.224,
            2.95,
            191_386.377_688_716_05,
            326_451.516_441_741_2,
        ),
        (
            3857,
            100.39625,
            3.975,
            11_176_059.427_554_19,
            442_850.369_085_527_5,
        ),
        (
            3380,
            100.87495,
            3.205,
            -91_983.030_531_970_26,
            3_439.612_717_755_233_5,
        ),
        (
            6933,
            100.39625,
            3.975,
            9_686_860.713_639_07,
            506_707.993_165_587_54,
        ),
        (
            990001,
            100.0,
            70.0,
            3_803_094.761_415_982,
            7_783_653.638_365_662,
        ),
    ];
    for (srid, lon, lat, east, north) in cases {
        let geom = format!("ST_Transform(ST_Point({lon:?}, {lat:?}), {srid})");
        let (x, y, srid_text) = position(&srv, &geom).await;
        assert!(
            (x - east).abs() < MM && (y - north).abs() < MM,
            "SRID {srid}: got ({x}, {y}), want ({east}, {north})"
        );
        assert_eq!(srid_text, srid.to_string());
    }
}

/// GDM2000 to WGS 84 is a null shift, so the coordinates pass unchanged.
#[tokio::test]
async fn transform_to_gdm2000_keeps_the_coordinates() {
    let srv = TestServer::start().await;
    let geom = "ST_Transform(ST_Point(101.49875, 2.101457), 4742)";
    let (x, y, srid) = position(&srv, geom).await;
    assert!(
        (x - 101.49875).abs() < DEG && (y - 2.101457).abs() < DEG,
        "got ({x}, {y})"
    );
    assert_eq!(srid, "4742");
}

/// The RSO Malaya Grid (metre) on Kertau (RSO) back to WGS 84, through the
/// Kertau 1968 Helmert shift.
#[tokio::test]
async fn transform_from_rso_malaya_grid_matches_proj() {
    let srv = TestServer::start().await;
    let geom = "ST_Transform(ST_SetSRID(\
                ST_Point(230287.07313047396, 440448.09783019935), 3168), 4326)";
    let (x, y, srid) = position(&srv, geom).await;
    assert!(
        (x - 100.064_258_238_970_55).abs() < DEG && (y - 3.974_747_627_683_754_6).abs() < DEG,
        "got ({x}, {y})"
    );
    assert_eq!(srid, "4326");
}

#[tokio::test]
async fn an_undefined_srid_is_refused() {
    let srv = TestServer::start().await;
    let undefined = "SELECT ST_Transform(ST_Point(101.7, 3.1), 999)";
    srv.expect_error(undefined, "SRID 999 is not defined").await;
    srv.expect_error(undefined, "22000").await;
    let zero = "SELECT ST_Transform(ST_Point(101.7, 3.1), 0)";
    srv.expect_error(zero, "SRID 0 is outside 1..=998999").await;
    srv.expect_error(zero, "22000").await;
    srv.expect_error(
        "SELECT ST_SRID(ST_SetSRID(ST_Point(1, 2), 999))",
        "SRID 999 is not defined",
    )
    .await;
}

/// A geographic CRS other than WGS 84 still writes the GeoJSON `crs`
/// member, so a round trip keeps the SRID.
#[tokio::test]
async fn gdm2000_geojson_keeps_the_crs_member() {
    let srv = TestServer::start().await;
    let json = scalar(
        &srv,
        "SELECT ST_AsGeoJSON(ST_SetSRID(ST_Point(101.7, 3.1), 4742))",
    )
    .await;
    assert!(json.contains("EPSG:4742"), "{json}");
    let srid = scalar(
        &srv,
        &format!("SELECT ST_SRID(ST_GeomFromGeoJSON('{json}'))"),
    )
    .await;
    assert_eq!(srid, "4742");
}

const SIZE: i64 = 16;
const PIXEL_M: f64 = 50.0;
const EAST0: f64 = 600_000.0;
const NORTH0: f64 = 662_000.0;

/// World centre of pixel `(row, col)`, UTM 50N metres.
fn centre(row: i64, col: i64) -> [f64; 2] {
    [
        EAST0 + (col as f64 + 0.5) * PIXEL_M,
        NORTH0 - (row as f64 + 0.5) * PIXEL_M,
    ]
}

fn utm50n_to_wgs84() -> Transformer {
    let utm50n = Srid::new(32650).expect("srid");
    CrsContext::builtin_only()
        .transformer(utm50n, Srid::WGS84)
        .expect("UTM 50N and WGS 84 are built in")
}

fn to_lon_lat(to_wgs84: &Transformer, p: [f64; 2]) -> [f64; 2] {
    let [lon, lat, _] = to_wgs84.apply([p[0], p[1], 0.0]).expect("inside UTM 50N");
    [lon, lat]
}

/// A closed WGS 84 ring over the middle of the grid, from UTM corners.
fn zone_ring() -> Vec<[f64; 2]> {
    let to_wgs84 = utm50n_to_wgs84();
    let mut ring: Vec<[f64; 2]> = [
        [EAST0 + 120.0, NORTH0 - 90.0],
        [EAST0 + 610.0, NORTH0 - 170.0],
        [EAST0 + 540.0, NORTH0 - 650.0],
        [EAST0 + 160.0, NORTH0 - 560.0],
    ]
    .iter()
    .map(|p| to_lon_lat(&to_wgs84, *p))
    .collect();
    ring.push(ring[0]);
    ring
}

fn expected_cells(ring: &[[f64; 2]]) -> BTreeSet<(i64, i64)> {
    let to_wgs84 = utm50n_to_wgs84();
    let mut out = BTreeSet::new();
    for row in 0..SIZE {
        for col in 0..SIZE {
            let [lon, lat] = to_lon_lat(&to_wgs84, centre(row, col));
            if point_in_polygon(lon, lat, ring) {
                out.insert((row, col));
            }
        }
    }
    out
}

/// A GEOREF in UTM 50N, an SRID outside the Malaysian five, answers a WGS 84
/// zone with the cells whose centre lies inside it.
#[tokio::test]
async fn utm50n_georef_answers_a_wgs84_zone() {
    let srv = TestServer::start().await;
    srv.exec(&format!(
        "CREATE ARRAY kk DIMS (row INT64 [0..{last}], col INT64 [0..{last}]) \
         ATTRS (b1 FLOAT64) TILE_EXTENTS (8, 8) TILE_ORDER ROW_MAJOR \
         GEOREF (SRID 32650, X col, Y row, TRANSFORM ({EAST0}, {PIXEL_M}, 0, {NORTH0}, 0, \
         -{PIXEL_M}))",
        last = SIZE - 1
    ))
    .await
    .expect("CREATE ARRAY kk");
    for row in 0..SIZE {
        let cells: Vec<String> = (0..SIZE)
            .map(|col| format!("COORDS ({row}, {col}) VALUES (1.0)"))
            .collect();
        srv.exec(&format!("INSERT INTO ARRAY kk {}", cells.join(", ")))
            .await
            .expect("INSERT kk");
    }
    srv.exec("SELECT ARRAY_FLUSH('kk')")
        .await
        .expect("ARRAY_FLUSH");

    let ring = zone_ring();
    let expected = expected_cells(&ring);
    assert!(
        expected.len() > 20,
        "the zone holds cells: {}",
        expected.len()
    );
    let points: Vec<String> = ring
        .iter()
        .map(|p| format!("{:?} {:?}", p[0], p[1]))
        .collect();
    let zone = format!("'SRID=4326;POLYGON(({}))'", points.join(", "));
    let sql = format!("SELECT * FROM ARRAY_AGG('kk', 'b1', 'count', NULL, NULL, {zone}, NULL)");
    let rows = srv
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: one scalar row: {rows:?}");
    let count: f64 = rows[0]
        .get("result")
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("{sql}: no numeric result in {rows:?}"));
    assert_eq!(count, expected.len() as f64);
}
