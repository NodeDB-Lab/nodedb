// SPDX-License-Identifier: BUSL-1.1

//! Coordinate reference systems over pgwire: `ST_SRID`, `ST_SetSRID`,
//! `ST_Transform`, the SRID argument of the text constructors, planar
//! measures in a projected SRID, and the one-SRID-per-index rule.
//!
//! Reference coordinates come from PROJ 9.8.1 via pyproj (EPSG:4326 to the
//! target, always_xy). `spatial_crs_open.rs` covers CRSs beyond these.

use crate::harness::TestServer;

/// Largest error accepted against a PROJ reference value, metres.
const MM: f64 = 1e-3;

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

#[tokio::test]
async fn setsrid_then_srid_reads_the_new_srid() {
    let srv = TestServer::start().await;
    let srid = scalar(&srv, "SELECT ST_SRID(ST_SetSRID(ST_Point(1, 2), 32648))").await;
    assert_eq!(srid, "32648");
}

#[tokio::test]
async fn geomfromtext_srid_argument_sets_the_srid() {
    let srv = TestServer::start().await;
    let srid = scalar(&srv, "SELECT ST_SRID(ST_GeomFromText('POINT(1 2)', 3375))").await;
    assert_eq!(srid, "3375");
    let plain = scalar(&srv, "SELECT ST_SRID(ST_GeomFromText('POINT(1 2)'))").await;
    assert_eq!(plain, "4326", "WKT without an SRID argument is WGS 84");
}

#[tokio::test]
async fn mixed_srid_predicate_is_an_error() {
    let srv = TestServer::start().await;
    srv.expect_error(
        "SELECT ST_Intersects(ST_Point(1, 2), ST_SetSRID(ST_Point(1, 2), 32648))",
        "different SRIDs",
    )
    .await;
    srv.expect_error(
        "SELECT ST_Intersects(ST_Point(1, 2), ST_SetSRID(ST_Point(1, 2), 32648))",
        "22000",
    )
    .await;
}

#[tokio::test]
async fn transform_to_an_undefined_srid_names_it() {
    let srv = TestServer::start().await;
    let sql = "SELECT ST_Transform(ST_Point(101.7, 3.1), 999)";
    srv.expect_error(sql, "SRID 999 is not defined").await;
    srv.expect_error(sql, "22000").await;
}

#[tokio::test]
async fn transform_klcc_matches_proj() {
    let srv = TestServer::start().await;
    let cases = [
        (32647, 801_401.794_9, 349_439.510_6),
        (32648, 134_445.592_4, 349_624.727_8),
        (3375, 412_834.468_1, 349_472.917_7),
    ];
    for (srid, east, north) in cases {
        let geom = format!("ST_Transform(ST_Point(101.7117, 3.1579), {srid})");
        let x = number(&srv, &format!("SELECT ST_X({geom})")).await;
        let y = number(&srv, &format!("SELECT ST_Y({geom})")).await;
        assert!(
            (x - east).abs() < MM && (y - north).abs() < MM,
            "SRID {srid}: got ({x}, {y}), want ({east}, {north})"
        );
        let srid_text = scalar(&srv, &format!("SELECT ST_SRID({geom})")).await;
        assert_eq!(srid_text, srid.to_string());
    }
    let x = number(
        &srv,
        "SELECT ST_X(ST_Transform(ST_Point(116.0735, 5.9804), 3376))",
    )
    .await;
    assert!(
        (x - 708_697.150_6).abs() < MM,
        "3376 Kota Kinabalu: got {x}"
    );
}

#[tokio::test]
async fn projected_area_is_planar_square_metres() {
    let srv = TestServer::start().await;
    let area = number(
        &srv,
        "SELECT ST_Area(ST_GeomFromText(\
         'POLYGON((500000 100000, 500100 100000, 500100 100100, 500000 100100, 500000 100000))', \
         32648))",
    )
    .await;
    assert!((area - 10_000.0).abs() < 1e-6, "got {area}");
}

#[tokio::test]
async fn geojson_round_trip_keeps_the_srid() {
    let srv = TestServer::start().await;
    let json = scalar(
        &srv,
        "SELECT ST_AsGeoJSON(ST_SetSRID(ST_Point(1, 2), 3376))",
    )
    .await;
    assert!(json.contains("EPSG:3376"), "{json}");
    let srid = scalar(
        &srv,
        &format!("SELECT ST_SRID(ST_GeomFromGeoJSON('{json}'))"),
    )
    .await;
    assert_eq!(srid, "3376");

    let plain = scalar(&srv, "SELECT ST_AsGeoJSON(ST_Point(1, 2))").await;
    assert!(!plain.contains("crs"), "4326 writes no crs member: {plain}");
}

/// The index SRID is fixed by its first row. A row in another SRID is
/// refused, and the collection keeps only the first row.
#[tokio::test]
async fn second_srid_on_an_indexed_field_is_refused() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION crs_mixed \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO crs_mixed (id, loc) VALUES ('a', ST_Point(101.7, 3.1))")
        .await
        .unwrap();
    srv.expect_error(
        "INSERT INTO crs_mixed (id, loc) \
         VALUES ('b', ST_SetSRID(ST_Point(500000, 0), 32648))",
        "SRID",
    )
    .await;

    let rows = srv.query_rows("SELECT id FROM crs_mixed").await.unwrap();
    assert_eq!(rows.len(), 1, "the refused row was not stored: {rows:?}");
    assert_eq!(rows[0][0], "a");
}

/// A projected index answers `ST_DWithin` in grid metres, and refuses a
/// query geometry in another SRID.
#[tokio::test]
async fn projected_index_is_queried_in_its_own_srid() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION crs_utm \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO crs_utm (id, loc) VALUES \
         ('near', ST_SetSRID(ST_Point(500300, 400), 32648))",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO crs_utm (id, loc) VALUES \
         ('far', ST_SetSRID(ST_Point(510000, 0), 32648))",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM crs_utm \
             WHERE ST_DWithin(loc, ST_SetSRID(ST_Point(500000, 0), 32648), 501)",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["near"], "planar 500 m lies within 501 m only");

    srv.expect_error(
        "SELECT id FROM crs_utm WHERE ST_DWithin(loc, ST_Point(101.7, 3.1), 500)",
        "SRID",
    )
    .await;
}
