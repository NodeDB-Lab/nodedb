// SPDX-License-Identifier: BUSL-1.1

//! Geodesic measures, overlay, buffer, validity and linear referencing.
//!
//! A geographic CRS measures geodesics on its ellipsoid. A projected CRS
//! measures in grid metres. The reference values come from GeographicLib
//! (pyproj `Geod(ellps="WGS84")`) and from shapely for the planar overlay.

use crate::harness::TestServer;

async fn scalar(srv: &TestServer, sql: &str) -> String {
    let rows = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    assert_eq!(rows.len(), 1, "`{sql}` must return one row, got {rows:?}");
    rows[0][0].clone()
}

async fn number(srv: &TestServer, sql: &str) -> f64 {
    let cell = scalar(srv, sql).await;
    cell.parse()
        .unwrap_or_else(|_| panic!("`{sql}` must return a number, got {cell:?}"))
}

fn is_true(cell: &str) -> bool {
    matches!(cell.to_lowercase().as_str(), "t" | "true")
}

/// A square in UTM 48N, metres.
fn utm_square(x0: f64, y0: f64, side: f64) -> String {
    let (x1, y1) = (x0 + side, y0 + side);
    format!(
        "ST_GeomFromText('POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))', 32648)"
    )
}

#[tokio::test]
async fn geographic_measures_are_geodesic() {
    let srv = TestServer::start().await;
    let d = number(&srv, "SELECT ST_Distance(ST_Point(0, 0), ST_Point(0, 1))").await;
    assert!((d - 110_574.388_557_798_78).abs() < 1e-6, "{d}");

    let square = "ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')";
    let area = number(&srv, &format!("SELECT ST_Area({square})")).await;
    assert!((area - 12_308_778_361.469).abs() < 1.0, "{area}");
    let perimeter = number(&srv, &format!("SELECT ST_Perimeter({square})")).await;
    assert!((perimeter - 443_770.917_248).abs() < 1e-3, "{perimeter}");

    let length = number(
        &srv,
        "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0,0 1)'))",
    )
    .await;
    assert!((length - 110_574.388_557_798_78).abs() < 1e-6, "{length}");
}

/// The R-tree search box covers the geodesic reach in latitude: a point
/// 0.904 degrees north of the equator lies 99,959 m away, inside 100 km.
#[tokio::test]
async fn indexed_dwithin_reaches_the_full_geodesic_distance() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ops_reach COLUMNS (id TEXT, loc GEOMETRY) WITH (engine='spatial')")
        .await
        .unwrap();
    srv.exec("INSERT INTO ops_reach (id, loc) VALUES ('north', ST_Point(0, 0.904))")
        .await
        .unwrap();
    srv.exec("INSERT INTO ops_reach (id, loc) VALUES ('beyond', ST_Point(0, 0.91))")
        .await
        .unwrap();
    let rows = srv
        .query_rows("SELECT id FROM ops_reach WHERE ST_DWithin(loc, ST_Point(0, 0), 100000)")
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["north"]);
}

/// Two 10 m squares overlapping by a 5 m square.
#[tokio::test]
async fn overlay_operations() {
    let srv = TestServer::start().await;
    let (a, b) = (utm_square(0.0, 0.0, 10.0), utm_square(5.0, 5.0, 10.0));
    let cases = [
        ("ST_Union", 175.0),
        ("ST_Intersection", 25.0),
        ("ST_Difference", 75.0),
        ("ST_SymDifference", 150.0),
    ];
    for (function, want) in cases {
        let area = number(&srv, &format!("SELECT ST_Area({function}({a}, {b}))")).await;
        assert!((area - want).abs() < 1e-6, "{function}: {area}");
        let srid = scalar(&srv, &format!("SELECT ST_SRID({function}({a}, {b}))")).await;
        assert_eq!(srid, "32648", "{function} keeps the SRID");
    }
    let disjoint = utm_square(100.0, 100.0, 1.0);
    let empty = scalar(
        &srv,
        &format!("SELECT ST_IsEmpty(ST_Intersection({a}, {disjoint}))"),
    )
    .await;
    assert!(is_true(&empty), "disjoint intersection is EMPTY: {empty}");
}

#[tokio::test]
async fn buffer_in_projected_and_geographic_crs() {
    let srv = TestServer::start().await;
    // A 10 m buffer of a point: 32 segments approximate a circle of
    // area 314.16 m² from inside.
    let planar = number(
        &srv,
        "SELECT ST_Area(ST_Buffer(ST_GeomFromText('POINT(500000 0)', 32648), 10))",
    )
    .await;
    assert!((305.0..=314.2).contains(&planar), "{planar}");

    // A 1 km buffer in WGS 84 is metres on the ground, not degrees.
    let geographic = number(
        &srv,
        "SELECT ST_Area(ST_Buffer(ST_Point(101.7, 3.1), 1000))",
    )
    .await;
    let circle = std::f64::consts::PI * 1.0e6;
    assert!(
        (geographic - circle).abs() / circle < 0.02,
        "{geographic} vs {circle}"
    );
    let srid = scalar(
        &srv,
        "SELECT ST_SRID(ST_Buffer(ST_Point(101.7, 3.1), 1000))",
    )
    .await;
    assert_eq!(srid, "4326");
}

#[tokio::test]
async fn validity_reasons() {
    let srv = TestServer::start().await;
    let valid = scalar(
        &srv,
        "SELECT ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))",
    )
    .await;
    assert_eq!(valid, "Valid Geometry");

    let bowtie = "ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 1,0 0))')";
    let reason = scalar(&srv, &format!("SELECT ST_IsValidReason({bowtie})")).await;
    assert!(reason.contains("crosses itself"), "{reason}");
    assert!(!is_true(
        &scalar(&srv, &format!("SELECT ST_IsValid({bowtie})")).await
    ));

    let repaired = scalar(&srv, &format!("SELECT ST_IsValid(ST_MakeValid({bowtie}))")).await;
    assert!(
        is_true(&repaired),
        "ST_MakeValid output is valid: {repaired}"
    );

    // Winding order is not a validity rule: a clockwise shell is valid.
    let clockwise = scalar(
        &srv,
        "SELECT ST_IsValid(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
    )
    .await;
    assert!(is_true(&clockwise));
}

#[tokio::test]
async fn relate_and_named_predicates() {
    let srv = TestServer::start().await;
    let (a, b) = (utm_square(0.0, 0.0, 10.0), utm_square(5.0, 5.0, 10.0));
    let matrix = scalar(&srv, &format!("SELECT ST_Relate({a}, {b})")).await;
    assert_eq!(matrix, "212101212");
    let pattern = scalar(&srv, &format!("SELECT ST_Relate({a}, {b}, '2********')")).await;
    assert!(is_true(&pattern));
    assert!(is_true(
        &scalar(&srv, &format!("SELECT ST_Overlaps({a}, {b})")).await
    ));
    let inner = utm_square(2.0, 2.0, 2.0);
    assert!(is_true(
        &scalar(&srv, &format!("SELECT ST_Covers({a}, {inner})")).await
    ));
    assert!(!is_true(
        &scalar(&srv, &format!("SELECT ST_Touches({a}, {inner})")).await
    ));
}

#[tokio::test]
async fn shape_operations() {
    let srv = TestServer::start().await;
    let a = utm_square(0.0, 0.0, 10.0);
    let centroid = scalar(&srv, &format!("SELECT ST_AsText(ST_Centroid({a}))")).await;
    assert_eq!(centroid, "POINT(5 5)");

    let hull_area = number(
        &srv,
        "SELECT ST_Area(ST_ConvexHull(ST_GeomFromText(\
         'MULTIPOINT((0 0),(10 0),(10 10),(0 10),(5 5))', 32648)))",
    )
    .await;
    assert!((hull_area - 100.0).abs() < 1e-9, "{hull_area}");

    let simplified = scalar(
        &srv,
        "SELECT ST_NPoints(ST_Simplify(ST_GeomFromText(\
         'LINESTRING(0 0,5 0.1,10 0)', 32648), 1))",
    )
    .await;
    assert_eq!(simplified, "2");

    let shifted = scalar(
        &srv,
        "SELECT ST_AsText(ST_ShiftLongitude(ST_Point(-170, 1)))",
    )
    .await;
    assert_eq!(shifted, "POINT(190 1)");
}

#[tokio::test]
async fn linear_referencing() {
    let srv = TestServer::start().await;
    let line = "ST_GeomFromText('LINESTRING(0 0,10 0)', 32648)";
    let mid = scalar(
        &srv,
        &format!("SELECT ST_AsText(ST_LineInterpolatePoint({line}, 0.5))"),
    )
    .await;
    assert_eq!(mid, "POINT(5 0)");

    let measured = "ST_GeomFromText('LINESTRING M (0 0 0,10 0 100)', 32648)";
    let along = scalar(
        &srv,
        &format!("SELECT ST_AsText(ST_LocateAlong({measured}, 25))"),
    )
    .await;
    assert!(along.contains("2.5 0 25"), "{along}");
    let m = number(
        &srv,
        &format!("SELECT ST_InterpolatePoint({measured}, ST_GeomFromText('POINT(7 0)', 32648))"),
    )
    .await;
    assert!((m - 70.0).abs() < 1e-9, "{m}");
}

#[tokio::test]
async fn tile_envelope_is_web_mercator() {
    let srv = TestServer::start().await;
    let srid = scalar(&srv, "SELECT ST_SRID(ST_TileEnvelope(0, 0, 0))").await;
    assert_eq!(srid, "3857");
    let area = number(&srv, "SELECT ST_Area(ST_TileEnvelope(0, 0, 0))").await;
    let side = 2.0 * 20_037_508.342_789_244;
    assert!((area - side * side).abs() / (side * side) < 1e-12, "{area}");
}

/// Mixed SRIDs in a two-geometry function fail the statement.
#[tokio::test]
async fn mixed_srids_are_refused() {
    let srv = TestServer::start().await;
    srv.expect_error(
        &format!(
            "SELECT ST_Union({}, ST_Point(0, 0))",
            utm_square(0.0, 0.0, 1.0)
        ),
        "SRID",
    )
    .await;
}
