// SPDX-License-Identifier: BUSL-1.1

//! SRID zones and cell geometry over georeferenced arrays.
//!
//! Two arrays hold the same 32x32 grid of 30 m UTM 47N pixels: `zpx` as
//! pixel indices with a TRANSFORM, `zwd` as pixel-centre world coordinates.
//! A zone drawn in WGS 84 must select the same cells from both. The
//! expected cells come from projecting each centre to WGS 84 in the test.

use std::collections::BTreeSet;

use nodedb_crs::{CrsContext, Srid, Transformer};
use nodedb_types::geometry::point_in_polygon;

use crate::harness::TestServer;

const SIZE: i64 = 32;
const PIXEL_M: f64 = 30.0;
const EAST0: f64 = 550_000.0;
const NORTH0: f64 = 664_000.0;

/// The UTM 47N to WGS 84 transformer.
fn utm47n_to_wgs84() -> Transformer {
    let utm47n = Srid::new(32647).expect("srid");
    CrsContext::builtin_only()
        .transformer(utm47n, Srid::WGS84)
        .expect("UTM 47N and WGS 84 are built in")
}

/// One UTM 47N position in WGS 84 degrees.
fn to_lon_lat(to_wgs84: &Transformer, p: [f64; 2]) -> [f64; 2] {
    let [lon, lat, _] = to_wgs84.apply([p[0], p[1], 0.0]).expect("inside UTM 47N");
    [lon, lat]
}

/// World centre of pixel `(row, col)`, UTM 47N metres.
fn centre(row: i64, col: i64) -> [f64; 2] {
    [
        EAST0 + (col as f64 + 0.5) * PIXEL_M,
        NORTH0 - (row as f64 + 0.5) * PIXEL_M,
    ]
}

fn cell_value(row: i64, col: i64) -> f64 {
    (row * SIZE + col) as f64
}

/// A WGS 84 quadrilateral over the middle of the grid, from UTM corners.
fn zone_ring() -> Vec<[f64; 2]> {
    let to_wgs84 = utm47n_to_wgs84();
    let utm = [
        [EAST0 + 140.0, NORTH0 - 110.0],
        [EAST0 + 830.0, NORTH0 - 260.0],
        [EAST0 + 700.0, NORTH0 - 880.0],
        [EAST0 + 210.0, NORTH0 - 760.0],
    ];
    let mut ring: Vec<[f64; 2]> = utm.iter().map(|p| to_lon_lat(&to_wgs84, *p)).collect();
    ring.push(ring[0]);
    ring
}

/// `ring` as an EWKT literal in SRID 4326.
fn ewkt(ring: &[[f64; 2]]) -> String {
    let points: Vec<String> = ring
        .iter()
        .map(|p| format!("{:?} {:?}", p[0], p[1]))
        .collect();
    format!("'SRID=4326;POLYGON(({}))'", points.join(", "))
}

/// The cells whose centre lies inside `ring`.
fn expected_cells(ring: &[[f64; 2]]) -> BTreeSet<(i64, i64)> {
    let to_wgs84 = utm47n_to_wgs84();
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

/// Create `zpx` (pixel mode) and `zwd` (world mode), fill and flush both.
async fn create_grids(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE ARRAY zpx DIMS (row INT64 [0..{last}], col INT64 [0..{last}]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (8, 8) TILE_ORDER ROW_MAJOR \
             GEOREF (SRID 32647, X col, Y row, TRANSFORM ({EAST0}, {PIXEL_M}, 0, {NORTH0}, 0, \
             -{PIXEL_M}))",
            last = SIZE - 1
        ))
        .await
        .expect("CREATE ARRAY zpx");
    server
        .exec(
            "CREATE ARRAY zwd DIMS (x FLOAT64 [550000..551000], y FLOAT64 [663000..664000]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (240, 240) TILE_ORDER ROW_MAJOR \
             GEOREF (SRID 32647, X x, Y y)",
        )
        .await
        .expect("CREATE ARRAY zwd");
    for row in 0..SIZE {
        let pixel: Vec<String> = (0..SIZE)
            .map(|col| format!("COORDS ({row}, {col}) VALUES ({:?})", cell_value(row, col)))
            .collect();
        server
            .exec(&format!("INSERT INTO ARRAY zpx {}", pixel.join(", ")))
            .await
            .expect("INSERT zpx");
        let world: Vec<String> = (0..SIZE)
            .map(|col| {
                let [x, y] = centre(row, col);
                format!("COORDS ({x:?}, {y:?}) VALUES ({:?})", cell_value(row, col))
            })
            .collect();
        server
            .exec(&format!("INSERT INTO ARRAY zwd {}", world.join(", ")))
            .await
            .expect("INSERT zwd");
    }
    for name in ["zpx", "zwd"] {
        server
            .exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
            .await
            .expect("ARRAY_FLUSH");
    }
}

/// The SQLSTATE `sql` is refused with.
async fn sqlstate(server: &TestServer, sql: &str) -> String {
    let err = server
        .client
        .simple_query(sql)
        .await
        .expect_err("statement must be refused");
    err.as_db_error()
        .map(|db| db.code().code().to_string())
        .unwrap_or_else(|| panic!("no SQLSTATE on {err}"))
}

fn number(row: &std::collections::HashMap<String, String>, col: &str) -> f64 {
    row.get(col)
        .and_then(|t| t.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("column {col} is not a number in {row:?}"))
}

async fn scalar(server: &TestServer, sql: &str) -> f64 {
    let rows = server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: one scalar row: {rows:?}");
    number(&rows[0], "result")
}

/// `(row, col)` of every pixel `zpx` slice row.
async fn pixel_cells(server: &TestServer, sql: &str) -> BTreeSet<(i64, i64)> {
    server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (number(r, "row") as i64, number(r, "col") as i64))
        .collect()
}

#[tokio::test]
async fn agg_over_a_wgs84_zone_matches_on_pixel_and_world_arrays() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    let ring = zone_ring();
    let expected = expected_cells(&ring);
    assert!(
        expected.len() > 50,
        "the zone holds cells: {}",
        expected.len()
    );
    let expected_sum: f64 = expected.iter().map(|&(r, c)| cell_value(r, c)).sum();
    let zone = ewkt(&ring);
    for name in ["zpx", "zwd"] {
        let count = scalar(
            &server,
            &format!("SELECT * FROM ARRAY_AGG('{name}', 'b1', 'count', NULL, NULL, {zone}, NULL)"),
        )
        .await;
        let sum = scalar(
            &server,
            &format!("SELECT * FROM ARRAY_AGG('{name}', 'b1', 'sum', NULL, NULL, {zone}, NULL)"),
        )
        .await;
        assert_eq!(count, expected.len() as f64, "{name}: count");
        assert!(
            (sum - expected_sum).abs() < 1e-6,
            "{name}: sum {sum} vs {expected_sum}"
        );
    }
}

#[tokio::test]
async fn slice_with_a_wgs84_zone_returns_the_cells_inside_it() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    let ring = zone_ring();
    let zone = ewkt(&ring);
    let got = pixel_cells(
        &server,
        &format!(
            "SELECT * FROM ARRAY_SLICE('zpx', '{{row: [0, 31]}}', '*', 0, {zone}, \
             ARRAY['col', 'row'])"
        ),
    )
    .await;
    assert_eq!(got, expected_cells(&ring));

    let world_rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('zwd', '{{x: [550000, 551000]}}', '*', 0, {zone}, NULL)"
        ))
        .await
        .expect("world slice");
    assert_eq!(
        world_rows.len(),
        got.len(),
        "world and pixel select the same cells"
    );
}

#[tokio::test]
async fn slice_all_versions_with_a_zone_keeps_both_versions_of_inside_cells() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    let ring = zone_ring();
    let expected = expected_cells(&ring);
    let inside = *expected.iter().next().expect("an inside cell");
    let outside = (0, 0);
    assert!(
        !expected.contains(&outside),
        "the corner pixel is outside the zone"
    );
    server
        .exec(&format!(
            "INSERT INTO ARRAY zpx COORDS ({}, {}) VALUES (-1.0), COORDS (0, 0) VALUES (-2.0)",
            inside.0, inside.1
        ))
        .await
        .expect("overwrite");
    server
        .exec("SELECT ARRAY_FLUSH('zpx')")
        .await
        .expect("ARRAY_FLUSH");
    let rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('zpx', '{{row: [0, 31]}}', '*', 0, {}, NULL) \
             AS OF SYSTEM TIME NULL",
            ewkt(&ring)
        ))
        .await
        .expect("audit slice");
    let versions_of = |cell: (i64, i64)| {
        rows.iter()
            .filter(|r| (number(r, "row") as i64, number(r, "col") as i64) == cell)
            .count()
    };
    assert_eq!(versions_of(inside), 2, "both versions of the inside cell");
    assert_eq!(versions_of(outside), 0, "no version of the outside cell");
    assert_eq!(
        rows.len(),
        expected.len() + 1,
        "one extra version, nothing outside"
    );
}

#[tokio::test]
async fn a_zone_beside_the_box_narrows_to_no_rows() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    // 100.5°E is about 166 km east of the grid, still inside UTM 47N.
    let far = "'SRID=4326;POLYGON((100.5 6, 100.6 6, 100.6 6.1, 100.5 6.1, 100.5 6))'";
    let rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('zpx', '{{col: [0, 31]}}', '*', 0, {far}, NULL)"
        ))
        .await
        .expect("an empty narrowed range is not an error");
    assert!(rows.is_empty(), "{rows:?}");
    let groups = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_AGG('zpx', 'b1', 'count', 'row', '{{col: [0, 31]}}', {far}, NULL)"
        ))
        .await
        .expect("an empty narrowed range is not an error");
    assert!(groups.is_empty(), "{groups:?}");
}

#[tokio::test]
async fn zone_refusals_carry_their_sqlstate() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    server
        .exec(
            "CREATE ARRAY zplain DIMS (row INT64 [0..3], col INT64 [0..3]) ATTRS (b1 FLOAT64) \
             TILE_EXTENTS (2, 2)",
        )
        .await
        .expect("CREATE ARRAY zplain");
    let zone = ewkt(&zone_ring());
    let no_georef =
        format!("SELECT * FROM ARRAY_AGG('zplain', 'b1', 'count', NULL, NULL, {zone}, NULL)");
    assert_eq!(sqlstate(&server, &no_georef).await, "55000");
    let other_axes = format!(
        "SELECT * FROM ARRAY_AGG('zpx', 'b1', 'count', NULL, NULL, {zone}, ARRAY['row', 'col'])"
    );
    assert_eq!(sqlstate(&server, &other_axes).await, "55000");
    let out_of_domain = "SELECT * FROM ARRAY_AGG('zpx', 'b1', 'count', NULL, NULL, \
         'SRID=4326;POLYGON((150 6, 151 6, 151 7, 150 6))', NULL)";
    assert_eq!(sqlstate(&server, out_of_domain).await, "22000");

    server
        .exec(&format!(
            "CREATE ARRAY ztiny DIMS (row INT64 [0..1000], col INT64 [0..1000]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (10, 10) \
             GEOREF (SRID 32647, X col, Y row, TRANSFORM ({EAST0}, 1e-9, 0, {NORTH0}, 0, -1e-9))"
        ))
        .await
        .expect("CREATE ARRAY ztiny");
    let long_edges = "SELECT * FROM ARRAY_AGG('ztiny', 'b1', 'count', NULL, NULL, \
         'SRID=4326;POLYGON((99 6, 100 6, 101 6, 102 6, 102 6.5, 101 6.5, 100 6.5, 99 6.5, \
         99 6))', NULL)";
    assert_eq!(sqlstate(&server, long_edges).await, "54000");
}

#[tokio::test]
async fn cell_point_transforms_like_a_hand_built_point() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    let rows = server
        .query_named_rows(
            "SELECT row, col, ST_X(ST_Transform(ARRAY_CELL_POINT(), 4326)) AS lon, \
             ST_Y(ST_Transform(ARRAY_CELL_POINT(), 4326)) AS lat \
             FROM ARRAY_SLICE('zpx', '{row: [0, 2], col: [0, 2]}')",
        )
        .await
        .expect("cell point");
    assert_eq!(rows.len(), 9, "{rows:?}");
    for row in &rows {
        let (r, c) = (number(row, "row") as i64, number(row, "col") as i64);
        let [x, y] = centre(r, c);
        let hand = server
            .query_named_rows(&format!(
                "SELECT ST_X(ST_Transform(ST_SetSRID(ST_Point({x:e}, {y:e}), 32647), 4326)) \
                 AS lon, ST_Y(ST_Transform(ST_SetSRID(ST_Point({x:e}, {y:e}), 32647), 4326)) \
                 AS lat"
            ))
            .await
            .expect("hand-built point");
        for axis in ["lon", "lat"] {
            let (cell, built) = (number(row, axis), number(&hand[0], axis));
            assert!(
                (cell - built).abs() <= 1e-12,
                "({r}, {c}) {axis}: {cell} vs {built}"
            );
        }
    }

    let world = server
        .query_named_rows(
            "SELECT x, ST_X(ARRAY_CELL_POINT()) AS px FROM ARRAY_SLICE('zwd', \
             '{x: [550000, 550100], y: [663900, 664000]}')",
        )
        .await
        .expect("world cell point");
    assert!(!world.is_empty());
    for row in &world {
        assert_eq!(
            number(row, "x"),
            number(row, "px"),
            "world mode reads the dims"
        );
    }
}

#[tokio::test]
async fn cell_footprint_refusals_and_shape() {
    let server = TestServer::start().await;
    create_grids(&server).await;
    server
        .exec(
            "CREATE ARRAY zbare DIMS (row INT64 [0..3], col INT64 [0..3]) ATTRS (b1 FLOAT64) \
             TILE_EXTENTS (2, 2)",
        )
        .await
        .expect("CREATE ARRAY zbare");
    let world = "SELECT ARRAY_CELL_FOOTPRINT() FROM ARRAY_SLICE('zwd', '{x: [550000, 551000]}')";
    assert_eq!(sqlstate(&server, world).await, "0A000");
    for call in ["ARRAY_CELL_FOOTPRINT()", "ARRAY_CELL_POINT()"] {
        let bare = format!("SELECT {call} FROM ARRAY_SLICE('zbare', '{{row: [0, 3]}}')");
        assert_eq!(sqlstate(&server, &bare).await, "55000", "{call}");
    }
    let rows = server
        .query_named_rows(
            "SELECT ST_NPoints(ARRAY_CELL_FOOTPRINT()) AS n, ST_Area(ARRAY_CELL_FOOTPRINT()) AS a \
             FROM ARRAY_SLICE('zpx', '{row: [0, 0], col: [0, 1]}')",
        )
        .await
        .expect("footprint");
    assert_eq!(rows.len(), 2, "{rows:?}");
    for row in &rows {
        assert_eq!(number(row, "n"), 5.0, "a closed four-corner ring");
        assert!(
            (number(row, "a") - PIXEL_M * PIXEL_M).abs() < 1e-6,
            "{row:?}"
        );
    }
}
