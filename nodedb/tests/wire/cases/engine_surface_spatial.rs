// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Spatial engine.
//!
//! Covers: ST_GeoHash encode/decode roundtrip, H3 encode/decode roundtrip,
//! ST_Distance, and basic collection lifecycle.

use crate::harness::TestServer;

#[tokio::test]
async fn create_spatial_collection_and_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION spatial_basic \
         COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    srv.exec(
        "INSERT INTO spatial_basic (id, location, name) \
         VALUES ('p1', ST_Point(-122.4, 37.8), 'SF')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT id, name FROM spatial_basic WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "SF");
}

// ── Geometry constructors on the write path ─────────────────────────────────
// ST_Point and ST_GeomFromGeoJSON were the only wired INSERT constructors;
// ST_MakePoint / ST_GeomFromText (WKT) / ST_GeomFromWKB (WKB) fell through to
// "unsupported value expression". These must produce storable geometry that
// round-trips back through a SELECT of the GEOMETRY column.

#[tokio::test]
async fn insert_st_makepoint_constructor_roundtrips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_makepoint \
         COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    srv.exec(
        "INSERT INTO sp_makepoint (id, location, name) \
         VALUES ('p1', ST_MakePoint(-122.4, 37.8), 'via MakePoint')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT name, location FROM sp_makepoint WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row must be retrievable, got {rows:?}");
    assert_eq!(rows[0][0], "via MakePoint");
    let geom = &rows[0][1];
    assert!(
        geom.contains("-122.4") && geom.contains("37.8"),
        "ST_MakePoint geometry must round-trip its coordinates, got: {geom}"
    );
}

#[tokio::test]
async fn insert_st_geomfromtext_point_roundtrips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_wkt_pt \
         COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    srv.exec(
        "INSERT INTO sp_wkt_pt (id, location, name) \
         VALUES ('p1', ST_GeomFromText('POINT(-122.4 37.8)'), 'via WKT')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT location FROM sp_wkt_pt WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row must be retrievable, got {rows:?}");
    let geom = &rows[0][0];
    assert!(
        geom.contains("-122.4") && geom.contains("37.8"),
        "ST_GeomFromText(POINT) must round-trip its coordinates, got: {geom}"
    );
}

#[tokio::test]
async fn insert_st_geomfromtext_linestring_roundtrips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_wkt_ls \
         COLUMNS (id TEXT, location GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    srv.exec(
        "INSERT INTO sp_wkt_ls (id, location) \
         VALUES ('l1', ST_GeomFromText('LINESTRING(-122.4 37.8, -121.0 36.5)'))",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT location FROM sp_wkt_ls WHERE id = 'l1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row must be retrievable, got {rows:?}");
    let geom = &rows[0][0].to_lowercase();
    assert!(
        geom.contains("linestring") || geom.contains("coordinates"),
        "ST_GeomFromText(LINESTRING) must round-trip as a geometry, got: {geom}"
    );
    assert!(
        rows[0][0].contains("-121"),
        "LINESTRING second vertex must survive, got: {}",
        rows[0][0]
    );
}

#[tokio::test]
async fn insert_st_geomfromwkb_constructor_roundtrips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_wkb \
         COLUMNS (id TEXT, location GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    // WKB for POINT(2 1): little-endian (01), type 1 point, x=2.0, y=1.0.
    srv.exec(
        "INSERT INTO sp_wkb (id, location) \
         VALUES ('w1', ST_GeomFromWKB(X'01010000000000000000000040000000000000F03F'))",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT location FROM sp_wkb WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row must be retrievable, got {rows:?}");
    assert!(
        !rows[0][0].is_empty(),
        "ST_GeomFromWKB geometry must be stored, got empty"
    );
}

#[tokio::test]
async fn st_geohash_encode_decode_roundtrip() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION spatial_geo WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT ST_GeoHash(-122.4, 37.8, 6)")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let hash = rows[0][0].clone();
    assert!(!hash.is_empty(), "expected geohash string, got empty");
    assert!(hash.starts_with('9'), "unexpected geohash prefix: {hash}");

    let rows2 = srv
        .query_rows(&format!("SELECT ST_GeoHashDecode('{hash}')"))
        .await
        .unwrap();
    assert_eq!(rows2.len(), 1);
    assert!(!rows2[0][0].is_empty(), "expected decoded bbox, got empty");
}

#[tokio::test]
async fn h3_latlngtocell_and_celltolatlng_roundtrip() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION spatial_h3 WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT H3_LatLngToCell(37.8, -122.4, 7)")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let cell = rows[0][0].clone();
    assert!(!cell.is_empty(), "expected H3 cell string, got empty");

    let rows2 = srv
        .query_rows(&format!("SELECT H3_CellToLatLng('{cell}')"))
        .await
        .unwrap();
    assert_eq!(rows2.len(), 1);
    assert!(
        !rows2[0][0].is_empty(),
        "expected decoded lat/lng, got empty"
    );
}

#[tokio::test]
async fn scalar_st_distance() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION spatial_dist WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT ST_Distance(\
               ST_Point(-122.4, 37.8), \
               ST_Point(-87.6, 41.8)\
             )",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let dist: f64 = rows[0][0].parse().expect("expected numeric distance");
    assert!(dist > 0.0, "distance should be positive, got {dist}");
}

// ── R-tree indexing parity: DOCUMENT collections vs the `spatial` engine ────
// SQL-inserted geometry (`ST_Point(...)`) is stored as a native geometry
// value in a DOCUMENT collection and as EWKB in a `spatial` collection. Both
// are R-tree-indexed, and both return identical results for a spatial
// predicate.
#[tokio::test]
async fn sql_geometry_insert_into_document_collection_matches_spatial_predicate() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION doc_geo WITH (engine='document_schemaless')")
        .await
        .unwrap();

    // Times Square, NYC — should match a nearby ST_DWithin search.
    srv.exec(
        "INSERT INTO doc_geo (id, loc, name) \
         VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')",
    )
    .await
    .unwrap();
    // Paris — far away, should not match.
    srv.exec(
        "INSERT INTO doc_geo (id, loc, name) \
         VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows(
            "SELECT name FROM doc_geo WHERE \
             ST_DWithin(loc, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row within 5 km of Times Square on a DOCUMENT \
         collection, got {rows:?}"
    );
    assert_eq!(rows[0][0], "Times Square");
}

// ── Predicate argument order ────────────────────────────────────────────────
// `ST_Contains(loc, q)` asks whether the STORED geometry contains the query
// geometry — the geofencing shape, where `loc` is a zone polygon and `q` a
// point. `ST_Within(loc, q)` is its converse. Evaluating either with the
// operands reversed silently returns the wrong rows rather than erroring, so
// both directions are pinned with a literal query geometry.

#[tokio::test]
async fn st_contains_matches_the_zone_holding_the_query_point() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_zones \
         COLUMNS (id TEXT, boundary GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO sp_zones (id, boundary) \
         VALUES ('inner', ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO sp_zones (id, boundary) \
         VALUES ('elsewhere', ST_GeomFromText('POLYGON((90 40, 95 40, 95 45, 90 45, 90 40))'))",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_zones WHERE \
             ST_Contains(boundary, '{\"type\":\"Point\",\"coordinates\":[1,2]}')",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "only the zone containing the point may match, got {rows:?}"
    );
    assert_eq!(rows[0][0], "inner");
}

#[tokio::test]
async fn st_within_matches_the_point_inside_the_query_polygon() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_points_in_zone \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO sp_points_in_zone (id, loc) VALUES ('inside', ST_Point(1, 2))")
        .await
        .unwrap();
    srv.exec("INSERT INTO sp_points_in_zone (id, loc) VALUES ('outside', ST_Point(90, 40))")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_points_in_zone WHERE \
             ST_Within(loc, '{\"type\":\"Polygon\",\"coordinates\":\
             [[[0,0],[5,0],[5,5],[0,5],[0,0]]]}')",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "only the point inside the polygon may match, got {rows:?}"
    );
    assert_eq!(rows[0][0], "inside");
}

#[tokio::test]
async fn count_spatial_rows() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION spatial_cnt \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();

    for i in 0..3u32 {
        let lng = -122.4 + i as f64 * 0.1;
        srv.exec(&format!(
            "INSERT INTO spatial_cnt (id, loc) VALUES ('p{i}', ST_Point({lng}, 37.8))"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM spatial_cnt")
        .await
        .unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 3);
}
