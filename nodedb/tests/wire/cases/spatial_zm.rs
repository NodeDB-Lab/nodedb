// SPDX-License-Identifier: BUSL-1.1

//! Z and M ordinates, EMPTY geometries, and the text and binary encodings.
//!
//! Every storage form keeps Z and M: the document engine stores a native
//! geometry, and the strict, columnar and spatial engines store EWKB. A
//! geometry read back renders the same WKT it was written with.

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

#[tokio::test]
async fn wkt_z_and_m_round_trip() {
    let srv = TestServer::start().await;
    for wkt in [
        "POINT Z (1 2 3)",
        "POINT M (1 2 4)",
        "POINT ZM (1 2 3 4)",
        "LINESTRING Z (0 0 1,1 1 2)",
        "POLYGON M ((0 0 1,1 0 2,1 1 3,0 0 1))",
    ] {
        let back = scalar(&srv, &format!("SELECT ST_AsText(ST_GeomFromText('{wkt}'))")).await;
        assert_eq!(back, wkt);
    }
}

/// Untagged WKT with three numbers per coordinate is XYZ, four is XYZM.
#[tokio::test]
async fn untagged_wkt_dimension_follows_the_coordinate_count() {
    let srv = TestServer::start().await;
    let z = scalar(&srv, "SELECT ST_AsText(ST_GeomFromText('POINT(1 2 3)'))").await;
    assert_eq!(z, "POINT Z (1 2 3)");
    let zm = scalar(&srv, "SELECT ST_AsText(ST_GeomFromText('POINT(1 2 3 4)'))").await;
    assert_eq!(zm, "POINT ZM (1 2 3 4)");
}

#[tokio::test]
async fn dimension_accessors() {
    let srv = TestServer::start().await;
    let g = "ST_GeomFromText('POINT ZM (1 2 3 4)')";
    assert_eq!(number(&srv, &format!("SELECT ST_Z({g})")).await, 3.0);
    assert_eq!(number(&srv, &format!("SELECT ST_M({g})")).await, 4.0);
    assert_eq!(scalar(&srv, &format!("SELECT ST_ZMFlag({g})")).await, "3");
    assert_eq!(scalar(&srv, &format!("SELECT ST_CoordDim({g})")).await, "4");
    assert!(is_true(
        &scalar(&srv, &format!("SELECT ST_HasZ({g})")).await
    ));
    assert!(is_true(
        &scalar(&srv, &format!("SELECT ST_HasM({g})")).await
    ));
    let xy = scalar(&srv, "SELECT ST_Z(ST_GeomFromText('POINT(1 2)'))").await;
    assert_eq!(xy, "", "an XY point has no Z: NULL");
}

#[tokio::test]
async fn st_makepoint_builds_every_dimension() {
    let srv = TestServer::start().await;
    let cases = [
        ("ST_MakePoint(1, 2)", "POINT(1 2)"),
        ("ST_MakePoint(1, 2, 3)", "POINT Z (1 2 3)"),
        ("ST_MakePoint(1, 2, 3, 4)", "POINT ZM (1 2 3 4)"),
        ("ST_MakePointM(1, 2, 4)", "POINT M (1 2 4)"),
    ];
    for (call, wkt) in cases {
        assert_eq!(
            scalar(&srv, &format!("SELECT ST_AsText({call})")).await,
            wkt
        );
    }
}

#[tokio::test]
async fn force_functions_add_and_drop_ordinates() {
    let srv = TestServer::start().await;
    let cases = [
        (
            "ST_Force2D(ST_GeomFromText('POINT ZM (1 2 3 4)'))",
            "POINT(1 2)",
        ),
        (
            "ST_Force3D(ST_GeomFromText('POINT(1 2)'))",
            "POINT Z (1 2 0)",
        ),
        (
            "ST_Force3D(ST_GeomFromText('POINT(1 2)'), 7)",
            "POINT Z (1 2 7)",
        ),
        (
            "ST_Force3DM(ST_GeomFromText('POINT Z (1 2 3)'))",
            "POINT M (1 2 0)",
        ),
        (
            "ST_Force4D(ST_GeomFromText('POINT Z (1 2 3)'))",
            "POINT ZM (1 2 3 0)",
        ),
    ];
    for (call, wkt) in cases {
        assert_eq!(
            scalar(&srv, &format!("SELECT ST_AsText({call})")).await,
            wkt
        );
    }
}

/// EWKT carries the SRID, and EWKB round-trips SRID, Z and M.
#[tokio::test]
async fn ewkt_and_ewkb_round_trip() {
    let srv = TestServer::start().await;
    let ewkt = scalar(
        &srv,
        "SELECT ST_AsEWKT(ST_GeomFromText('POINT Z (500000 10 3)', 32648))",
    )
    .await;
    assert_eq!(ewkt, "SRID=32648;POINT Z (500000 10 3)");

    let back = "ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromText('SRID=32648;POINT ZM (1 2 3 4)')))";
    assert_eq!(
        scalar(&srv, &format!("SELECT ST_AsText({back})")).await,
        "POINT ZM (1 2 3 4)"
    );
    assert_eq!(
        scalar(&srv, &format!("SELECT ST_SRID({back})")).await,
        "32648"
    );

    // ISO WKB keeps Z and M but has no SRID slot.
    let iso = "ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT Z (1 2 3)')))";
    assert_eq!(
        scalar(&srv, &format!("SELECT ST_AsText({iso})")).await,
        "POINT Z (1 2 3)"
    );
}

/// GeoJSON writes Z as the third position member. It has no M slot.
#[tokio::test]
async fn geojson_keeps_z() {
    let srv = TestServer::start().await;
    let json = scalar(
        &srv,
        "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT Z (1 2 3)'))",
    )
    .await;
    assert!(
        json.contains("[1.0,2.0,3.0]") || json.contains("[1,2,3]"),
        "{json}"
    );
    let z = number(
        &srv,
        "SELECT ST_Z(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1,2,3]}'))",
    )
    .await;
    assert_eq!(z, 3.0);
}

#[tokio::test]
async fn malformed_text_fails_the_statement() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT ST_GeomFromText('POINT Z (1 2)')", "WKT")
        .await;
    srv.expect_error("SELECT ST_GeomFromText('POINT(1 2')", "WKT")
        .await;
    srv.expect_error("SELECT ST_AsText('not a geometry')", "st_astext")
        .await;
}

#[tokio::test]
async fn empty_geometries() {
    let srv = TestServer::start().await;
    for wkt in [
        "POINT EMPTY",
        "LINESTRING EMPTY",
        "POLYGON EMPTY",
        "MULTIPOINT EMPTY",
        "MULTILINESTRING EMPTY",
        "MULTIPOLYGON EMPTY",
        "GEOMETRYCOLLECTION EMPTY",
        "POINT Z EMPTY",
    ] {
        let g = format!("ST_GeomFromText('{wkt}')");
        assert_eq!(scalar(&srv, &format!("SELECT ST_AsText({g})")).await, wkt);
        assert!(
            is_true(&scalar(&srv, &format!("SELECT ST_IsEmpty({g})")).await),
            "{wkt}"
        );
        assert_eq!(scalar(&srv, &format!("SELECT ST_NPoints({g})")).await, "0");
    }
    let x = scalar(&srv, "SELECT ST_X(ST_GeomFromText('POINT EMPTY'))").await;
    assert_eq!(x, "", "an EMPTY point has no X: NULL");
}

/// An EMPTY row is stored, never indexed, and matches no predicate.
#[tokio::test]
async fn stored_empty_matches_no_predicate() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION zm_empty COLUMNS (id TEXT, loc GEOMETRY) WITH (engine='spatial')")
        .await
        .unwrap();
    srv.exec("INSERT INTO zm_empty (id, loc) VALUES ('empty', ST_GeomFromText('POINT EMPTY'))")
        .await
        .unwrap();
    srv.exec("INSERT INTO zm_empty (id, loc) VALUES ('near', ST_GeomFromText('POINT(1 2)'))")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM zm_empty WHERE ST_DWithin(loc, ST_GeomFromText('POINT(1 2)'), 10000000)",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["near"]);

    let empty = scalar(
        &srv,
        "SELECT ST_AsText(loc) FROM zm_empty WHERE id = 'empty'",
    )
    .await;
    assert_eq!(empty, "POINT EMPTY");
}

/// Each engine stores the geometry with its Z and M intact.
#[tokio::test]
async fn every_engine_stores_z_and_m() {
    let srv = TestServer::start().await;
    let ddl = [
        (
            "zm_doc",
            "CREATE COLLECTION zm_doc WITH (engine='document_schemaless')",
        ),
        (
            "zm_strict",
            "CREATE COLLECTION zm_strict (id TEXT PRIMARY KEY, loc GEOMETRY) \
             WITH (engine='document_strict')",
        ),
        (
            "zm_columnar",
            "CREATE COLLECTION zm_columnar COLUMNS (id TEXT, loc GEOMETRY) \
             WITH (engine='columnar')",
        ),
        (
            "zm_spatial",
            "CREATE COLLECTION zm_spatial COLUMNS (id TEXT, loc GEOMETRY) \
             WITH (engine='spatial')",
        ),
    ];
    for (name, create) in ddl {
        srv.exec(create).await.unwrap();
        for (id, wkt) in [
            ("p", "POINT ZM (1 2 3 4)"),
            ("l", "LINESTRING M (0 0 1,1 1 2)"),
        ] {
            srv.exec(&format!(
                "INSERT INTO {name} (id, loc) VALUES ('{id}', ST_GeomFromText('{wkt}'))"
            ))
            .await
            .unwrap_or_else(|e| panic!("{name}: insert {wkt}: {e}"));
            let back = scalar(
                &srv,
                &format!("SELECT ST_AsText(loc) FROM {name} WHERE id = '{id}'"),
            )
            .await;
            assert_eq!(back, wkt, "{name} must keep Z and M");
        }
    }
}

/// `ST_3DDWithin` routes to the R-tree and counts the Z gap: a point 30 m
/// away in plan and 40 m up lies 50 m away.
#[tokio::test]
async fn st_3ddwithin_filters_through_the_index() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION zm_3d COLUMNS (id TEXT, loc GEOMETRY) WITH (engine='spatial')")
        .await
        .unwrap();
    for (id, wkt) in [
        ("level", "POINT Z (500030 0 40)"),
        ("high", "POINT Z (500030 0 400)"),
        ("far", "POINT Z (510000 0 0)"),
    ] {
        srv.exec(&format!(
            "INSERT INTO zm_3d (id, loc) VALUES ('{id}', ST_GeomFromText('{wkt}', 32648))"
        ))
        .await
        .unwrap();
    }
    let ids = |rows: Vec<Vec<String>>| {
        let mut ids: Vec<String> = rows.into_iter().map(|r| r[0].clone()).collect();
        ids.sort();
        ids
    };
    let q = "ST_GeomFromText('POINT Z (500000 0 0)', 32648)";

    let three_d = srv
        .query_rows(&format!(
            "SELECT id FROM zm_3d WHERE ST_3DDWithin(loc, {q}, 50.5)"
        ))
        .await
        .unwrap();
    assert_eq!(ids(three_d), ["level"]);

    let plan = srv
        .query_rows(&format!(
            "SELECT id FROM zm_3d WHERE ST_DWithin(loc, {q}, 50.5)"
        ))
        .await
        .unwrap();
    assert_eq!(ids(plan), ["high", "level"]);

    let d = number(
        &srv,
        &format!("SELECT ST_3DDistance(loc, {q}) FROM zm_3d WHERE id = 'level'"),
    )
    .await;
    assert!((d - 50.0).abs() < 1e-9, "{d}");
}
