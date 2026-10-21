// SPDX-License-Identifier: BUSL-1.1

//! Array `GEOREF`: CREATE, DESCRIBE, `ALTER ARRAY .. SET/DROP GEOREF`, and
//! the ELEMENTWISE equal-georef rule.
//!
//! A georef change keeps `schema_hash`. Two proofs run over the wire:
//! flushed segments stay readable after ALTER and after restart, and
//! ELEMENTWISE passes the Data Plane hash check after both arrays change.

use std::collections::HashMap;

use crate::harness::TestServer;

const PIXEL_GT: &str =
    "TRANSFORM (100.5, 8.983152841195214e-05, 0, 6.5, 0, -8.983152841195214E-05)";

fn create_raster(name: &str, georef: &str) -> String {
    format!(
        "CREATE ARRAY {name} \
         DIMS (row INT64 [0..15], col INT64 [0..15]) \
         ATTRS (b1 FLOAT64) \
         TILE_EXTENTS (4, 4) \
         TILE_ORDER ROW_MAJOR {georef}"
    )
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

/// DESCRIBE rows of `name`, keyed by field, valued by type.
async fn describe(server: &TestServer, name: &str) -> HashMap<String, String> {
    server
        .query_named_rows(&format!("DESCRIBE {name}"))
        .await
        .unwrap_or_else(|e| panic!("DESCRIBE {name}: {e}"))
        .into_iter()
        .map(|row| {
            let field = row.get("field").cloned().unwrap_or_default();
            let ty = row.get("type").cloned().unwrap_or_default();
            (field, ty)
        })
        .collect()
}

/// Every cell of `name` as `(row, col) -> b1`.
async fn cells(server: &TestServer, name: &str) -> HashMap<(i64, i64), f64> {
    server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{name}', '{{\"row\":[0,15],\"col\":[0,15]}}', '*', 1000)"
        ))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_SLICE {name}: {e}"))
        .iter()
        .map(|row| {
            let num = |c: &str| row.get(c).and_then(|t| t.parse::<f64>().ok());
            let key = (
                num("row").expect("row") as i64,
                num("col").expect("col") as i64,
            );
            (key, num("b1").expect("b1"))
        })
        .collect()
}

#[tokio::test]
async fn create_with_georef_then_describe() {
    let server = TestServer::start().await;
    server
        .exec(&create_raster(
            "geo_pixel",
            &format!("GEOREF (SRID 4326, X col, Y row, {PIXEL_GT})"),
        ))
        .await
        .expect("CREATE with pixel GEOREF");
    let rows = describe(&server, "geo_pixel").await;
    assert_eq!(
        rows.get("__georef").map(String::as_str),
        Some(
            "SRID 4326, X col, Y row, TRANSFORM (100.5, 8.983152841195213e-5, 0.0, 6.5, 0.0, \
             -8.983152841195213e-5)"
        ),
        "{rows:?}"
    );
    assert_eq!(rows.get("__storage").map(String::as_str), Some("array"));
    assert_eq!(rows.get("__tile_extents").map(String::as_str), Some("4, 4"));
    assert_eq!(
        rows.get("row").map(String::as_str),
        Some("INT64 DIM [0..15]")
    );

    server
        .exec(
            "CREATE ARRAY geo_world DIMS (lon FLOAT64 [99.0..120.0], lat FLOAT64 [0.0..8.0]) \
             ATTRS (v FLOAT64) TILE_EXTENTS (1, 1) GEOREF (SRID 4326, X lon, Y lat)",
        )
        .await
        .expect("CREATE with world GEOREF");
    let rows = describe(&server, "geo_world").await;
    assert_eq!(
        rows.get("__georef").map(String::as_str),
        Some("SRID 4326, X lon, Y lat")
    );

    server
        .exec(&create_raster("geo_none", ""))
        .await
        .expect("CREATE without GEOREF");
    assert!(!describe(&server, "geo_none").await.contains_key("__georef"));
}

#[tokio::test]
async fn create_georef_refusals_carry_their_sqlstate() {
    let server = TestServer::start().await;
    let cases = [
        ("GEOREF (SRID 999, X col, Y row)", "22000"),
        ("GEOREF (SRID 4326, X col, Y lat)", "42703"),
        ("GEOREF (SRID 4326, X col, Y col)", "22000"),
        (
            "GEOREF (SRID 4326, X col, Y row, TRANSFORM (0, 1, 1, 0, 1, 1))",
            "22000",
        ),
        (
            "GEOREF (SRID 4326, X col, Y row, TRANSFORM (0, 1e999, 0, 0, 0, -1))",
            "22000",
        ),
        (
            "GEOREF (SRID 4326, X col, Y row, TRANSFORM (0, 1, 0, 0, 0))",
            "22000",
        ),
    ];
    for (i, (clause, code)) in cases.into_iter().enumerate() {
        let sql = create_raster(&format!("geo_bad_{i}"), clause);
        assert_eq!(sqlstate(&server, &sql).await, code, "{clause}");
    }
    let floats = "CREATE ARRAY geo_bad_float DIMS (x FLOAT64 [0.0..1.0], y FLOAT64 [0.0..1.0]) \
                  ATTRS (v FLOAT64) TILE_EXTENTS (1, 1) \
                  GEOREF (SRID 32647, X x, Y y, TRANSFORM (0, 10, 0, 0, 0, -10))";
    assert_eq!(sqlstate(&server, floats).await, "22000");
}

#[tokio::test]
async fn alter_set_and_drop_georef_keep_data_and_survive_restart() {
    let server = TestServer::start().await;
    server
        .exec(&create_raster("geo_alter", ""))
        .await
        .expect("CREATE");
    let mut want = HashMap::new();
    for (row, col) in [(0, 0), (3, 7), (15, 15)] {
        let v = (row * 16 + col) as f64 + 0.5;
        server
            .exec(&format!(
                "INSERT INTO ARRAY geo_alter COORDS ({row}, {col}) VALUES ({v})"
            ))
            .await
            .expect("INSERT before flush");
        want.insert((row, col), v);
    }
    server
        .exec("SELECT ARRAY_FLUSH('geo_alter')")
        .await
        .expect("flush");
    server
        .exec("INSERT INTO ARRAY geo_alter COORDS (8, 8) VALUES (136.5)")
        .await
        .expect("INSERT into the memtable");
    want.insert((8, 8), 136.5);
    let before = describe(&server, "geo_alter").await;

    server
        .exec(
            "ALTER ARRAY geo_alter SET GEOREF (SRID 32647, X col, Y row, \
             TRANSFORM (600000, 10, 0, 700000, 0, -10))",
        )
        .await
        .expect("SET GEOREF");
    let georef = "SRID 32647, X col, Y row, TRANSFORM (600000.0, 10.0, 0.0, 700000.0, 0.0, -10.0)";
    let after = describe(&server, "geo_alter").await;
    assert_eq!(after.get("__georef").map(String::as_str), Some(georef));
    let mut without = after.clone();
    without.remove("__georef");
    assert_eq!(without, before, "SET GEOREF keeps every other field");
    assert_eq!(cells(&server, "geo_alter").await, want);

    assert_eq!(
        sqlstate(
            &server,
            "ALTER ARRAY geo_alter SET GEOREF (SRID 4326, X col, Y lat)"
        )
        .await,
        "42703"
    );
    assert_eq!(
        sqlstate(
            &server,
            "ALTER ARRAY geo_alter SET GEOREF (SRID 9999, X col, Y row)"
        )
        .await,
        "22000"
    );

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, dir) = TestServer::open_on_path(dir).await;
    let restarted = describe(&server, "geo_alter").await;
    assert_eq!(restarted, after, "the georef survives restart");
    assert_eq!(cells(&server, "geo_alter").await, want);

    server
        .exec("ALTER ARRAY geo_alter DROP GEOREF")
        .await
        .expect("DROP GEOREF");
    assert_eq!(describe(&server, "geo_alter").await, before);
    assert_eq!(
        sqlstate(&server, "ALTER ARRAY geo_alter DROP GEOREF").await,
        "55000"
    );

    // A server opened on a path holds a placeholder; `dir` is the real one.
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    assert_eq!(describe(&server, "geo_alter").await, before);
    assert_eq!(cells(&server, "geo_alter").await, want);
}

#[tokio::test]
async fn elementwise_requires_equal_georefs() {
    let server = TestServer::start().await;
    for name in ["geo_left", "geo_right"] {
        server.exec(&create_raster(name, "")).await.expect("CREATE");
        server
            .exec(&format!(
                "INSERT INTO ARRAY {name} COORDS (1, 2) VALUES (4.0)"
            ))
            .await
            .expect("INSERT");
        server
            .exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
            .await
            .expect("flush");
    }
    let sql = "SELECT * FROM ARRAY_ELEMENTWISE('geo_left', 'geo_right', 'add', 'b1')";
    let rows = server
        .query_rows(sql)
        .await
        .expect("no georef on either side");
    assert_eq!(rows.len(), 1);

    let set = |name: &str, x0: &str| {
        format!(
            "ALTER ARRAY {name} SET GEOREF (SRID 32647, X col, Y row, \
             TRANSFORM ({x0}, 10, 0, 700000, 0, -10))"
        )
    };
    server
        .exec(&set("geo_left", "600000"))
        .await
        .expect("SET left");
    assert_eq!(sqlstate(&server, sql).await, "22000");
    server
        .exec(&set("geo_right", "600010"))
        .await
        .expect("SET right");
    assert_eq!(sqlstate(&server, sql).await, "22000");
    server
        .exec(&set("geo_right", "600000"))
        .await
        .expect("SET right equal");
    // The Data Plane refuses unequal schema hashes, so success proves both
    // ALTERs kept the hash.
    let rows = server.query_rows(sql).await.expect("equal georefs");
    assert_eq!(rows.len(), 1);
}
