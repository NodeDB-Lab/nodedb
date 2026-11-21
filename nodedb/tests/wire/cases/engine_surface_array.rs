// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Array (ND sparse) engine.
//!
//! Covers: CREATE ARRAY DDL, INSERT INTO ARRAY, ARRAY_SLICE TVF query,
//! wrong-DDL rejection (engine='array' must be rejected), and WAL durability.

use crate::harness::TestServer;

#[tokio::test]
async fn create_array_and_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_basic \
         DIMS (row INT64, col INT64) \
         ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO ARRAY arr_basic COORDS (0, 0) VALUES (1.5)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ARRAY arr_basic COORDS (0, 1) VALUES (2.5)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ARRAY arr_basic COORDS (1, 0) VALUES (3.5)")
        .await
        .unwrap();
}

#[tokio::test]
async fn array_slice_query() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_slice \
         DIMS (x INT64, y INT64) \
         ATTRS (temp FLOAT64) \
         TILE_EXTENTS (5, 5)",
    )
    .await
    .unwrap();

    for x in 0..3i64 {
        for y in 0..3i64 {
            let temp = (x * 3 + y) as f64;
            srv.exec(&format!(
                "INSERT INTO ARRAY arr_slice COORDS ({x}, {y}) VALUES ({temp})"
            ))
            .await
            .unwrap();
        }
    }

    // ARRAY_SLICE bounds are inclusive on both ends (closed range).
    // See `nodedb-array/src/query/slice.rs` for the documented semantics.
    let rows = srv
        .query_rows(
            "SELECT * FROM ARRAY_SLICE('arr_slice', \
             '{\"x\":[0,1],\"y\":[0,1]}', '*', 100)",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        4,
        "expected 4 cells from inclusive [0,1] x [0,1] slice, got {}",
        rows.len()
    );
}

#[tokio::test]
async fn engine_array_flag_rejected_in_with_clause() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_array WITH (engine='array')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("array")
            || err.to_lowercase().contains("create array")
            || err.to_lowercase().contains("unsupported"),
        "expected array-rejection error, got: {err}"
    );
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_wal \
         DIMS (i INT64) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (10)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO ARRAY arr_wal COORDS (5) VALUES (99.0)")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT * FROM ARRAY_SLICE('arr_wal', '{\"i\":[5,6]}', '*', 10)")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "expected persisted array cell after restart"
    );
}

/// A 4x4 two-band image: cell `(x, y)` has `nir = (4x + y + 1) / 20` and
/// `red = (16 - (4x + y)) / 40`.
async fn create_img(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE ARRAY {name} \
         DIMS (x INT64 [0..3], y INT64 [0..3]) \
         ATTRS (nir FLOAT64, red FLOAT64) \
         TILE_EXTENTS (2, 2)"
    ))
    .await
    .unwrap();
    let cells: Vec<String> = (0..4i64)
        .flat_map(|x| {
            (0..4i64).map(move |y| {
                let k = 4 * x + y;
                format!(
                    "COORDS ({x}, {y}) VALUES ({}, {})",
                    (k + 1) as f64 / 20.0,
                    (16 - k) as f64 / 40.0
                )
            })
        })
        .collect();
    srv.exec(&format!("INSERT INTO ARRAY {name} {}", cells.join(", ")))
        .await
        .unwrap();
}

/// NDVI over the wire: a computed column, an attr filter, and an ordering
/// over it all apply to the slice's rows.
#[tokio::test]
async fn array_slice_ndvi_where_order_wire() {
    let srv = TestServer::start().await;
    create_img(&srv, "img_ndvi").await;

    let rows = srv
        .query_rows(
            "SELECT x, y, (nir - red) / (nir + red) AS ndvi \
             FROM ARRAY_SLICE('img_ndvi', '{x: [0, 3], y: [0, 3]}') AS t \
             WHERE t.nir > t.red \
             ORDER BY ndvi DESC \
             LIMIT 3",
        )
        .await
        .unwrap();
    let got: Vec<(i64, i64, f64)> = rows
        .iter()
        .map(|r| {
            (
                r[0].parse().unwrap(),
                r[1].parse().unwrap(),
                r[2].parse().unwrap(),
            )
        })
        .collect();
    // NDVI rises with k, so the three greenest cells are k = 15, 14, 13.
    let expected = [(3, 3), (3, 2), (3, 1)];
    assert_eq!(got.len(), 3, "LIMIT 3 keeps three rows: {got:?}");
    for ((x, y, ndvi), (ex, ey)) in got.iter().zip(expected) {
        assert_eq!(
            (*x, *y),
            (ex, ey),
            "rows ordered by ndvi descending: {got:?}"
        );
        let k = 4 * ex + ey;
        let (nir, red) = ((k + 1) as f64 / 20.0, (16 - k) as f64 / 40.0);
        assert!(((nir - red) / (nir + red) - ndvi).abs() < 1e-9);
    }
}

/// The extended protocol describes the slice's columns with the types its
/// rows carry: `int8` dims and `float8` attrs. Each cell decodes as that
/// type in the binary result format.
#[tokio::test]
async fn array_slice_describe_types_match_rows() {
    const INT8: u32 = 20;
    const FLOAT8: u32 = 701;
    let srv = TestServer::start().await;
    create_img(&srv, "img_describe").await;

    let stmt = srv
        .client
        .prepare("SELECT * FROM ARRAY_SLICE('img_describe', '{x: [1, 1], y: [0, 3]}')")
        .await
        .expect("prepare slice");
    let described: Vec<(String, u32)> = stmt
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect();
    assert_eq!(
        described,
        [
            ("x".to_string(), INT8),
            ("y".to_string(), INT8),
            ("nir".to_string(), FLOAT8),
            ("red".to_string(), FLOAT8),
        ]
    );

    let rows = srv.client.query(&stmt, &[]).await.expect("execute slice");
    assert_eq!(rows.len(), 4, "x = 1 holds four cells");
    let mut ys = Vec::new();
    for row in &rows {
        let x: i64 = row.get("x");
        let y: i64 = row.get("y");
        let nir: f64 = row.get("nir");
        let red: f64 = row.get("red");
        assert_eq!(x, 1);
        let k = 4 * x + y;
        assert!((nir - (k + 1) as f64 / 20.0).abs() < 1e-9);
        assert!((red - (16 - k) as f64 / 40.0).abs() < 1e-9);
        ys.push(y);
    }
    ys.sort_unstable();
    assert_eq!(ys, [0, 1, 2, 3]);
}

#[tokio::test]
async fn array_ddl_prepares_and_runs_on_the_extended_query() {
    let srv = TestServer::start().await;
    for sql in [
        "CREATE ARRAY ext_arr DIMS (x INT64 [0..100]) ATTRS (v FLOAT64) TILE_EXTENTS (16)",
        "ALTER ARRAY ext_arr ALTER ATTR v SET (NODATA 0)",
        "DROP ARRAY ext_arr",
    ] {
        let stmt = srv
            .client
            .prepare(sql)
            .await
            .unwrap_or_else(|e| panic!("Parse must keep `{sql}`: {e}"));
        srv.client
            .execute(&stmt, &[])
            .await
            .unwrap_or_else(|e| panic!("Execute must run `{sql}`: {e}"));
    }
}
