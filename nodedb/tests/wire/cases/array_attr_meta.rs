// SPDX-License-Identifier: BUSL-1.1

//! Array attr metadata (`NODATA`, `SCALE`, `OFFSET`, `UNITS`) and the
//! 1-byte `INT8` attr type over pgwire.
//!
//! - `CREATE ARRAY` takes the metadata after the attr type. DESCRIBE shows
//!   it in DDL form.
//! - `ALTER ARRAY .. ALTER ATTR .. SET (...)` / `DROP ...` change it and keep
//!   `schema_hash`: flushed segments stay readable.
//! - `physical => true` reads `raw * scale + offset`, and NULL for nodata.
//! - Nodata is not data: `ARRAY_AGG` skips it, `COUNT` included.

use std::collections::HashMap;

use crate::harness::TestServer;

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

/// `x -> red` of every cell `sql` returns. NULL reads as `None`.
async fn reds(server: &TestServer, sql: &str) -> HashMap<i64, Option<f64>> {
    server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| {
            let x: i64 = row["x"].parse().expect("x");
            let red = row.get("red").filter(|t| !t.is_empty()).map(|t| {
                t.parse::<f64>()
                    .unwrap_or_else(|_| panic!("red '{t}' is not a number"))
            });
            (x, red)
        })
        .collect()
}

/// The `result` of `ARRAY_AGG('am', attr, <rest>)`. NULL reads as `None`.
async fn agg(server: &TestServer, rest: &str) -> Option<f64> {
    let sql = format!("SELECT * FROM ARRAY_AGG('am', {rest})");
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let text = rows.first().and_then(|r| r.get("result")).cloned()?;
    if text.is_empty() {
        return None;
    }
    Some(text.parse().unwrap_or_else(|_| panic!("{sql}: '{text}'")))
}

/// `am` holds `red` (Landsat-style DN with nodata 0) and `qa` (INT8).
/// Cell `x` holds `red = 1000 * x` (so cell 0 is nodata) and `qa = x - 4`.
async fn create_am(server: &TestServer) {
    server
        .exec(
            "CREATE ARRAY am DIMS (x INT64 [0..15]) \
             ATTRS (red UINT16 NODATA 0 SCALE 0.0000275 OFFSET -0.2 UNITS 'reflectance', \
                    qa INT8) \
             TILE_EXTENTS (4)",
        )
        .await
        .expect("CREATE ARRAY am");
    let cells: Vec<String> = (0..8)
        .map(|x| format!("COORDS ({x}) VALUES ({}, {})", 1000 * x, x - 4))
        .collect();
    server
        .exec(&format!("INSERT INTO ARRAY am {}", cells.join(", ")))
        .await
        .expect("INSERT INTO ARRAY am");
    server
        .exec("SELECT ARRAY_FLUSH('am')")
        .await
        .expect("ARRAY_FLUSH");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_describe_show_attr_metadata() {
    let server = TestServer::start().await;
    create_am(&server).await;
    let rows = describe(&server, "am").await;
    assert_eq!(
        rows.get("red").map(String::as_str),
        Some("UINT16 NODATA 0.0 SCALE 2.75e-5 OFFSET -0.2 UNITS 'reflectance'")
    );
    assert_eq!(rows.get("qa").map(String::as_str), Some("INT8"));

    // A nodata the attr type cannot hold is a data exception.
    assert_eq!(
        sqlstate(
            &server,
            "CREATE ARRAY am_bad DIMS (x INT64 [0..3]) ATTRS (qa INT8 NODATA 200) \
             TILE_EXTENTS (4)"
        )
        .await,
        "22000"
    );
    assert_eq!(
        sqlstate(
            &server,
            "CREATE ARRAY am_bad DIMS (x INT64 [0..3]) ATTRS (label STRING NODATA 0) \
             TILE_EXTENTS (4)"
        )
        .await,
        "22000"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_attr_sets_and_drops_metadata_and_keeps_segments() {
    let server = TestServer::start().await;
    create_am(&server).await;

    server
        .exec("ALTER ARRAY am ALTER ATTR red SET (SCALE 0.5)")
        .await
        .expect("SET SCALE");
    server
        .exec("ALTER ARRAY am ALTER ATTR red DROP OFFSET, UNITS")
        .await
        .expect("DROP OFFSET, UNITS");
    assert_eq!(
        describe(&server, "am").await.get("red").map(String::as_str),
        Some("UINT16 NODATA 0.0 SCALE 0.5")
    );

    // The flushed segment stays readable: the change kept `schema_hash`.
    let raw = reds(&server, "SELECT * FROM ARRAY_SLICE('am', '{x: [0, 15]}')").await;
    assert_eq!(raw.len(), 8);
    assert_eq!(raw[&0], Some(0.0));
    assert_eq!(raw[&3], Some(3000.0));

    assert_eq!(
        sqlstate(&server, "ALTER ARRAY am ALTER ATTR red DROP OFFSET").await,
        "55000"
    );
    assert_eq!(
        sqlstate(&server, "ALTER ARRAY am ALTER ATTR red SET (NODATA -1)").await,
        "22000"
    );
    assert_eq!(
        sqlstate(&server, "ALTER ARRAY am ALTER ATTR nope SET (NODATA 0)").await,
        "42703"
    );

    server
        .exec("ALTER ARRAY am ALTER ATTR red DROP NODATA, SCALE")
        .await
        .expect("DROP the rest");
    assert_eq!(
        describe(&server, "am").await.get("red").map(String::as_str),
        Some("UINT16")
    );
    // Without a nodata, cell 0 counts.
    assert_eq!(agg(&server, "'red', 'count'").await, Some(8.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn physical_reads_apply_scale_offset_and_null_nodata() {
    let server = TestServer::start().await;
    create_am(&server).await;

    let physical = reds(
        &server,
        "SELECT * FROM ARRAY_SLICE('am', '{x: [0, 15]}', physical => true)",
    )
    .await;
    assert_eq!(physical[&0], None, "nodata reads as NULL");
    let want = 3000.0 * 0.0000275 - 0.2;
    let got = physical[&3].expect("cell 3");
    assert!((got - want).abs() < 1e-12, "{got} != {want}");

    // Raw sum skips nodata (it is 0 here, so the sum is unchanged), and the
    // physical sum is the sum of the physical values.
    let raw_sum: f64 = (1..8).map(|x| 1000.0 * f64::from(x)).sum();
    assert_eq!(agg(&server, "'red', 'sum'").await, Some(raw_sum));
    let physical_sum = agg(&server, "'red', 'sum', physical => true")
        .await
        .expect("sum");
    let want_sum = raw_sum * 0.0000275 - 7.0 * 0.2;
    assert!((physical_sum - want_sum).abs() < 1e-9, "{physical_sum}");

    // Nodata is not data: COUNT and MEAN skip cell 0.
    assert_eq!(agg(&server, "'red', 'count'").await, Some(7.0));
    assert_eq!(agg(&server, "'red', 'mean'").await, Some(raw_sum / 7.0));
    assert_eq!(agg(&server, "'red', 'min'").await, Some(1000.0));

    // An unknown named argument is refused, never dropped.
    assert!(
        server
            .exec("SELECT * FROM ARRAY_AGG('am', 'red', 'sum', colour => 1)")
            .await
            .is_err()
    );
    assert!(
        server
            .exec("SELECT * FROM ARRAY_SLICE('am', '{x: [0, 3]}', physical = true)")
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn int8_attrs_store_one_signed_byte() {
    let server = TestServer::start().await;
    create_am(&server).await;
    server
        .exec("INSERT INTO ARRAY am COORDS (8) VALUES (1, -128), COORDS (9) VALUES (1, 127)")
        .await
        .expect("INT8 extremes");
    assert_eq!(
        sqlstate(&server, "INSERT INTO ARRAY am COORDS (10) VALUES (1, 128)").await,
        "22003"
    );

    let rows = server
        .query_named_rows("SELECT * FROM ARRAY_SLICE('am', '{x: [8, 9]}')")
        .await
        .expect("slice");
    let qa: HashMap<String, String> = rows
        .into_iter()
        .map(|r| (r["x"].clone(), r["qa"].clone()))
        .collect();
    assert_eq!(qa.get("8").map(String::as_str), Some("-128"));
    assert_eq!(qa.get("9").map(String::as_str), Some("127"));

    // Cells 0..=7 hold -4..=3, then -128 and 127.
    assert_eq!(agg(&server, "'qa', 'min'").await, Some(-128.0));
    assert_eq!(agg(&server, "'qa', 'max'").await, Some(127.0));
    assert_eq!(agg(&server, "'qa', 'sum'").await, Some(-4.0 - 1.0));
    assert_eq!(agg(&server, "'qa', 'count'").await, Some(10.0));
}
