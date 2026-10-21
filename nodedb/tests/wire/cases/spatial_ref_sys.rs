// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SPATIAL_REF_SYS` and `DROP SPATIAL_REF_SYS` over pgwire: a
//! user-defined CRS in each text form, `ST_Transform` to it, the
//! `nodedb.spatial_ref_sys` view, the tombstone a drop leaves, the code and
//! privilege checks, and a restart.

use nodedb_crs::{CrsContext, CustomCrsSet, Srid, parse_proj_string};

use crate::harness::TestServer;

/// The user-defined Transverse Mercator every case defines.
const TEST_TM: &str =
    "+proj=tmerc +lat_0=0 +lon_0=101.25 +k=0.99991 +x_0=123456 +y_0=0 +ellps=GRS80 +units=m";

/// Largest error accepted against a projected value, metres.
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

async fn define(srv: &TestServer, code: u32, clause: &str) {
    let sql = format!("CREATE SPATIAL_REF_SYS {code} (NAME 'Test TM', {clause})");
    srv.exec(&sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// `(x, y)` of `(lon, lat)` WGS 84 transformed to `code` on the server.
async fn transformed(srv: &TestServer, lon: f64, lat: f64, code: u32) -> (f64, f64) {
    let geom = format!("ST_Transform(ST_Point({lon:?}, {lat:?}), {code})");
    let x = number(srv, &format!("SELECT ST_X({geom})")).await;
    let y = number(srv, &format!("SELECT ST_Y({geom})")).await;
    (x, y)
}

/// `(x, y)` of `(lon, lat)` in [`TEST_TM`], computed in the test.
fn expected(lon: f64, lat: f64) -> (f64, f64) {
    let srid = Srid::new(910_100).expect("srid");
    let mut set = CustomCrsSet::new();
    set.define(parse_proj_string(srid, "Test TM", TEST_TM).expect("parses"))
        .expect("defines");
    let t = CrsContext::new(&set)
        .transformer(Srid::WGS84, srid)
        .expect("defined");
    let [x, y, _] = t.apply([lon, lat, 0.0]).expect("inside the projection");
    (x, y)
}

/// A PROJ string, its WKT2 and its PROJJSON define one CRS: `ST_Transform`
/// to each lands on the same coordinates as the test computes.
#[tokio::test]
async fn each_text_form_defines_the_same_crs() {
    let srv = TestServer::start().await;
    let def = parse_proj_string(Srid::new(910_100).expect("srid"), "x", TEST_TM).expect("parses");
    define(&srv, 910_101, &format!("PROJ '{TEST_TM}'")).await;
    define(&srv, 910_102, &format!("WKT2 '{}'", def.to_wkt2())).await;
    define(&srv, 910_103, &format!("PROJJSON '{}'", def.to_projjson())).await;

    let (origin_x, origin_y) = transformed(&srv, 101.25, 0.0, 910_101).await;
    assert!(
        (origin_x - 123_456.0).abs() < MM && origin_y.abs() < MM,
        "the origin maps to the false origin: ({origin_x}, {origin_y})"
    );
    let (want_x, want_y) = expected(103.8, 1.45);
    for code in [910_101, 910_102, 910_103] {
        let (x, y) = transformed(&srv, 103.8, 1.45, code).await;
        assert!(
            (x - want_x).abs() < MM && (y - want_y).abs() < MM,
            "SRID {code}: got ({x}, {y}), want ({want_x}, {want_y})"
        );
    }
    let back = "ST_Transform(ST_Transform(ST_Point(103.8, 1.45), 910101), 4326)";
    let lon = number(&srv, &format!("SELECT ST_X({back})")).await;
    let lat = number(&srv, &format!("SELECT ST_Y({back})")).await;
    assert!(
        (lon - 103.8).abs() < 1e-9 && (lat - 1.45).abs() < 1e-9,
        "({lon}, {lat})"
    );
}

/// The view lists built-ins and live user-defined CRSs, never a dropped
/// one.
#[tokio::test]
async fn the_view_lists_builtins_and_live_definitions() {
    let srv = TestServer::start().await;
    define(&srv, 910_111, &format!("PROJ '{TEST_TM}'")).await;
    let rows = srv
        .query_rows(
            "SELECT srid, auth_name, auth_srid, name, kind, proj4text, srtext \
             FROM nodedb.spatial_ref_sys WHERE srid = 910111",
        )
        .await
        .expect("view reads");
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row[0], "910111");
    assert_eq!(row[1], "NODEDB");
    assert_eq!(row[2], "910111");
    assert_eq!(row[3], "Test TM");
    assert_eq!(row[4], "projected");
    assert!(row[5].contains("+proj=tmerc"), "{}", row[5]);
    assert!(row[6].starts_with("PROJCRS["), "{}", row[6]);

    let wgs84 = srv
        .query_rows(
            "SELECT auth_name, kind, datum_shift_accuracy_m FROM nodedb.spatial_ref_sys \
             WHERE srid = 4326",
        )
        .await
        .expect("view reads");
    assert_eq!(wgs84.len(), 1, "{wgs84:?}");
    assert_eq!(wgs84[0][0], "EPSG");
    assert_eq!(wgs84[0][1], "geographic");

    srv.exec("DROP SPATIAL_REF_SYS 910111").await.expect("drop");
    let gone = srv
        .query_rows("SELECT srid FROM nodedb.spatial_ref_sys WHERE srid = 910111")
        .await
        .expect("view reads");
    assert!(gone.is_empty(), "{gone:?}");
}

/// After a drop, stored data keeps its code, a transform of it fails with
/// the dropped error, and the code cannot be defined again.
#[tokio::test]
async fn a_dropped_code_fails_and_is_never_reused() {
    let srv = TestServer::start().await;
    define(&srv, 910_121, &format!("PROJ '{TEST_TM}'")).await;
    srv.exec("CREATE COLLECTION srs_drop")
        .await
        .expect("collection");
    srv.exec(
        "INSERT INTO srs_drop (id, loc) VALUES ('a', ST_SetSRID(ST_Point(123456, 0), 910121))",
    )
    .await
    .expect("insert in the user-defined SRID");
    srv.exec("DROP SPATIAL_REF_SYS 910121").await.expect("drop");

    let ids = srv
        .query_rows("SELECT id FROM srs_drop")
        .await
        .expect("the row stays readable");
    assert_eq!(ids.len(), 1, "{ids:?}");
    srv.expect_error(
        "SELECT ST_Transform(loc, 4326) FROM srs_drop",
        "SRID 910121 was dropped",
    )
    .await;
    srv.expect_error(
        "SELECT ST_Transform(ST_Point(101.25, 0.0), 910121)",
        "SRID 910121 was dropped",
    )
    .await;
    srv.expect_error(
        &format!("CREATE SPATIAL_REF_SYS 910121 (NAME 'again', PROJ '{TEST_TM}')"),
        "was dropped and cannot be reused",
    )
    .await;
    srv.expect_error("DROP SPATIAL_REF_SYS 910121", "42704")
        .await;
}

/// A drop of a code a collection definition names as an SRID is refused
/// with the definition named. The same number in another role holds
/// nothing. Once the naming definition goes, the drop proceeds.
#[tokio::test]
async fn a_named_code_is_not_dropped() {
    let srv = TestServer::start().await;
    define(&srv, 910_151, &format!("PROJ '{TEST_TM}'")).await;
    srv.exec("CREATE COLLECTION srs_dep")
        .await
        .expect("collection");
    srv.exec(
        "ALTER COLLECTION srs_dep ADD CONSTRAINT in_tm \
         CHECK (ST_X(ST_Transform(NEW.loc, 910151)) > 0)",
    )
    .await
    .expect("constraint");
    srv.exec(
        "ALTER COLLECTION srs_dep ADD CONSTRAINT under_code \
         CHECK (NEW.amount < 910151)",
    )
    .await
    .expect("constraint");
    srv.expect_error("DROP SPATIAL_REF_SYS 910151", "2BP01")
        .await;
    srv.expect_error(
        "DROP SPATIAL_REF_SYS 910151",
        "collection 'srs_dep' (tenant",
    )
    .await;
    srv.expect_error("DROP SPATIAL_REF_SYS 910151", "CHECK constraint 'in_tm'")
        .await;
    assert!(
        transformed(&srv, 101.25, 0.0, 910_151).await.0.is_finite(),
        "the refused drop leaves the code live"
    );

    srv.exec("DROP CONSTRAINT in_tm ON srs_dep")
        .await
        .expect("drop constraint");
    srv.exec("DROP SPATIAL_REF_SYS 910151")
        .await
        .expect("`amount < 910151` does not name the code as an SRID");
}

/// A define returns once every core holds the set: the next statement on
/// any core resolves the code.
#[tokio::test]
async fn a_defined_code_resolves_on_the_next_statement() {
    let srv = TestServer::start().await;
    define(&srv, 910_161, &format!("PROJ '{TEST_TM}'")).await;
    srv.exec("CREATE COLLECTION srs_next")
        .await
        .expect("collection");
    for i in 0..16 {
        srv.exec(&format!(
            "INSERT INTO srs_next (id, loc) VALUES ('r{i}', \
             ST_Transform(ST_SetSRID(ST_Point(123456, 0), 910161), 4326))"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {i} resolves the new code: {e}"));
    }
}

#[tokio::test]
async fn codes_and_definitions_are_checked() {
    let srv = TestServer::start().await;
    define(&srv, 910_131, &format!("PROJ '{TEST_TM}'")).await;
    srv.expect_error(
        &format!("CREATE SPATIAL_REF_SYS 910131 (NAME 'twice', PROJ '{TEST_TM}')"),
        "already defined",
    )
    .await;
    for code in [4326, 909_999, 990_001, 999_999] {
        srv.expect_error(
            &format!("CREATE SPATIAL_REF_SYS {code} (NAME 'x', PROJ '{TEST_TM}')"),
            "outside the user range",
        )
        .await;
    }
    srv.expect_error("DROP SPATIAL_REF_SYS 4326", "22023").await;
    srv.expect_error(
        "CREATE SPATIAL_REF_SYS 910132 (NAME 'x', PROJ '+proj=bonne +lat_1=10')",
        "invalid PROJ definition",
    )
    .await;
    srv.expect_error(
        "CREATE SPATIAL_REF_SYS 910133 (NAME 'x', WKT2 'PROJCS[\"x\"]')",
        "invalid WKT2 definition",
    )
    .await;
    srv.expect_error(
        "CREATE SPATIAL_REF_SYS 910134 (NAME 'x', PROJJSON '{')",
        "invalid PROJJSON definition",
    )
    .await;
}

#[tokio::test]
async fn a_non_superuser_is_refused() {
    let srv = TestServer::start().await;
    srv.exec("CREATE USER srs_user PASSWORD 'srs_pass_1'")
        .await
        .expect("create user");
    srv.exec("GRANT ROLE readwrite TO srs_user")
        .await
        .expect("grant role");
    let (client, _task) = srv
        .connect_as("srs_user", "srs_pass_1")
        .await
        .expect("connect as srs_user");
    for sql in [
        format!("CREATE SPATIAL_REF_SYS 910141 (NAME 'x', PROJ '{TEST_TM}')"),
        "DROP SPATIAL_REF_SYS 910141".to_string(),
    ] {
        let err = client
            .simple_query(&sql)
            .await
            .expect_err("a non-superuser is refused");
        let code = err.as_db_error().map(|db| db.code().code().to_string());
        assert_eq!(code.as_deref(), Some("42501"), "{sql}: {err:?}");
    }
    srv.expect_error("SELECT ST_Transform(ST_Point(1, 2), 910141)", "not defined")
        .await;
}

/// A definition and a tombstone survive a restart, and every core resolves
/// them from the first statement.
#[tokio::test]
async fn definitions_survive_restart() {
    let srv = TestServer::start_multicores(2).await;
    define(&srv, 910_151, &format!("PROJ '{TEST_TM}'")).await;
    define(&srv, 910_152, &format!("PROJ '{TEST_TM}'")).await;
    srv.exec("DROP SPATIAL_REF_SYS 910152").await.expect("drop");
    srv.exec("CREATE COLLECTION srs_restart")
        .await
        .expect("collection");
    srv.exec(
        "INSERT INTO srs_restart (id, loc) VALUES ('a', ST_SetSRID(ST_Point(123456, 0), 910151))",
    )
    .await
    .expect("insert");

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path(dir).await;

    let (want_x, want_y) = expected(103.8, 1.45);
    let (x, y) = transformed(&srv, 103.8, 1.45, 910_151).await;
    assert!(
        (x - want_x).abs() < MM && (y - want_y).abs() < MM,
        "after restart: ({x}, {y})"
    );
    let lon = number(
        &srv,
        "SELECT ST_X(ST_Transform(loc, 4326)) FROM srs_restart",
    )
    .await;
    assert!((lon - 101.25).abs() < 1e-9, "{lon}");
    srv.expect_error(
        "SELECT ST_Transform(ST_Point(101.25, 0.0), 910152)",
        "SRID 910152 was dropped",
    )
    .await;
    srv.expect_error(
        &format!("CREATE SPATIAL_REF_SYS 910152 (NAME 'again', PROJ '{TEST_TM}')"),
        "cannot be reused",
    )
    .await;
}
