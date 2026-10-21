// SPDX-License-Identifier: BUSL-1.1

//! Planar measures in a CRS whose unit is not the metre. EPSG:2263 (NAD83 /
//! New York Long Island) counts US survey feet: distances, lengths and
//! areas come back in metres, and an `ST_DWithin` distance is metres.

use crate::harness::TestServer;

/// Metres per US survey foot.
const US_FOOT_M: f64 = 1200.0 / 3937.0;

async fn number(srv: &TestServer, sql: &str) -> f64 {
    let rows = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    assert_eq!(rows.len(), 1, "`{sql}`: {rows:?}");
    rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("`{sql}` must return a number, got {rows:?}"))
}

#[tokio::test]
async fn feet_crs_measures_report_metres() {
    let srv = TestServer::start().await;
    let d = number(
        &srv,
        "SELECT ST_Distance(ST_SetSRID(ST_Point(1000000, 200000), 2263), \
         ST_SetSRID(ST_Point(1001000, 200000), 2263))",
    )
    .await;
    assert!((d - 1000.0 * US_FOOT_M).abs() < 1e-6, "{d}");
    let metre = number(
        &srv,
        "SELECT ST_Distance(ST_SetSRID(ST_Point(500000, 0), 32648), \
         ST_SetSRID(ST_Point(501000, 0), 32648))",
    )
    .await;
    assert!((metre - 1000.0).abs() < 1e-9, "{metre}");
    let area = number(
        &srv,
        "SELECT ST_Area(ST_SetSRID(ST_GeomFromText(\
         'POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))'), 2263))",
    )
    .await;
    assert!(
        (area - 10_000.0 * US_FOOT_M * US_FOOT_M).abs() < 1e-6,
        "{area}"
    );
}

/// A spatial index in feet answers `ST_DWithin` in metres: 300 ft is about
/// 91.44 m.
#[tokio::test]
async fn feet_index_dwithin_is_metres() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ft_pts COLUMNS (id TEXT, loc GEOMETRY) WITH (engine='spatial')")
        .await
        .expect("collection");
    srv.exec(
        "INSERT INTO ft_pts (id, loc) VALUES \
         ('near', ST_SetSRID(ST_Point(1000300, 200000), 2263))",
    )
    .await
    .expect("insert");
    for (metres, want) in [(92.0, 1usize), (91.0, 0)] {
        let rows = srv
            .query_rows(&format!(
                "SELECT id FROM ft_pts WHERE \
                 ST_DWithin(loc, ST_SetSRID(ST_Point(1000000, 200000), 2263), {metres})"
            ))
            .await
            .expect("query");
        assert_eq!(rows.len(), want, "{metres} m: {rows:?}");
    }
}
