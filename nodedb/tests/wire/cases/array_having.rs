// SPDX-License-Identifier: BUSL-1.1

//! `HAVING` over an `ARRAY_AGG` or `ARRAY_ELEMENTWISE` relation filters
//! groups. A star beside it is a grouping error, never a silent drop.

use crate::harness::TestServer;

async fn create_arrays(server: &TestServer) {
    for name in ["e1", "e2"] {
        server
            .exec(&format!(
                "CREATE ARRAY {name} DIMS (x INT64 [0..3], y INT64 [0..3]) \
                 ATTRS (a INT64) TILE_EXTENTS (2, 2)"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
        server
            .exec(&format!(
                "INSERT INTO ARRAY {name} COORDS (0, 0) VALUES (1), COORDS (0, 1) VALUES (2), \
                 COORDS (1, 0) VALUES (3)"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT INTO ARRAY {name}: {e}"));
        server
            .exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
            .await
            .unwrap_or_else(|e| panic!("ARRAY_FLUSH {name}: {e}"));
    }
}

#[tokio::test]
async fn star_with_having_over_elementwise_is_a_grouping_error() {
    let server = TestServer::start().await;
    create_arrays(&server).await;
    server
        .expect_error(
            "SELECT * FROM ARRAY_ELEMENTWISE('e1', 'e2', 'add', 'a') HAVING COUNT(*) > 100",
            "42803",
        )
        .await;
}

#[tokio::test]
async fn having_over_agg_removes_the_rows_it_rejects() {
    let server = TestServer::start().await;
    create_arrays(&server).await;
    let sql = "SELECT 1 AS one FROM ARRAY_AGG('e1', 'a', 'sum') HAVING COUNT(*) > 5";
    let rows = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert!(rows.is_empty(), "{sql}: HAVING rejects the group: {rows:?}");
}
