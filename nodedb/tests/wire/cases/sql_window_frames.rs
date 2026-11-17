// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for SQL window frame semantics.
//!
//! Covers ROWS, RANGE, and GROUPS frame variants, including peer-aware
//! behaviour, default frame resolution, and plan-time validation errors.

use crate::harness::TestServer;

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn create_nums(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION nums TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY,\
                n INT\
             )",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO nums (id, n) VALUES \
             ('a',1),('b',2),('c',3),('d',4),('e',5)",
        )
        .await
        .unwrap();
}

/// Create a collection with ties in the ordering column.
async fn create_tied(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION tied TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY,\
                grp INT,\
                n INT\
             )",
        )
        .await
        .unwrap();
    // grp values: [1,1,2,3,3] — two peer groups of size 2 and one singleton
    server
        .exec(
            "INSERT INTO tied (id, grp, n) VALUES \
             ('a',1,10),('b',1,10),('c',2,20),('d',3,30),('e',3,30)",
        )
        .await
        .unwrap();
}

fn parse_f64(s: &str) -> f64 {
    s.parse().unwrap_or(f64::NAN)
}

fn col(rows: &[Vec<String>], row: usize, col: usize) -> f64 {
    parse_f64(rows[row].get(col).map(String::as_str).unwrap_or("NaN"))
}

// ── Test 1: ROWS 1 PRECEDING / 1 FOLLOWING ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_1_preceding_1_following_sum() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, SUM(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s \
             FROM nums ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // row 0 (n=1): window [1,2]        → sum=3
    // row 1 (n=2): window [1,2,3]      → sum=6
    // row 2 (n=3): window [2,3,4]      → sum=9
    // row 3 (n=4): window [3,4,5]      → sum=12
    // row 4 (n=5): window [4,5]        → sum=9
    let expected = [3.0, 6.0, 9.0, 12.0, 9.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 2: ROWS UNBOUNDED PRECEDING TO CURRENT ROW (running sum) ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_unbounded_preceding_to_current_running_sum() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, SUM(n) OVER \
               (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \
             FROM nums ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    let expected = [1.0, 3.0, 6.0, 10.0, 15.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 3: ROWS CURRENT ROW TO UNBOUNDED FOLLOWING (reverse running sum) ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_current_to_unbounded_following_reverse_sum() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, SUM(n) OVER \
               (ORDER BY n ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s \
             FROM nums ORDER BY n",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // row 0: 1+2+3+4+5=15, row 1: 2+3+4+5=14, ..., row 4: 5
    let expected = [15.0, 14.0, 12.0, 9.0, 5.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 4: RANGE UNBOUNDED PRECEDING TO CURRENT ROW with ties ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_unbounded_preceding_current_row_peer_aware() {
    let server = TestServer::start().await;
    create_tied(&server).await;

    // n values (sorted): [10,10,20,30,30]
    // RANGE UNBOUNDED PRECEDING TO CURRENT ROW: each row sees all rows up to
    // and including all peers (rows with same n).
    let rows = server
        .query_rows(
            "SELECT id, SUM(n) OVER \
               (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \
             FROM tied ORDER BY n, id",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // rows a,b (n=10): both see sum of [10,10] = 20
    // row c  (n=20): sees sum of [10,10,20] = 40
    // rows d,e (n=30): both see sum of [10,10,20,30,30] = 100
    let expected = [20.0, 20.0, 40.0, 100.0, 100.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 5: GROUPS 1 PRECEDING AND 1 FOLLOWING ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_1_preceding_1_following_sum() {
    let server = TestServer::start().await;
    create_tied(&server).await;

    // grp values (sorted): [1,1,2,3,3] — groups [0,0,1,2,2]
    // GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING:
    //   rows a,b (group 0): frame spans groups max(0-1,0)=0..min(0+1,2)=1
    //     → rows with group 0 or 1 → n=[10,10,20] → sum=40
    //   row c (group 1): frame spans groups 0..2 → all rows → n=[10,10,20,30,30] → sum=100
    //   rows d,e (group 2): frame spans groups 1..2 → n=[20,30,30] → sum=80
    let rows = server
        .query_rows(
            "SELECT id, SUM(n) OVER \
               (ORDER BY grp GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s \
             FROM tied ORDER BY grp, id",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    let expected = [40.0, 40.0, 100.0, 80.0, 80.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 6: Default frame WITH ORDER BY → RANGE UNBOUNDED PRECEDING TO CURRENT ROW ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_frame_with_order_by_is_range_unbounded_to_current() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    // No explicit frame clause. PostgreSQL default with ORDER BY is
    // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    let rows = server
        .query_rows("SELECT id, SUM(n) OVER (ORDER BY n) AS s FROM nums ORDER BY n")
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // Running sum: [1, 3, 6, 10, 15]
    let expected = [1.0, 3.0, 6.0, 10.0, 15.0];
    for (i, &want) in expected.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - want).abs() < 1e-9,
            "row {i}: got {got}, want {want}; rows={rows:?}"
        );
    }
}

// ── Test 7: Default frame WITHOUT ORDER BY → whole partition ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_frame_without_order_by_is_whole_partition() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    // No ORDER BY and no frame clause → every row sees the full partition.
    let rows = server
        .query_rows("SELECT id, SUM(n) OVER () AS s FROM nums ORDER BY n")
        .await
        .unwrap();

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // Every row must see 1+2+3+4+5=15.
    for (i, row) in rows.iter().enumerate() {
        let got = col(&rows, i, 1);
        assert!(
            (got - 15.0).abs() < 1e-9,
            "row {i}: got {got}, want 15.0; row={row:?}"
        );
    }
}

// ── Test 8: GROUPS without ORDER BY → plan-time error ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_without_order_by_is_plan_time_error() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    let result = server
        .query_rows(
            "SELECT id, SUM(n) OVER \
               (GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \
             FROM nums ORDER BY n",
        )
        .await;

    assert!(
        result.is_err(),
        "GROUPS without ORDER BY must be rejected at plan time, got: {result:?}"
    );
    let msg = result.unwrap_err().to_lowercase();
    assert!(
        msg.contains("groups") || msg.contains("order") || msg.contains("invalid"),
        "error should mention GROUPS or ORDER BY: {msg}"
    );
}

// ── Expression arguments over an explicit frame ──────────────────────────────

/// A framed aggregate whose argument is an expression must average the
/// *evaluated* expression over the frame. A bare-column argument over the same
/// frame already computes correctly, so an all-NULL column here means the
/// argument expression was dropped rather than evaluated — a silently wrong
/// (empty) result rather than an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn framed_aggregate_over_expression_argument_averages_evaluated_values() {
    let server = TestServer::start().await;
    create_nums(&server).await;

    let rows = server
        .query_rows(
            "SELECT id, AVG(n * 2) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING \
             AND CURRENT ROW) AS a FROM nums ORDER BY n",
        )
        .await
        .expect("framed AVG over an expression argument must plan and execute");

    assert_eq!(rows.len(), 5, "rows: {rows:?}");

    // Argument n*2 = 2,4,6,8,10; running means = 2,3,4,5,6. PostgreSQL's
    // avg over integers is numeric with 16 fractional digits here.
    let expected = [
        "2.0000000000000000",
        "3.0000000000000000",
        "4.0000000000000000",
        "5.0000000000000000",
        "6.0000000000000000",
    ];
    for (i, want) in expected.iter().enumerate() {
        assert_eq!(rows[i][1], *want, "row {i}; rows={rows:?}");
    }
}
