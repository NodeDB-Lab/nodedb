// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for volatile DEFAULT expression re-evaluation.
//! A volatile DEFAULT (`UUID_V7()`, `NOW()`, `nextval(...)`) evaluates once
//! per execution, never once per cached plan.

use crate::harness::TestServer;

/// `DEFAULT UUID_V7()` produces a distinct value for each of three inserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uuid_default_produces_a_distinct_value_per_row() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vol_uuid_rows (\
                id TEXT PRIMARY KEY, \
                u TEXT DEFAULT UUID_V7()) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO vol_uuid_rows (id) VALUES ('k1')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO vol_uuid_rows (id) VALUES ('k2')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO vol_uuid_rows (id) VALUES ('k3')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT u FROM vol_uuid_rows ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "three rows expected: {rows:?}");
    for row in &rows {
        assert_not_null(row, "u");
    }
    let distinct: std::collections::HashSet<&str> = rows.iter().map(|r| r.as_str()).collect();
    assert_eq!(
        distinct.len(),
        3,
        "each row must carry a distinct UUID, got {rows:?}"
    );
}

/// Every volatile DEFAULT re-evaluates when the identical INSERT text runs
/// three times. A cached plan must never replay a frozen value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volatile_defaults_re_evaluate_for_repeated_identical_statements() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vol_uuid_cached (\
                id TEXT DEFAULT UUID_V7() PRIMARY KEY, \
                t TIMESTAMP DEFAULT NOW(), \
                v TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    // The three statements are byte-identical, so the plan cache keys them the same.
    // NOW() renders at second granularity, so the gap must exceed one second.
    for _ in 0..3 {
        server
            .exec("INSERT INTO vol_uuid_cached (v) VALUES ('same')")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    }

    let rows = server
        .query_text("SELECT id FROM vol_uuid_cached")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "a repeated default collapses rows on a primary key, got {rows:?}"
    );
    let distinct: std::collections::HashSet<&str> = rows.iter().map(|r| r.as_str()).collect();
    assert_eq!(
        distinct.len(),
        3,
        "each execution must produce a distinct id, got {rows:?}"
    );

    let stamps = server
        .query_text("SELECT t FROM vol_uuid_cached")
        .await
        .unwrap();
    assert_eq!(stamps.len(), 3, "three rows expected: {stamps:?}");
    for stamp in &stamps {
        assert_not_null(stamp, "t");
    }
    let distinct_stamps: std::collections::HashSet<&str> =
        stamps.iter().map(|r| r.as_str()).collect();
    assert!(
        distinct_stamps.len() > 1,
        "NOW() must advance across executions, got {stamps:?}"
    );
}

/// `DEFAULT nextval('seq')` advances when the identical INSERT text runs
/// three times, filling a strict-engine primary key each time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_default_advances_across_repeated_identical_statements() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_vol_cached;")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION vol_seq_cached (\
                id BIGINT DEFAULT nextval('seq_vol_cached') PRIMARY KEY, \
                v TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    for _ in 0..3 {
        server
            .exec("INSERT INTO vol_seq_cached (v) VALUES ('same')")
            .await
            .unwrap();
    }

    let rows = server
        .query_text("SELECT id FROM vol_seq_cached ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
        "nextval must advance across repeated executions, got {rows:?}"
    );
}

/// Asserts a rendered row carries a real value in place of an absent or NULL column.
fn assert_not_null(row: &str, label: &str) {
    let trimmed = row.trim();
    assert!(
        !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("null"),
        "{label}: expected a value, got `{row}`"
    );
}
