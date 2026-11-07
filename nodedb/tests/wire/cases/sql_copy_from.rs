// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `COPY <collection> FROM '<path>'` bulk import.
//!
//! Spins up a full NodeDB server via the pgwire harness and exercises
//! NDJSON, JSON array, and CSV import paths over the wire.

use std::io::Write;

use crate::harness::TestServer;

// ── helpers ─────────────────────────────────────────────────────────────────

/// Write content to a temporary file and return its path.
/// The caller is responsible for keeping the `tempfile::NamedTempFile` alive.
fn write_temp(content: &str, suffix: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .suffix(suffix)
        .tempfile()
        .expect("create temp file");
    f.write_all(content.as_bytes()).expect("write temp file");
    f.flush().expect("flush temp file");
    f
}

/// Count rows in a collection via SELECT COUNT(*).
async fn count_rows(srv: &TestServer, collection: &str) -> i64 {
    let rows = srv
        .query_text(&format!("SELECT COUNT(*) FROM {collection}"))
        .await
        .expect("SELECT COUNT(*)");
    rows.first()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

// ── test 1: NDJSON import ────────────────────────────────────────────────────

#[tokio::test]
async fn copy_ndjson_roundtrip() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_ndjson_test (id INT, name TEXT, score FLOAT)")
        .await
        .expect("CREATE COLLECTION");

    let ndjson = r#"{"id": 1, "name": "alice", "score": 9.5}
{"id": 2, "name": "bob", "score": 8.1}
{"id": 3, "name": "carol", "score": 7.7}
{"id": 4, "name": "dave", "score": 6.2}
{"id": 5, "name": "eve", "score": 5.0}
"#;
    let f = write_temp(ndjson, ".ndjson");
    let path = f.path().to_string_lossy().to_string();

    srv.exec(&format!("COPY copy_ndjson_test FROM '{path}'"))
        .await
        .expect("COPY NDJSON");

    assert_eq!(count_rows(&srv, "copy_ndjson_test").await, 5);
}

// ── test 2: JSON array import ────────────────────────────────────────────────

#[tokio::test]
async fn copy_json_array_roundtrip() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_json_arr_test (id INT, name TEXT, score FLOAT)")
        .await
        .expect("CREATE COLLECTION");

    let json = r#"[
  {"id": 1, "name": "alice", "score": 9.5},
  {"id": 2, "name": "bob",   "score": 8.1},
  {"id": 3, "name": "carol", "score": 7.7},
  {"id": 4, "name": "dave",  "score": 6.2},
  {"id": 5, "name": "eve",   "score": 5.0}
]"#;
    let f = write_temp(json, ".json");
    let path = f.path().to_string_lossy().to_string();

    srv.exec(&format!("COPY copy_json_arr_test FROM '{path}'"))
        .await
        .expect("COPY JSON array");

    assert_eq!(count_rows(&srv, "copy_json_arr_test").await, 5);
}

// ── test 3: CSV import with header row ──────────────────────────────────────

#[tokio::test]
async fn copy_csv_with_header() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_csv_test (id INT, name TEXT, score FLOAT)")
        .await
        .expect("CREATE COLLECTION");

    let csv = "id,name,score\n1,alice,9.5\n2,bob,8.1\n3,carol,7.7\n4,dave,6.2\n5,eve,5.0\n";
    let f = write_temp(csv, ".csv");
    let path = f.path().to_string_lossy().to_string();

    srv.exec(&format!("COPY copy_csv_test FROM '{path}'"))
        .await
        .expect("COPY CSV");

    assert_eq!(count_rows(&srv, "copy_csv_test").await, 5);
}

// ── test 4: auto-detect extension ────────────────────────────────────────────

#[tokio::test]
async fn copy_autodetect_ndjson_extension() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_ext_test (id INT, name TEXT)")
        .await
        .expect("CREATE COLLECTION");

    let ndjson = "{\"id\": 1, \"name\": \"alice\"}\n{\"id\": 2, \"name\": \"bob\"}\n";
    let f = write_temp(ndjson, ".ndjson");
    let path = f.path().to_string_lossy().to_string();
    // Path ends in .ndjson — no WITH clause needed.
    srv.exec(&format!("COPY copy_ext_test FROM '{path}'"))
        .await
        .expect("COPY with .ndjson extension");

    assert_eq!(count_rows(&srv, "copy_ext_test").await, 2);
}

// ── test 5: explicit FORMAT csv with non-default delimiter ──────────────────

#[tokio::test]
async fn copy_csv_semicolon_delimiter() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_semi_test (id INT, name TEXT, val FLOAT)")
        .await
        .expect("CREATE COLLECTION");

    let csv = "id;name;val\n1;alice;1.1\n2;bob;2.2\n3;carol;3.3\n";
    // Write with an arbitrary suffix — format will be given explicitly.
    let f = write_temp(csv, ".txt");
    let path = f.path().to_string_lossy().to_string();

    srv.exec(&format!(
        "COPY copy_semi_test FROM '{path}' WITH (FORMAT csv, DELIMITER ';')"
    ))
    .await
    .expect("COPY CSV DELIMITER ;");

    assert_eq!(count_rows(&srv, "copy_semi_test").await, 3);
}

// ── test 6: HEADER false (positional columns) ────────────────────────────────

#[tokio::test]
async fn copy_csv_header_false() {
    let srv = TestServer::start().await;

    // Use a schemaless collection so positional column names (col_0, col_1)
    // are accepted without conflict with a declared schema.
    srv.exec("CREATE COLLECTION copy_noheader_test")
        .await
        .expect("CREATE COLLECTION");

    // Three rows, no header — columns become col_0, col_1 (positional names).
    // Distinct first-column values ensure no key collision.
    let csv = "alice,9.5\nbob,8.1\ncarol,7.7\n";
    let f = write_temp(csv, ".csv");
    let path = f.path().to_string_lossy().to_string();

    srv.exec(&format!(
        "COPY copy_noheader_test FROM '{path}' WITH (FORMAT csv, HEADER false)"
    ))
    .await
    .expect("COPY CSV HEADER false");

    assert_eq!(count_rows(&srv, "copy_noheader_test").await, 3);
}

// ── test 7: path with .. rejected ────────────────────────────────────────────

#[tokio::test]
async fn copy_path_traversal_rejected() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_security_test (id INT)")
        .await
        .expect("CREATE COLLECTION");

    srv.expect_error("COPY copy_security_test FROM '/tmp/../etc/passwd'", "..")
        .await;
}

// ── test 8: timeseries collection rejected ───────────────────────────────────

#[tokio::test]
async fn copy_timeseries_rejected() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION copy_ts_test (ts TIMESTAMP, val FLOAT) \
         WITH (engine='timeseries', time_key='ts', interval='1h')",
    )
    .await
    .expect("CREATE COLLECTION timeseries");

    let ndjson = "{\"ts\": 1000, \"val\": 1.0}\n";
    let f = write_temp(ndjson, ".ndjson");
    let path = f.path().to_string_lossy().to_string();

    srv.expect_error(&format!("COPY copy_ts_test FROM '{path}'"), "timeseries")
        .await;
}

// ── test 9: per-row failure rolls back entire COPY ──────────────────────────

#[tokio::test]
async fn copy_ndjson_row_failure_aborts() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_fail_test (id INT, name TEXT)")
        .await
        .expect("CREATE COLLECTION");

    // Lines 1-2 are fine; line 3 is malformed JSON.
    let ndjson = "{\"id\": 1, \"name\": \"alice\"}\n\
                  {\"id\": 2, \"name\": \"bob\"}\n\
                  {not valid json at all\n\
                  {\"id\": 4, \"name\": \"dave\"}\n";
    let f = write_temp(ndjson, ".ndjson");
    let path = f.path().to_string_lossy().to_string();

    let result = srv
        .exec(&format!("COPY copy_fail_test FROM '{path}'"))
        .await;
    assert!(result.is_err(), "expected COPY to fail on malformed row");

    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("3") || err_msg.to_lowercase().contains("row"),
        "error should reference row 3, got: {err_msg}"
    );

    // No rows should have been committed (all-or-nothing semantics).
    assert_eq!(count_rows(&srv, "copy_fail_test").await, 0);
}

/// A malformed row late in the file writes no row, whatever the format.
#[tokio::test]
async fn copy_csv_and_json_array_bad_row_writes_nothing() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION copy_bad_csv (id INT, name TEXT)")
        .await
        .expect("CREATE COLLECTION");
    let f = write_temp("id,name\n1,alice\n2,bob\n3\n", ".csv");
    let path = f.path().to_string_lossy().to_string();
    let result = srv.exec(&format!("COPY copy_bad_csv FROM '{path}'")).await;
    assert!(result.is_err(), "row 4 has one column");
    assert_eq!(count_rows(&srv, "copy_bad_csv").await, 0);

    srv.exec("CREATE COLLECTION copy_bad_json (id INT, name TEXT)")
        .await
        .expect("CREATE COLLECTION");
    for json in [
        r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, 3]"#,
        r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}] trailing"#,
    ] {
        let f = write_temp(json, ".json");
        let path = f.path().to_string_lossy().to_string();
        let result = srv.exec(&format!("COPY copy_bad_json FROM '{path}'")).await;
        assert!(result.is_err(), "{json} must be refused");
        assert_eq!(count_rows(&srv, "copy_bad_json").await, 0, "{json}");
    }
}
