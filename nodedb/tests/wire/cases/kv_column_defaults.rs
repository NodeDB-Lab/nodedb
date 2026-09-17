// SPDX-License-Identifier: BUSL-1.1

//! Column `DEFAULT` on the key-value engine.
//!
//! The KV engine stores the bytes it is handed and has no typed write path, so
//! a DEFAULT is materialized once, at plan time, into the row that gets written.
//! That is what makes `SELECT`, `RETURNING`, and the bytes on disk agree by
//! construction instead of by three read paths independently filling the same
//! hole — and it is why these tests check the value AFTER a restart, which is
//! the assertion a read-time synthesis could not survive.

use crate::harness::TestServer;

async fn create_kv(server: &TestServer, name: &str, columns: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} ({columns}) WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// A column the statement omits is filled from its DEFAULT, and the value is in
/// the stored bytes — so it is still there after the process restarts and the
/// row is rebuilt from the WAL, with no planner in the picture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_omitted_column_is_materialized_from_its_default_and_survives_restart() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_def_basic",
        "key TEXT PRIMARY KEY, n INT, status TEXT DEFAULT 'pending'",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_def_basic (key, n) VALUES ('k1', 1) RETURNING key, n, status",
        )
        .await
        .expect("KV INSERT RETURNING must return the stored row");
    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(
        returned[0].get("status").map(String::as_str),
        Some("pending"),
        "RETURNING must show the materialized default: {returned:?}"
    );

    let selected = server
        .query_named_rows("SELECT key, n, status FROM kv_def_basic WHERE key = 'k1'")
        .await
        .expect("read the same key back");
    assert_eq!(
        selected[0].get("status").map(String::as_str),
        Some("pending"),
        "SELECT must show the materialized default: {selected:?}"
    );

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let after = server
        .query_named_rows("SELECT key, n, status FROM kv_def_basic WHERE key = 'k1'")
        .await
        .expect("read the key back after restart");
    assert_eq!(after.len(), 1, "the row must have survived: {after:?}");
    assert_eq!(
        after[0].get("status").map(String::as_str),
        Some("pending"),
        "the default was written into the row, so a restart cannot lose it: {after:?}"
    );
}

/// A value the statement supplies wins over the DEFAULT. The default fills a
/// hole; it never overwrites a choice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_supplied_value_wins_over_the_default() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_def_supplied",
        "key TEXT PRIMARY KEY, status TEXT DEFAULT 'pending'",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_def_supplied (key, status) VALUES ('k1', 'shipped') \
             RETURNING key, status",
        )
        .await
        .expect("insert with an explicit value");
    assert_eq!(
        returned[0].get("status").map(String::as_str),
        Some("shipped"),
        "the supplied value must not be replaced by the default: {returned:?}"
    );

    let selected = server
        .query_named_rows("SELECT status FROM kv_def_supplied WHERE key = 'k1'")
        .await
        .expect("read back");
    assert_eq!(
        selected[0].get("status").map(String::as_str),
        Some("shipped"),
        "the stored bytes must hold the supplied value: {selected:?}"
    );
}

/// An explicitly supplied `NULL` is a value the author chose, not an omission.
/// Overwriting it with the default would make a declared-DEFAULT column unable
/// to hold NULL at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_null_is_not_overwritten_by_the_default() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_def_null",
        "key TEXT PRIMARY KEY, status TEXT DEFAULT 'pending'",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_def_null (key, status) VALUES ('k1', NULL) RETURNING key, status",
        )
        .await
        .expect("insert with an explicit NULL");
    // The harness renders a NULL column as the empty string, so "" and an
    // absent column both mean "the default was not applied" — which is the
    // claim under test.
    assert_eq!(
        returned[0].get("status").map(String::as_str).unwrap_or(""),
        "",
        "an explicit NULL must stay NULL: {returned:?}"
    );

    let selected = server
        .query_named_rows("SELECT status FROM kv_def_null WHERE key = 'k1'")
        .await
        .expect("read back");
    assert_eq!(
        selected[0].get("status").map(String::as_str).unwrap_or(""),
        "",
        "the stored row must hold NULL, not the default: {selected:?}"
    );
}

/// UPSERT reaches the same expansion point as plain INSERT (all three KV write
/// entry points share one planner helper), so an omitted column is filled there
/// too rather than only on the path that happened to get a test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_materializes_an_omitted_columns_default() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_def_upsert",
        "key TEXT PRIMARY KEY, n INT, status TEXT DEFAULT 'pending'",
    )
    .await;

    server
        .exec("UPSERT INTO kv_def_upsert (key, n) VALUES ('u1', 5)")
        .await
        .expect("upsert must succeed");

    let selected = server
        .query_named_rows("SELECT key, n, status FROM kv_def_upsert WHERE key = 'u1'")
        .await
        .expect("read back");
    assert_eq!(selected.len(), 1, "one row: {selected:?}");
    assert_eq!(
        selected[0].get("status").map(String::as_str),
        Some("pending"),
        "UPSERT must materialize the default too: {selected:?}"
    );
}

/// A `DEFAULT` beyond the declared width is refused where it is declared,
/// so no insert can reach it: the `CREATE` fails with the same range error a
/// supplied literal gets, naming the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_beyond_the_declared_width_is_rejected_like_a_supplied_literal() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "CREATE COLLECTION kv_def_range (key TEXT PRIMARY KEY, n SMALLINT DEFAULT 999999) \
             WITH (engine='kv')",
            "range",
        )
        .await;
}
