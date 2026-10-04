// SPDX-License-Identifier: BUSL-1.1

//! An integer literal past `i64::MAX` keeps its exact number end to end.
//!
//! `18446744073709551615` (`u64::MAX`) is stored as a msgpack `uint64` and
//! reads back digit for digit. WHERE equality finds it, and ORDER BY puts it
//! after every smaller integer. A literal past the exact numeric range is
//! refused rather than rounded.

use crate::harness::TestServer;

const U64_MAX: &str = "18446744073709551615";
const JUST_ABOVE_I64: &str = "9223372036854775808";

/// Create `collection` with `create` and insert the three test rows.
async fn seed(srv: &TestServer, create: &str, collection: &str) {
    srv.exec(create).await.unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, v) VALUES ('max', {U64_MAX}), \
         ('mid', {JUST_ABOVE_I64}), ('small', 5)"
    ))
    .await
    .unwrap();
}

/// The one-column result of `sql`, row by row.
async fn column(srv: &TestServer, sql: &str) -> Vec<String> {
    srv.query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

/// Read back, WHERE equality, and ORDER BY on `collection`.
async fn check(srv: &TestServer, collection: &str) {
    assert_eq!(
        column(srv, &format!("SELECT v FROM {collection} WHERE id = 'max'")).await,
        vec![U64_MAX.to_string()],
        "{collection}: u64::MAX reads back exactly"
    );
    assert_eq!(
        column(srv, &format!("SELECT v FROM {collection} WHERE id = 'mid'")).await,
        vec![JUST_ABOVE_I64.to_string()],
        "{collection}: i64::MAX + 1 reads back exactly"
    );
    assert_eq!(
        column(
            srv,
            &format!("SELECT id FROM {collection} WHERE v = {U64_MAX}")
        )
        .await,
        vec!["max".to_string()],
        "{collection}: WHERE equality finds u64::MAX"
    );
    assert_eq!(
        column(
            srv,
            &format!("SELECT id FROM {collection} WHERE v = {JUST_ABOVE_I64}")
        )
        .await,
        vec!["mid".to_string()],
        "{collection}: WHERE equality tells i64::MAX + 1 from u64::MAX"
    );
    assert_eq!(
        column(srv, &format!("SELECT id FROM {collection} ORDER BY v")).await,
        vec!["small", "mid", "max"],
        "{collection}: ORDER BY v ascending"
    );
    assert_eq!(
        column(srv, &format!("SELECT id FROM {collection} ORDER BY v DESC")).await,
        vec!["max", "mid", "small"],
        "{collection}: ORDER BY v descending"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_document_keeps_u64_max() {
    let srv = TestServer::start().await;
    seed(
        &srv,
        "CREATE COLLECTION u64_doc WITH (engine='document_schemaless')",
        "u64_doc",
    )
    .await;
    check(&srv, "u64_doc").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_decimal_column_keeps_u64_max() {
    let srv = TestServer::start().await;
    seed(
        &srv,
        "CREATE COLLECTION u64_strict (id TEXT PRIMARY KEY, v DECIMAL) \
         WITH (engine='document_strict')",
        "u64_strict",
    )
    .await;
    check(&srv, "u64_strict").await;
}

/// `i64::MIN` written as a negated literal is the integer `i64::MIN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negated_literal_reaches_i64_min() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION i64_min_doc WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("INSERT INTO i64_min_doc (id, v) VALUES ('min', -9223372036854775808)")
        .await
        .unwrap();
    assert_eq!(
        column(
            &srv,
            "SELECT v FROM i64_min_doc WHERE v = -9223372036854775808"
        )
        .await,
        vec!["-9223372036854775808".to_string()]
    );
}

/// A literal past the 96-bit exact range is an error, never a rounded float.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_past_exact_range_is_refused() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION u64_huge WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.expect_error(
        "INSERT INTO u64_huge (id, v) VALUES ('x', 79228162514264337593543950336)",
        "out of range",
    )
    .await;
}
