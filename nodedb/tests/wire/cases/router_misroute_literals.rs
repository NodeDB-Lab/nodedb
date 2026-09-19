// SPDX-License-Identifier: BUSL-1.1

//! Prefix anchoring for the router's function arms.
//!
//! `TOPK`, `WEIGHTED_PICK` and `NDB_CHUNK_TEXT` are recognized only at the
//! statement prefix. A statement carrying one of those tokens anywhere else —
//! a doc-object UPSERT value, a string literal, a comment — belongs to its own
//! handler and must never reach the function arm. These tests hold both
//! directions: the literal stores verbatim, the anchored form still routes.
//!
//! The query-function family (`VERIFY_BALANCE`, `VERIFY_AUDIT_CHAIN`,
//! `VERIFY_HASH_CHAIN`, `BALANCE_AS_OF`, `TEMPORAL_LOOKUP`,
//! `CONVERT_CURRENCY_LOOKUP`) is matched by `contains` over the statement, so
//! the same rule applies to a token inside a literal there.

use crate::harness::TestServer;

/// A doc-object value carrying `merge_from_topk()` must store verbatim, and
/// `SELECT * FROM TOPK(...)` must still route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_literal_does_not_misroute_topk() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION lit_c (id STRING PRIMARY KEY, name STRING, score INT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO lit_c { id: 'a', name: 'plain', score: 10 }")
        .await
        .expect("seed a");
    server
        .exec("CREATE SORTED INDEX lit_idx ON lit_c (score DESC) KEY id")
        .await
        .expect("create sorted index");

    // The value carries the token, the statement is an UPSERT: it must store
    // the value verbatim rather than reach the sorted-index handler.
    server
        .exec("UPSERT INTO lit_c { id: 'b', name: 'merge_from_topk()', score: 20 }")
        .await
        .expect("UPSERT with TOPK-shaped literal must store verbatim, not misroute");

    let rows = server
        .query_text("SELECT name FROM lit_c WHERE id = 'b'")
        .await
        .expect("read back the upserted row");
    assert_eq!(
        rows,
        vec!["merge_from_topk()".to_string()],
        "the literal must be stored verbatim"
    );

    // The anchored form still routes and returns rows.
    let top = server
        .query_text("SELECT * FROM TOPK(lit_idx, 3)")
        .await
        .expect("anchored TOPK must still route");
    assert_eq!(top.len(), 2, "both seeded rows must be returned: {top:?}");
}

/// A doc-object value carrying `weighted_pick()` must store verbatim — the
/// token in call order, so a `contains("WEIGHTED_PICK(")` predicate would
/// match it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_literal_weighted_pick_stores_verbatim() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION lit_c2 (id STRING PRIMARY KEY, name STRING, weight INT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO lit_c2 { id: 'a', name: 'plain', weight: 1 }")
        .await
        .expect("seed a");

    // The token in call order inside an INSERT doc-object value.
    server
        .exec("INSERT INTO lit_c2 { id: 'b', name: 'weighted_pick()', weight: 2 }")
        .await
        .expect("INSERT with WEIGHTED_PICK-shaped literal must store verbatim");

    let rows = server
        .query_text("SELECT name FROM lit_c2 WHERE id = 'b'")
        .await
        .expect("read back");
    assert_eq!(
        rows,
        vec!["weighted_pick()".to_string()],
        "the literal must be stored verbatim"
    );
}

/// A SELECT whose WHERE literal carries `ndb_chunk_text(x)` must return the
/// matching row, not route into the chunk-text handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_literal_does_not_misroute_ndb_chunk_text() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION lit_c3 (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='kv')",
        )
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO lit_c3 { id: 'a', name: 'plain' }")
        .await
        .expect("seed a");
    server
        .exec("INSERT INTO lit_c3 { id: 'b', name: 'ndb_chunk_text(x)' }")
        .await
        .expect("seed b");

    // The token in a WHERE literal must return the matching row, not route into
    // the chunk-text handler.
    let rows = server
        .query_text("SELECT name FROM lit_c3 WHERE name = 'ndb_chunk_text(x)'")
        .await
        .expect("SELECT with a chunk-text-shaped literal must not misroute");
    assert_eq!(
        rows,
        vec!["ndb_chunk_text(x)".to_string()],
        "the matching row must be returned, not chunk-text output"
    );
}

/// Leading whitespace must not break the prefix anchors — the router trims
/// before uppercasing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leading_whitespace_still_routes_anchored_arms() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ws_c (id STRING PRIMARY KEY, score INT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO ws_c { id: 'a', score: 10 }")
        .await
        .expect("seed a");
    server
        .exec("CREATE SORTED INDEX ws_idx ON ws_c (score DESC) KEY id")
        .await
        .expect("create sorted index");

    let top = server
        .query_text("\n  SELECT * FROM TOPK(ws_idx, 3)")
        .await
        .expect("leading whitespace must not break the TOPK anchor");
    assert_eq!(
        top.len(),
        1,
        "row must be returned despite leading whitespace"
    );
}

/// The six query-function keywords are matched by `contains`, not by a prefix
/// anchor, so a value carrying one used to reach the function arm and answer
/// with its argument error. The literal must store verbatim, and the anchored
/// call must still reach the function arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn value_carrying_verify_balance_does_not_misroute() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION lit_qf (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='kv')",
        )
        .await
        .expect("create collection");

    // The value carries a query-function token. The parenthesised form is a
    // non-DDL parse, so it reaches the neutral dispatcher where the pre-fix
    // substring match hijacked it; the brace form never gets there.
    server
        .exec("INSERT INTO lit_qf (id, name) VALUES ('a', 'verify_balance')")
        .await
        .expect("INSERT with a query-function token in a value must not misroute");

    let rows = server
        .query_text("SELECT name FROM lit_qf WHERE id = 'a'")
        .await
        .expect("read back the inserted row");
    assert_eq!(
        rows,
        vec!["verify_balance".to_string()],
        "the value must be stored verbatim"
    );

    // The anchored form still routes: the function arm reports the collection
    // it was asked for, so a missing collection names it.
    let err = server
        .query_text("SELECT VERIFY_BALANCE('lit_qf_missing', 'name')")
        .await
        .expect_err("the anchored call must reach the function arm");
    assert!(
        err.contains("lit_qf_missing"),
        "the anchored call must reach the function arm, got: {err}"
    );
}
