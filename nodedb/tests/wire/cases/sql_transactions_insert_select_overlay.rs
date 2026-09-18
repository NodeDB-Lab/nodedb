// SPDX-License-Identifier: BUSL-1.1

//! `INSERT INTO <target> SELECT ... FROM <source> WHERE <predicate>`
//! (compiling to `DocumentOp::InsertSelect`) must execute at STATEMENT time
//! inside a transaction by staging the copied rows into the per-transaction
//! overlay: the statement returns a real `INSERT 0 n` tag (not `OK`), and the
//! transaction's own later reads observe the copied rows before COMMIT.
//! COMMIT's buffered plan replay remains the sole durable apply.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a simple-query
/// response (PostgreSQL's `INSERT 0 N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

async fn scan_ints(server: &TestServer, sql: &str) -> Vec<i64> {
    let mut v: Vec<i64> = server
        .query_text(sql)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    v.sort_unstable();
    v
}

async fn setup(server: &TestServer, src: &str, tgt: &str, engine: &str) {
    for coll in [src, tgt] {
        server
            .exec(&format!(
                "CREATE COLLECTION {coll} \
                 (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='{engine}')"
            ))
            .await
            .unwrap();
    }
    for (id, n) in [("a", 1), ("b", 1), ("c", 2), ("unrelated", 100)] {
        server
            .exec(&format!("INSERT INTO {src} (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }
}

/// An `INSERT ... SELECT ... WHERE` inside a transaction stages the copied
/// rows: the statement reports the real `INSERT 0 n` tag, and the
/// transaction's own scan of the target observes the copied rows before
/// COMMIT.
async fn insert_select_stages_and_is_visible(engine: &str, src: &str, tgt: &str) {
    let server = TestServer::start().await;
    setup(&server, src, tgt, engine).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(&format!(
            "INSERT INTO {tgt} SELECT * FROM {src} WHERE n = 1"
        ))
        .await
        .expect("in-tx insert-select should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "{engine}: INSERT ... SELECT must report the real copied-row count, not OK"
    );

    // In-tx visibility: the target must show exactly the copied rows.
    let seen = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert_eq!(
        seen,
        vec![1, 1],
        "{engine}: in-tx scan of target must observe the staged copy"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let after = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert_eq!(
        after,
        vec![1, 1],
        "{engine}: committed insert-select must persist"
    );
}

/// ROLLBACK after a staged `INSERT ... SELECT` must discard the copy: a
/// SELECT on the target after ROLLBACK must be empty.
async fn insert_select_rollback_discards_staged_copy(engine: &str, src: &str, tgt: &str) {
    let server = TestServer::start().await;
    setup(&server, src, tgt, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {tgt} SELECT * FROM {src} WHERE n = 1"
        ))
        .await
        .unwrap();

    let staged = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert_eq!(
        staged,
        vec![1, 1],
        "{engine}: staged copy must be visible pre-rollback"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert!(
        after.is_empty(),
        "{engine}: ROLLBACK must discard the staged copy, got {after:?}"
    );
}

/// A `LIMIT` on the source SELECT must be respected when staging: only the
/// first `n` matched rows are copied.
async fn insert_select_respects_limit(engine: &str, src: &str, tgt: &str) {
    let server = TestServer::start().await;
    setup(&server, src, tgt, engine).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query(&format!(
            "INSERT INTO {tgt} SELECT * FROM {src} WHERE n = 1 LIMIT 1"
        ))
        .await
        .expect("in-tx insert-select with LIMIT should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "{engine}: LIMIT must cap the staged copied-row count"
    );

    let seen = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert_eq!(
        seen,
        vec![1],
        "{engine}: in-tx scan of target must show only the LIMIT-ed copy"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// A source row staged earlier in the SAME transaction (via a plain point
/// INSERT) must also be picked up by a later `INSERT ... SELECT` from that
/// source — proving the source-side scan is resolved against BASE ∪ OVERLAY,
/// not BASE alone.
async fn insert_select_sees_source_rows_staged_earlier_in_txn(engine: &str, src: &str, tgt: &str) {
    let server = TestServer::start().await;
    setup(&server, src, tgt, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {src} (id, n) VALUES ('fresh', 1)"))
        .await
        .unwrap();

    let msgs = server
        .client
        .simple_query(&format!(
            "INSERT INTO {tgt} SELECT * FROM {src} WHERE n = 1"
        ))
        .await
        .expect("in-tx insert-select should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(3),
        "{engine}: copy must include the source row staged earlier in this txn"
    );

    let seen = scan_ints(&server, &format!("SELECT n FROM {tgt}")).await;
    assert_eq!(
        seen,
        vec![1, 1, 1],
        "{engine}: target must contain the copy of the in-txn-staged source row"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_select_stages_and_is_visible() {
    insert_select_stages_and_is_visible("document_schemaless", "is_sc_src", "is_sc_tgt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_select_rollback_discards_staged_copy() {
    insert_select_rollback_discards_staged_copy(
        "document_schemaless",
        "is_sc_rb_src",
        "is_sc_rb_tgt",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_select_respects_limit() {
    insert_select_respects_limit("document_schemaless", "is_sc_lim_src", "is_sc_lim_tgt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_select_sees_source_rows_staged_earlier_in_txn() {
    insert_select_sees_source_rows_staged_earlier_in_txn(
        "document_schemaless",
        "is_sc_ov_src",
        "is_sc_ov_tgt",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_select_stages_and_is_visible() {
    insert_select_stages_and_is_visible("document_strict", "is_st_src", "is_st_tgt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_select_rollback_discards_staged_copy() {
    insert_select_rollback_discards_staged_copy("document_strict", "is_st_rb_src", "is_st_rb_tgt")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_select_respects_limit() {
    insert_select_respects_limit("document_strict", "is_st_lim_src", "is_st_lim_tgt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_select_sees_source_rows_staged_earlier_in_txn() {
    insert_select_sees_source_rows_staged_earlier_in_txn(
        "document_strict",
        "is_st_ov_src",
        "is_st_ov_tgt",
    )
    .await;
}

/// In-transaction `INSERT ... SELECT` stages through the per-transaction
/// overlay, so the source read is transactional. The kv materialize-scan
/// carries no snapshot fields; the read is refused by name rather than copying
/// committed-only rows the transaction did not ask for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_source_refuses_an_in_transaction_read() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION is_kv_tx_src (id STRING PRIMARY KEY, n INT) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO is_kv_tx_src (id, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION is_kv_tx_tgt").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .expect_error(
            "INSERT INTO is_kv_tx_tgt SELECT * FROM is_kv_tx_src",
            "transactional",
        )
        .await;
    server.exec("ROLLBACK").await.unwrap();
}
