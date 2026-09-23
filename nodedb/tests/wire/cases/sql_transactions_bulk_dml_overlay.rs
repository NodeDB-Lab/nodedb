// SPDX-License-Identifier: BUSL-1.1

//! Predicate DML (`UPDATE ... WHERE <predicate>` / `DELETE ... WHERE
//! <predicate>`, compiling to `DocumentOp::BulkUpdate` / `BulkDelete`) must
//! execute at STATEMENT time inside a transaction by staging the matched rows
//! into the per-transaction overlay: the statement returns a real `UPDATE n`
//! / `DELETE n` tag (not `OK`), and the transaction's own later reads (scan
//! and index lookup) observe the change before COMMIT. COMMIT's buffered plan
//! replay remains the sole durable apply.
//!
//! Every predicate here is on the NON-PK column `n`, so the plan compiles to
//! `BulkUpdate` / `BulkDelete` rather than the point-write path.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a simple-query
/// response (PostgreSQL's `UPDATE N` / `DELETE N` count).
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

async fn setup(server: &TestServer, coll: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 1), ("c", 2), ("unrelated", 100)] {
        server
            .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }
}

/// A predicate UPDATE inside a transaction stages its matched rows: the
/// statement reports the real `UPDATE n` tag, and the transaction's own scan
/// observes the new value before COMMIT.
async fn bulk_update_stages_and_is_visible(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(&format!("UPDATE {coll} SET n = 7 WHERE n = 1"))
        .await
        .expect("in-tx bulk update should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "{engine}: predicate UPDATE must report the real matched-row count, not OK"
    );

    // The transaction's own scan must see the staged value, not the old one.
    let seen = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 7")).await;
    assert_eq!(
        seen,
        vec![7, 7],
        "{engine}: in-tx scan must observe the staged bulk update"
    );
    let old = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 1")).await;
    assert!(
        old.is_empty(),
        "{engine}: rows updated away from n=1 must no longer match n=1, got {old:?}"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // A fresh statement after COMMIT must see the persisted change.
    let after = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 7")).await;
    assert_eq!(
        after,
        vec![7, 7],
        "{engine}: committed bulk update must persist"
    );
}

/// A predicate DELETE inside a transaction stages tombstones for its matched
/// rows: the statement reports the real `DELETE n` tag, and the transaction's
/// own scan no longer returns the deleted rows before COMMIT.
async fn bulk_delete_stages_and_is_visible(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE n = 1"))
        .await
        .expect("in-tx bulk delete should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "{engine}: predicate DELETE must report the real matched-row count, not OK"
    );

    let remaining = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        remaining,
        vec![2, 100],
        "{engine}: in-tx scan must hide the staged bulk delete"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let after = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        after,
        vec![2, 100],
        "{engine}: committed bulk delete must persist"
    );
}

/// ROLLBACK after a staged bulk update/delete must discard the changes: a
/// SELECT after ROLLBACK must show the original data.
async fn bulk_dml_rollback_discards_staged_changes(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("UPDATE {coll} SET n = 7 WHERE n = 1"))
        .await
        .unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE n = 2"))
        .await
        .unwrap();

    let staged = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        staged,
        vec![7, 7, 100],
        "{engine}: staged changes must be visible pre-rollback"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        after,
        vec![1, 1, 2, 100],
        "{engine}: ROLLBACK must restore the original rows, got {after:?}"
    );
}

/// A staged bulk update's changed value must be visible via BOTH a table scan
/// AND an equality lookup on an indexed column, in the same transaction,
/// before COMMIT.
async fn bulk_update_visible_via_index_lookup(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;
    server
        .exec(&format!("CREATE INDEX ON {coll}(n)"))
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("UPDATE {coll} SET n = 42 WHERE n = 1"))
        .await
        .unwrap();

    // Scan visibility.
    let via_scan = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 42")).await;
    assert_eq!(
        via_scan,
        vec![42, 42],
        "{engine}: table scan must see the staged bulk update"
    );

    // Index-lookup visibility on the SAME indexed column.
    let via_index = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 42")).await;
    assert_eq!(
        via_index,
        vec![42, 42],
        "{engine}: indexed equality lookup must see the staged bulk update"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// A staged bulk update's changed value must also be visible via a primary-key
/// point lookup (`WHERE id = <pk>`) in the same transaction — the point-get path
/// resolves the row by its surrogate, so a bulk-touched row reads its staged
/// body, not the stale base body.
async fn bulk_update_visible_via_point_get_by_pk(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("UPDATE {coll} SET n = 7 WHERE n = 1"))
        .await
        .unwrap();

    // 'a' matched the predicate (base n = 1). A PK point-get must see n = 7.
    let a = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE id = 'a'")).await;
    assert_eq!(
        a,
        vec![7],
        "{engine}: PK point-get must see the staged bulk update"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE id = 'a'")).await;
    assert_eq!(after, vec![1], "{engine}: ROLLBACK restores the base row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_update_visible_via_point_get_by_pk() {
    bulk_update_visible_via_point_get_by_pk("document_schemaless", "bu_sc_pk").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_update_visible_via_point_get_by_pk() {
    bulk_update_visible_via_point_get_by_pk("document_strict", "bu_st_pk").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_update_stages_and_is_visible() {
    bulk_update_stages_and_is_visible("document_schemaless", "bu_sc_upd").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_delete_stages_and_is_visible() {
    bulk_delete_stages_and_is_visible("document_schemaless", "bu_sc_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_dml_rollback_discards_staged_changes() {
    bulk_dml_rollback_discards_staged_changes("document_schemaless", "bu_sc_rb").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_update_visible_via_index_lookup() {
    bulk_update_visible_via_index_lookup("document_schemaless", "bu_sc_idx").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_update_stages_and_is_visible() {
    bulk_update_stages_and_is_visible("document_strict", "bu_st_upd").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_delete_stages_and_is_visible() {
    bulk_delete_stages_and_is_visible("document_strict", "bu_st_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_dml_rollback_discards_staged_changes() {
    bulk_dml_rollback_discards_staged_changes("document_strict", "bu_st_rb").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_update_visible_via_index_lookup() {
    bulk_update_visible_via_index_lookup("document_strict", "bu_st_idx").await;
}

/// A predicate UPDATE's BASE ∪ OVERLAY resolution must fold in a row inserted
/// earlier in the SAME transaction by a plain point `INSERT`, not just rows
/// already durable at BEGIN. Strict (Binary Tuple) collections store the
/// staged row as a Binary Tuple; the predicate must be decoded against the
/// collection's schema before evaluation, or the staged-earlier row is
/// silently dropped from the matched set (it never fails, it just isn't
/// counted or updated).
async fn strict_bulk_update_sees_row_staged_earlier_in_txn(coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, "document_strict").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('fresh', 1)"))
        .await
        .unwrap();

    let msgs = server
        .client
        .simple_query(&format!("UPDATE {coll} SET n = 9 WHERE n = 1"))
        .await
        .expect("in-tx bulk update should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(3),
        "strict: predicate UPDATE must include the row staged earlier \
         in this txn by a point INSERT (base 'a','b' + staged 'fresh')"
    );

    let seen = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 9")).await;
    assert_eq!(
        seen,
        vec![9, 9, 9],
        "strict: in-tx scan must show the staged-earlier row updated too"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// Same latent bug, exercised via predicate `DELETE`: a row inserted earlier
/// in the transaction must be tombstoned by a later matching bulk delete.
async fn strict_bulk_delete_sees_row_staged_earlier_in_txn(coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, "document_strict").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('fresh', 1)"))
        .await
        .unwrap();

    let msgs = server
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE n = 1"))
        .await
        .expect("in-tx bulk delete should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(3),
        "strict: predicate DELETE must include the row staged earlier \
         in this txn by a point INSERT (base 'a','b' + staged 'fresh')"
    );

    let remaining = scan_ints(&server, &format!("SELECT n FROM {coll}")).await;
    assert_eq!(
        remaining,
        vec![2, 100],
        "strict: in-tx scan must hide both base and staged-earlier deleted rows"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_update_sees_row_staged_earlier_in_txn_case() {
    strict_bulk_update_sees_row_staged_earlier_in_txn("bu_st_ov_upd").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_delete_sees_row_staged_earlier_in_txn_case() {
    strict_bulk_delete_sees_row_staged_earlier_in_txn("bu_st_ov_del").await;
}

/// A predicate `UPDATE ... RETURNING` inside a transaction is REFUSED, naming
/// the limitation.
///
/// Succeeding with the real `UPDATE n` tag and zero data rows would give a
/// caller that asked for rows silence, precisely the failure this
/// clause exists to remove. The write is staged into the per-transaction
/// overlay, and staging answers with a count rather than a row image, while
/// COMMIT answers with one tag for the whole transaction; so there is no point
/// at which the rows could be surfaced. Refusing says so instead of pretending
/// the statement matched nothing.
///
/// The refusal is verb-agnostic — it fires for any row-returning plan — so the
/// non-RETURNING form of the same statement is asserted alongside to pin that
/// staging itself is untouched.
async fn bulk_update_returning_in_txn_is_refused(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();

    let error = server
        .client
        .simple_query(&format!(
            "UPDATE {coll} SET n = 7 WHERE n = 1 RETURNING id, n"
        ))
        .await
        .expect_err("in-tx UPDATE ... RETURNING must be refused, not answered with no rows");
    let message = error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string());
    assert!(
        message.contains("RETURNING") && message.contains("transaction"),
        "{engine}: the refusal must name the clause and the limitation, got: {message}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    // The same statement without the clause still stages and still reports its
    // real matched-row count: the refusal is about the projection, not about
    // predicate DML in a transaction.
    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query(&format!("UPDATE {coll} SET n = 7 WHERE n = 1"))
        .await
        .expect("in-tx bulk update should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "{engine}: in-tx UPDATE must report the real matched-row count, not OK"
    );

    let seen = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 7")).await;
    assert_eq!(
        seen,
        vec![7, 7],
        "{engine}: in-tx scan must observe the staged update"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after = scan_ints(&server, &format!("SELECT n FROM {coll} WHERE n = 7")).await;
    assert_eq!(after, vec![7, 7], "{engine}: committed update must persist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_returning_in_txn_is_refused_case() {
    bulk_update_returning_in_txn_is_refused("document_schemaless", "bu_ret_ov_upd").await;
}

/// Sorted `id=n` pairs of the whole collection in the session's database.
async fn id_n_pairs(server: &TestServer, coll: &str) -> Vec<String> {
    let mut v: Vec<String> = server
        .query_rows(&format!("SELECT id, n FROM {coll}"))
        .await
        .unwrap_or_else(|e| panic!("read {coll}: {e}"))
        .into_iter()
        .map(|row| row.join("="))
        .collect();
    v.sort();
    v
}

/// Predicate DML committed in a named database applies to that database's
/// collection. The same-named collection in `default` keeps its rows.
async fn bulk_dml_commit_applies_in_session_database(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;
    server.exec("CREATE DATABASE bulk_dml_named").await.unwrap();
    server.exec("USE DATABASE bulk_dml_named").await.unwrap();
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    for sql in [
        format!("UPDATE {coll} SET n = 7 WHERE n = 1"),
        format!("DELETE FROM {coll} WHERE n = 2"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        id_n_pairs(&server, coll).await,
        vec!["a=7", "b=7", "unrelated=100"],
        "{engine}: COMMIT applies the predicate DML in the session's database"
    );
    server.exec("USE DATABASE default").await.unwrap();
    assert_eq!(
        id_n_pairs(&server, coll).await,
        vec!["a=1", "b=1", "c=2", "unrelated=100"],
        "{engine}: a transaction in another database leaves `default` untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bulk_dml_commit_applies_in_session_database() {
    bulk_dml_commit_applies_in_session_database("document_schemaless", "bu_db_s").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bulk_dml_commit_applies_in_session_database() {
    bulk_dml_commit_applies_in_session_database("document_strict", "bu_db_t").await;
}
