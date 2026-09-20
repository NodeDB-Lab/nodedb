// SPDX-License-Identifier: BUSL-1.1

//! KV predicate DML (`UPDATE ... WHERE <non-key predicate>` / `DELETE FROM
//! ... WHERE <non-key predicate>`, compiling to `KvOp::PredicateUpdate` /
//! `PredicateDelete`) executes at STATEMENT time inside a transaction by
//! staging its matched rows into the per-transaction overlay: the statement
//! answers a real `UPDATE n` / `DELETE n` tag, the transaction's own reads
//! observe the change before COMMIT, and ROLLBACK / ROLLBACK TO SAVEPOINT
//! drop it. COMMIT's buffered plan replay remains the sole durable apply.
//!
//! Every predicate here is on the NON-KEY column `n`, so the plan compiles
//! to the predicate form rather than the keyed point-write path.

use crate::harness::TestServer;
use crate::harness::raw_pgwire::{RawPgConn, command_tags};

/// `CommandComplete` tags `sql` answers with on a fresh trust-mode
/// connection, in wire order. A multi-statement `sql` keeps its transaction
/// on that one connection.
async fn raw_command_tags(port: u16, sql: &str) -> Vec<String> {
    let mut conn = RawPgConn::connect(port, "nodedb", "default").await;
    let messages = conn.simple_query(sql).await;
    command_tags(&messages)
}

fn tags(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// The row count the harness connection's command tag reports for `sql`.
async fn count(server: &TestServer, sql: &str) -> Option<u64> {
    server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"))
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
}

/// Sorted keys `sql` returns on the harness connection (which observes the
/// transaction's staging overlay while one is open).
async fn keys(server: &TestServer, sql: &str) -> Vec<String> {
    let mut v = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"));
    v.sort();
    v
}

/// Sorted first-column strings `sql` returns on `client`.
async fn keys_on(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let mut v: Vec<String> = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

/// Sorted `(key, n)` pairs of the whole collection, as strings.
async fn rows(server: &TestServer, coll: &str) -> Vec<Vec<String>> {
    let mut v = server
        .query_rows(&format!("SELECT key, n FROM {coll}"))
        .await
        .unwrap_or_else(|e| panic!("read {coll}: {e}"));
    v.sort();
    v
}

fn pairs(list: &[(&str, &str)]) -> Vec<Vec<String>> {
    list.iter()
        .map(|(k, n)| vec![k.to_string(), n.to_string()])
        .collect()
}

/// A KV collection holding `a`/`b` at `n = 1`, `c` at `n = 2`, and
/// `unrelated` at `n = 100`.
async fn setup(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .unwrap();
    for (key, n) in [("a", 1), ("b", 1), ("c", 2), ("unrelated", 100)] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (key, n) VALUES ('{key}', {n})"
            ))
            .await
            .unwrap();
    }
}

/// A predicate UPDATE inside a transaction stages its matched rows: the
/// statement reports the real `UPDATE n` tag, and the transaction's own scan
/// observes the new value before COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_update_in_transaction_reports_count_and_is_visible_before_commit() {
    let server = TestServer::start().await;
    setup(&server, "kvp_upd").await;

    let got = raw_command_tags(
        server.pg_port,
        "BEGIN; UPDATE kvp_upd SET n = 7 WHERE n = 1; ROLLBACK",
    )
    .await;
    assert_eq!(
        got,
        tags(&["BEGIN", "UPDATE 2", "ROLLBACK"]),
        "a staged predicate UPDATE reports its matched-row count"
    );

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        count(&server, "UPDATE kvp_upd SET n = 7 WHERE n = 1").await,
        Some(2)
    );
    assert_eq!(
        keys(&server, "SELECT key FROM kvp_upd WHERE n = 7").await,
        tags(&["a", "b"]),
        "the transaction's own scan observes the staged update"
    );
    assert_eq!(
        keys(&server, "SELECT key FROM kvp_upd WHERE n = 1").await,
        Vec::<String>::new(),
        "rows updated away from n = 1 no longer match it"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(&server, "kvp_upd").await,
        pairs(&[("a", "7"), ("b", "7"), ("c", "2"), ("unrelated", "100")]),
        "COMMIT applies the staged update"
    );
}

/// A predicate DELETE inside a transaction stages tombstones: the statement
/// reports the real `DELETE n` tag, the transaction's own scan hides the
/// rows, and COMMIT removes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_delete_in_transaction_hides_rows_and_commit_applies() {
    let server = TestServer::start().await;
    setup(&server, "kvp_del").await;

    let got = raw_command_tags(
        server.pg_port,
        "BEGIN; DELETE FROM kvp_del WHERE n = 1; ROLLBACK",
    )
    .await;
    assert_eq!(
        got,
        tags(&["BEGIN", "DELETE 2", "ROLLBACK"]),
        "a staged predicate DELETE reports its matched-row count"
    );

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        count(&server, "DELETE FROM kvp_del WHERE n = 1").await,
        Some(2)
    );
    assert_eq!(
        keys(&server, "SELECT key FROM kvp_del").await,
        tags(&["c", "unrelated"]),
        "the transaction's own scan hides the staged delete"
    );
    assert_eq!(
        keys(&server, "SELECT key FROM kvp_del WHERE key = 'a'").await,
        Vec::<String>::new(),
        "a point read of a staged-deleted key returns no row"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(&server, "kvp_del").await,
        pairs(&[("c", "2"), ("unrelated", "100")]),
        "COMMIT applies the staged delete"
    );
}

/// A predicate that matches nothing answers `UPDATE 0` / `DELETE 0` and
/// stages nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_dml_matching_nothing_reports_zero() {
    let server = TestServer::start().await;
    setup(&server, "kvp_zero").await;

    let got = raw_command_tags(
        server.pg_port,
        "BEGIN; UPDATE kvp_zero SET n = 7 WHERE n = 12345; \
         DELETE FROM kvp_zero WHERE n = 12345; COMMIT",
    )
    .await;
    assert_eq!(got, tags(&["BEGIN", "UPDATE 0", "DELETE 0", "COMMIT"]));
    assert_eq!(
        rows(&server, "kvp_zero").await,
        pairs(&[("a", "1"), ("b", "1"), ("c", "2"), ("unrelated", "100")]),
        "a no-match predicate DML changes nothing"
    );
}

/// A row inserted earlier in the same transaction is part of the predicate's
/// candidate set, and a row updated earlier is matched by its staged value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_dml_sees_row_staged_earlier_in_transaction() {
    let server = TestServer::start().await;
    setup(&server, "kvp_own").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO kvp_own (key, n) VALUES ('d', 1)")
        .await
        .unwrap();
    server
        .exec("UPDATE kvp_own SET n = 1 WHERE key = 'c'")
        .await
        .unwrap();

    // `a`, `b` (base), `c` (staged update), `d` (staged insert) all match.
    assert_eq!(
        count(&server, "UPDATE kvp_own SET n = 9 WHERE n = 1").await,
        Some(4),
        "the predicate matches base rows and rows staged earlier in the transaction"
    );
    assert_eq!(
        keys(&server, "SELECT key FROM kvp_own WHERE n = 9").await,
        tags(&["a", "b", "c", "d"])
    );

    assert_eq!(
        count(&server, "DELETE FROM kvp_own WHERE n = 9").await,
        Some(4),
        "a predicate DELETE matches the rows the earlier staged UPDATE produced"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(&server, "kvp_own").await,
        pairs(&[("unrelated", "100")]),
        "COMMIT applies the whole chain: insert, keyed update, predicate update, predicate delete"
    );
}

/// ROLLBACK discards staged predicate writes: the original rows come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_dml_rollback_restores_rows() {
    let server = TestServer::start().await;
    setup(&server, "kvp_rb").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE kvp_rb SET n = 7 WHERE n = 1")
        .await
        .unwrap();
    server.exec("DELETE FROM kvp_rb WHERE n = 2").await.unwrap();
    assert_eq!(
        rows(&server, "kvp_rb").await,
        pairs(&[("a", "7"), ("b", "7"), ("unrelated", "100")]),
        "both staged writes are visible before ROLLBACK"
    );
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        rows(&server, "kvp_rb").await,
        pairs(&[("a", "1"), ("b", "1"), ("c", "2"), ("unrelated", "100")]),
        "ROLLBACK restores every row the predicate writes touched"
    );
}

/// ROLLBACK TO SAVEPOINT drops only the predicate write staged after the
/// savepoint; a write staged before it survives and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_dml_rollback_to_savepoint_undoes_only_the_predicate_write() {
    let server = TestServer::start().await;
    setup(&server, "kvp_sp").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO kvp_sp (key, n) VALUES ('p', 50)")
        .await
        .unwrap();
    server.exec("SAVEPOINT s1").await.unwrap();
    server
        .exec("UPDATE kvp_sp SET n = 7 WHERE n = 1")
        .await
        .unwrap();
    server.exec("DELETE FROM kvp_sp WHERE n = 2").await.unwrap();
    assert_eq!(
        rows(&server, "kvp_sp").await,
        pairs(&[("a", "7"), ("b", "7"), ("p", "50"), ("unrelated", "100")]),
        "the predicate writes are visible before ROLLBACK TO"
    );

    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    assert_eq!(
        rows(&server, "kvp_sp").await,
        pairs(&[
            ("a", "1"),
            ("b", "1"),
            ("c", "2"),
            ("p", "50"),
            ("unrelated", "100")
        ]),
        "ROLLBACK TO drops the post-savepoint predicate writes and keeps the pre-savepoint insert"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(&server, "kvp_sp").await,
        pairs(&[
            ("a", "1"),
            ("b", "1"),
            ("c", "2"),
            ("p", "50"),
            ("unrelated", "100")
        ]),
        "COMMIT persists only what survived the savepoint rollback"
    );
}

/// A staged predicate write is private to its transaction: another
/// connection reads the base rows until COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_dml_is_invisible_to_another_connection_until_commit() {
    let server = TestServer::start().await;
    setup(&server, "kvp_iso").await;
    let (other, _handle) = server.connect_as("nodedb", "nodedb").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE kvp_iso SET n = 7 WHERE n = 1")
        .await
        .unwrap();
    server
        .exec("DELETE FROM kvp_iso WHERE n = 2")
        .await
        .unwrap();

    assert_eq!(
        keys_on(&other, "SELECT key FROM kvp_iso WHERE n = 1").await,
        tags(&["a", "b"]),
        "another connection still reads the base value of the staged update"
    );
    assert_eq!(
        keys_on(&other, "SELECT key FROM kvp_iso WHERE n = 2").await,
        tags(&["c"]),
        "another connection still reads the staged-deleted row"
    );

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        keys_on(&other, "SELECT key FROM kvp_iso WHERE n = 7").await,
        tags(&["a", "b"]),
        "after COMMIT the other connection reads the update"
    );
    assert_eq!(
        keys_on(&other, "SELECT key FROM kvp_iso WHERE n = 2").await,
        Vec::<String>::new(),
        "after COMMIT the other connection no longer reads the deleted row"
    );
}

/// A staged TRUNCATE hides every base row from a later predicate write in
/// the same transaction: the UPDATE matches nothing, and COMMIT leaves the
/// collection empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_update_after_truncate_in_transaction_matches_nothing() {
    let server = TestServer::start().await;
    setup(&server, "kvp_trunc").await;

    let got = raw_command_tags(
        server.pg_port,
        "BEGIN; TRUNCATE kvp_trunc; UPDATE kvp_trunc SET n = 7 WHERE n = 1; \
         DELETE FROM kvp_trunc WHERE n = 100; COMMIT",
    )
    .await;
    assert_eq!(
        got,
        tags(&["BEGIN", "TRUNCATE", "UPDATE 0", "DELETE 0", "COMMIT"]),
        "predicate DML after a staged TRUNCATE matches no base row"
    );
    assert_eq!(
        rows(&server, "kvp_trunc").await,
        Vec::<Vec<String>>::new(),
        "COMMIT replays the truncate and the no-op predicate writes"
    );
}
