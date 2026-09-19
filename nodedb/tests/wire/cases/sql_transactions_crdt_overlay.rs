// SPDX-License-Identifier: BUSL-1.1

//! In-transaction DML on a `crdt='true'` collection is staged at statement
//! time: it answers the real command tag of its SQL verb, and the same
//! transaction's point reads and scans see the write (read-your-own-writes).
//! Sibling `sql_transactions_crdt_overlay_lifecycle` covers COMMIT,
//! ROLLBACK, and `ROLLBACK TO SAVEPOINT`.
//!
//! `tokio_postgres` surfaces only the count of a command tag. The tag word
//! (`INSERT` / `UPSERT` / `UPDATE`) is read off a raw pgwire connection.

use crate::harness::TestServer;
use crate::harness::raw_pgwire::{RawPgConn, command_tags};
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in `sql`'s
/// response.
pub(super) async fn affected(server: &TestServer, sql: &str) -> Option<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

pub(super) async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
             WITH (crdt='true')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

pub(super) fn insert_sql(name: &str, id: &str, title: &str, body: &str) -> String {
    format!("INSERT INTO {name} (id, title, body) VALUES ('{id}', '{title}', '{body}')")
}

pub(super) async fn insert(server: &TestServer, name: &str, id: &str, title: &str, body: &str) {
    server
        .exec(&insert_sql(name, id, title, body))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {name}: {e}"));
}

/// `(title, body)` of the point read `WHERE id = '<id>'` on the transaction's
/// own connection.
pub(super) async fn point(server: &TestServer, name: &str, id: &str) -> Vec<(String, String)> {
    server
        .query_rows(&format!("SELECT title, body FROM {name} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("point read {id} from {name}: {e}"))
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect()
}

/// Ids of every row a full scan returns, sorted.
pub(super) async fn scan_ids(server: &TestServer, name: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("scan {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

pub(super) fn pair(title: &str, body: &str) -> Vec<(String, String)> {
    vec![(title.to_string(), body.to_string())]
}

/// Every `CommandComplete` tag string `sql` answers with, in wire order, on
/// a fresh trust-mode connection. A multi-statement `sql` keeps its
/// transaction on that one connection.
async fn raw_command_tags(port: u16, sql: &str) -> Vec<String> {
    let mut conn = RawPgConn::connect(port, "nodedb", "default").await;
    let messages = conn.simple_query(sql).await;
    command_tags(&messages)
}

/// BEGIN; INSERT answers `INSERT 0 1` at the statement; the same
/// transaction's point read and scan see the row before COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_insert_in_transaction_reports_insert_tag_and_is_visible_before_commit() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_ins").await;
    insert(&server, "crdt_txn_ins", "base", "t0", "b0").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, &insert_sql("crdt_txn_ins", "new", "t1", "b1")).await,
        Some(1),
        "an in-transaction CRDT INSERT answers a real count"
    );
    assert_eq!(
        point(&server, "crdt_txn_ins", "new").await,
        pair("t1", "b1")
    );
    assert_eq!(
        scan_ids(&server, "crdt_txn_ins").await,
        vec!["base".to_string(), "new".to_string()]
    );
    server.exec("ROLLBACK").await.unwrap();
}

/// INSERT, UPSERT and UPDATE inside one transaction each answer the tag of
/// their own verb, read off the raw wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_upsert_and_update_in_transaction_report_their_own_tags() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_tags").await;

    let tags = raw_command_tags(
        server.pg_port,
        "BEGIN; \
         INSERT INTO crdt_txn_tags (id, title, body) VALUES ('a', 't1', 'b1'); \
         UPSERT INTO crdt_txn_tags (id, title, body) VALUES ('a', 't2', 'b2'); \
         UPDATE crdt_txn_tags SET title = 't3' WHERE id = 'a'; \
         DELETE FROM crdt_txn_tags WHERE id = 'a'; \
         DELETE FROM crdt_txn_tags WHERE id = 'ghost'; \
         ROLLBACK",
    )
    .await;
    assert_eq!(
        tags,
        vec![
            "BEGIN".to_string(),
            "INSERT 0 1".to_string(),
            "UPSERT 1".to_string(),
            "UPDATE 1".to_string(),
            "DELETE 1".to_string(),
            "DELETE 0".to_string(),
            "ROLLBACK".to_string(),
        ]
    );
}

/// A partial `UPDATE SET` inside a transaction merges over the row's
/// current body: the touched field changes, the untouched field survives,
/// and a second update in the same transaction sees the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_partial_update_in_transaction_merges_over_untouched_fields() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_upd").await;
    insert(&server, "crdt_txn_upd", "a", "t1", "b1").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_upd SET title = 't2' WHERE id = 'a'"
        )
        .await,
        Some(1)
    );
    assert_eq!(point(&server, "crdt_txn_upd", "a").await, pair("t2", "b1"));

    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_upd SET body = 'b2' WHERE id = 'a'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        point(&server, "crdt_txn_upd", "a").await,
        pair("t2", "b2"),
        "the second update merges over the first staged body"
    );
    server.exec("ROLLBACK").await.unwrap();
}

/// A staged DELETE hides the row from the same transaction's point read
/// and scan while a separate connection still sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_delete_in_transaction_hides_row_from_point_get_and_scan() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_del").await;
    insert(&server, "crdt_txn_del", "a", "t1", "b1").await;
    insert(&server, "crdt_txn_del", "b", "t2", "b2").await;
    server
        .exec("CREATE USER crdt_del_reader WITH PASSWORD 'x' ROLE readwrite")
        .await
        .unwrap();
    let (other, _h) = server.connect_as("crdt_del_reader", "x").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_del WHERE id = 'a'").await,
        Some(1)
    );
    assert!(point(&server, "crdt_txn_del", "a").await.is_empty());
    assert_eq!(
        scan_ids(&server, "crdt_txn_del").await,
        vec!["b".to_string()]
    );

    let other_rows = other
        .simple_query("SELECT id FROM crdt_txn_del WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        other_rows
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_))),
        "a separate connection still sees the row before COMMIT"
    );
    server.exec("ROLLBACK").await.unwrap();
}

/// A DELETE of a row that is absent under BASE ∪ OVERLAY (never inserted,
/// or deleted earlier in the same transaction) answers `DELETE 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_delete_of_missing_row_in_transaction_reports_zero() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_del0").await;
    insert(&server, "crdt_txn_del0", "a", "t1", "b1").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_del0 WHERE id = 'ghost'").await,
        Some(0)
    );
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_del0 WHERE id = 'a'").await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_del0 WHERE id = 'a'").await,
        Some(0),
        "a row tombstoned earlier in the transaction is absent"
    );
    server.exec("ROLLBACK").await.unwrap();
}
