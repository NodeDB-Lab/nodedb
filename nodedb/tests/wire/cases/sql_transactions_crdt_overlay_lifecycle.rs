// SPDX-License-Identifier: BUSL-1.1

//! Lifecycle of staged CRDT row writes: ROLLBACK and `ROLLBACK TO
//! SAVEPOINT` discard them, COMMIT applies each exactly once through the
//! live handler. Statement-time tags and read-your-own-writes are covered by
//! `sql_transactions_crdt_overlay`, whose helpers this file shares.

use super::sql_transactions_crdt_overlay::{
    affected, create, insert, insert_sql, pair, point, scan_ids,
};
use crate::harness::TestServer;

/// ROLLBACK discards a staged insert, a staged update and a staged delete:
/// the base rows read exactly as before BEGIN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_rollback_discards_staged_writes() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_rb").await;
    insert(&server, "crdt_txn_rb", "keep", "t1", "b1").await;
    insert(&server, "crdt_txn_rb", "gone", "t2", "b2").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, &insert_sql("crdt_txn_rb", "new", "t3", "b3")).await,
        Some(1)
    );
    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_rb SET title = 'tx' WHERE id = 'keep'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_rb WHERE id = 'gone'").await,
        Some(1)
    );
    assert_eq!(
        scan_ids(&server, "crdt_txn_rb").await,
        vec!["keep".to_string(), "new".to_string()]
    );
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        scan_ids(&server, "crdt_txn_rb").await,
        vec!["gone".to_string(), "keep".to_string()]
    );
    assert_eq!(
        point(&server, "crdt_txn_rb", "keep").await,
        pair("t1", "b1")
    );
    assert_eq!(
        point(&server, "crdt_txn_rb", "gone").await,
        pair("t2", "b2")
    );
    assert!(point(&server, "crdt_txn_rb", "new").await.is_empty());
}

/// COMMIT applies the staged writes once: the inserted row exists exactly
/// once, the merged update is the durable value, and the deleted row is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_commit_applies_staged_writes_once() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_commit").await;
    insert(&server, "crdt_txn_commit", "gone", "t0", "b0").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, &insert_sql("crdt_txn_commit", "a", "t1", "b1")).await,
        Some(1)
    );
    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_commit SET title = 't2' WHERE id = 'a'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_commit WHERE id = 'gone'").await,
        Some(1)
    );
    server.exec("COMMIT").await.unwrap();

    let count = server
        .query_rows("SELECT COUNT(*) FROM crdt_txn_commit WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(count[0][0], "1", "the committed row exists exactly once");
    assert_eq!(
        point(&server, "crdt_txn_commit", "a").await,
        pair("t2", "b1")
    );
    assert_eq!(
        scan_ids(&server, "crdt_txn_commit").await,
        vec!["a".to_string()]
    );

    // The committed row keeps merging: a later autocommit update sees the
    // committed body, so the untouched field survives.
    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_commit SET body = 'b3' WHERE id = 'a'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        point(&server, "crdt_txn_commit", "a").await,
        pair("t2", "b3")
    );
}

/// `ROLLBACK TO SAVEPOINT` restores the overlay to the savepoint: writes
/// staged before it stay visible, writes staged after it are discarded, and
/// COMMIT makes only the surviving writes durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_rollback_to_savepoint_restores_prior_staged_state() {
    let server = TestServer::start().await;
    create(&server, "crdt_txn_sp").await;
    insert(&server, "crdt_txn_sp", "base", "t0", "b0").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, &insert_sql("crdt_txn_sp", "before", "t1", "b1")).await,
        Some(1)
    );
    server.exec("SAVEPOINT s1").await.unwrap();
    assert_eq!(
        affected(&server, &insert_sql("crdt_txn_sp", "after", "t2", "b2")).await,
        Some(1)
    );
    assert_eq!(
        affected(
            &server,
            "UPDATE crdt_txn_sp SET title = 'tx' WHERE id = 'before'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM crdt_txn_sp WHERE id = 'base'").await,
        Some(1)
    );
    assert_eq!(
        scan_ids(&server, "crdt_txn_sp").await,
        vec!["after".to_string(), "before".to_string()]
    );

    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    assert_eq!(
        scan_ids(&server, "crdt_txn_sp").await,
        vec!["base".to_string(), "before".to_string()]
    );
    assert_eq!(
        point(&server, "crdt_txn_sp", "before").await,
        pair("t1", "b1")
    );
    assert_eq!(
        point(&server, "crdt_txn_sp", "base").await,
        pair("t0", "b0")
    );
    assert!(point(&server, "crdt_txn_sp", "after").await.is_empty());

    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        scan_ids(&server, "crdt_txn_sp").await,
        vec!["base".to_string(), "before".to_string()]
    );
    assert_eq!(
        point(&server, "crdt_txn_sp", "before").await,
        pair("t1", "b1")
    );
}
