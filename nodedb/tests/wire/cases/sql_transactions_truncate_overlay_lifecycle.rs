// SPDX-License-Identifier: BUSL-1.1

//! Lifecycle of a staged in-transaction `TRUNCATE`: ROLLBACK and
//! `ROLLBACK TO SAVEPOINT` undo it, COMMIT applies it exactly once in
//! statement order (a write staged before it is wiped, a write staged after
//! it survives), and `RESTART IDENTITY` takes effect only at COMMIT.
//! Statement-time visibility lives in `sql_transactions_truncate_overlay`.

use super::sql_transactions_truncate_overlay::{create, ids, insert, point, scan_ids, seed};
use crate::harness::TestServer;

/// BEGIN; TRUNCATE; ROLLBACK leaves every row in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_rollback_restores_rows() {
    let server = TestServer::start().await;
    create(&server, "trunc_lc_rollback").await;
    seed(&server, "trunc_lc_rollback").await;

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_lc_rollback").await.unwrap();
    insert(&server, "trunc_lc_rollback", "z", "us").await;
    assert_eq!(scan_ids(&server, "trunc_lc_rollback").await, ids(&["z"]));
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        scan_ids(&server, "trunc_lc_rollback").await,
        ids(&["a", "b", "c"])
    );
    assert_eq!(
        point(&server, "trunc_lc_rollback", "a").await,
        vec!["us".to_string()]
    );
    assert_eq!(
        point(&server, "trunc_lc_rollback", "z").await,
        Vec::<String>::new()
    );
}

/// COMMIT replays the TRUNCATE in statement order: an INSERT staged before
/// it is wiped, an UPDATE staged before it leaves nothing behind, and an
/// INSERT staged after it survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_commit_wipes_earlier_staged_write_and_keeps_later_insert() {
    let server = TestServer::start().await;
    create(&server, "trunc_lc_commit").await;
    seed(&server, "trunc_lc_commit").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "trunc_lc_commit", "before", "us").await;
    server
        .exec("UPDATE trunc_lc_commit SET region = 'apac' WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        scan_ids(&server, "trunc_lc_commit").await,
        ids(&["a", "b", "before", "c"])
    );
    server.exec("TRUNCATE trunc_lc_commit").await.unwrap();
    assert_eq!(
        scan_ids(&server, "trunc_lc_commit").await,
        Vec::<String>::new()
    );
    insert(&server, "trunc_lc_commit", "after", "eu").await;
    assert_eq!(scan_ids(&server, "trunc_lc_commit").await, ids(&["after"]));
    server.exec("COMMIT").await.unwrap();

    assert_eq!(scan_ids(&server, "trunc_lc_commit").await, ids(&["after"]));
    assert_eq!(
        point(&server, "trunc_lc_commit", "after").await,
        vec!["eu".to_string()]
    );
    assert_eq!(
        point(&server, "trunc_lc_commit", "a").await,
        Vec::<String>::new()
    );
    assert_eq!(
        point(&server, "trunc_lc_commit", "before").await,
        Vec::<String>::new()
    );
}

/// `ROLLBACK TO SAVEPOINT` taken before the TRUNCATE restores both the base
/// rows and the write staged before the savepoint; the write staged after
/// the TRUNCATE is gone. COMMIT then keeps the pre-savepoint state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_rollback_to_savepoint_untruncates() {
    let server = TestServer::start().await;
    create(&server, "trunc_lc_sp").await;
    seed(&server, "trunc_lc_sp").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "trunc_lc_sp", "pre", "us").await;
    server.exec("SAVEPOINT s1").await.unwrap();
    server.exec("TRUNCATE trunc_lc_sp").await.unwrap();
    insert(&server, "trunc_lc_sp", "post", "eu").await;
    assert_eq!(scan_ids(&server, "trunc_lc_sp").await, ids(&["post"]));
    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();

    assert_eq!(
        scan_ids(&server, "trunc_lc_sp").await,
        ids(&["a", "b", "c", "pre"]),
        "the savepoint rollback drops the truncate marker and the later insert"
    );
    assert_eq!(
        point(&server, "trunc_lc_sp", "pre").await,
        vec!["us".to_string()]
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        scan_ids(&server, "trunc_lc_sp").await,
        ids(&["a", "b", "c", "pre"])
    );
    assert_eq!(
        point(&server, "trunc_lc_sp", "post").await,
        Vec::<String>::new()
    );
}

/// Values of the `SERIAL` column `n`, sorted ascending.
async fn serial_values(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT n FROM {name} ORDER BY n"))
        .await
        .unwrap_or_else(|e| panic!("serial values of {name}: {e}"))
}

/// `TRUNCATE ... RESTART IDENTITY` inside a transaction resets the `SERIAL`
/// sequence only at COMMIT: after a ROLLBACK the next value continues,
/// after a COMMIT it restarts from 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_restart_identity_applies_only_at_commit() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION trunc_lc_serial FIELDS (n SERIAL, v TEXT)")
        .await
        .unwrap();
    for v in ["a", "b"] {
        server
            .exec(&format!("INSERT INTO trunc_lc_serial (v) VALUES ('{v}')"))
            .await
            .unwrap();
    }
    assert_eq!(
        serial_values(&server, "trunc_lc_serial").await,
        ids(&["1", "2"])
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec("TRUNCATE trunc_lc_serial RESTART IDENTITY")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();
    server
        .exec("INSERT INTO trunc_lc_serial (v) VALUES ('c')")
        .await
        .unwrap();
    assert_eq!(
        serial_values(&server, "trunc_lc_serial").await,
        ids(&["1", "2", "3"]),
        "a rolled-back RESTART IDENTITY leaves the sequence where it was"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec("TRUNCATE trunc_lc_serial RESTART IDENTITY")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    server
        .exec("INSERT INTO trunc_lc_serial (v) VALUES ('d')")
        .await
        .unwrap();
    assert_eq!(
        serial_values(&server, "trunc_lc_serial").await,
        ids(&["1"]),
        "a committed RESTART IDENTITY restarts the sequence"
    );
}

/// Keys of every row in a KV collection, sorted ascending.
async fn kv_keys(server: &TestServer, name: &str) -> Vec<String> {
    let mut keys = server
        .query_text(&format!("SELECT k FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("kv keys of {name}: {e}"));
    keys.sort();
    keys
}

/// A KV `TRUNCATE` inside a transaction stages an overlay marker like the
/// document engine's: ROLLBACK leaves every row in place, and COMMIT
/// replays the truncate so the collection reads back empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_kv_inside_transaction_rollback_restores_and_commit_empties() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION trunc_lc_kv (k TEXT PRIMARY KEY, v TEXT) WITH (engine='kv')")
        .await
        .unwrap();
    for k in ["a", "b", "c"] {
        server
            .exec(&format!(
                "INSERT INTO trunc_lc_kv (k, v) VALUES ('{k}', 'x')"
            ))
            .await
            .unwrap();
    }
    assert_eq!(kv_keys(&server, "trunc_lc_kv").await, ids(&["a", "b", "c"]));

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_lc_kv").await.unwrap();
    assert_eq!(
        kv_keys(&server, "trunc_lc_kv").await,
        Vec::<String>::new(),
        "a staged KV truncate hides every base row from the transaction's reads"
    );
    server.exec("ROLLBACK").await.unwrap();
    assert_eq!(
        kv_keys(&server, "trunc_lc_kv").await,
        ids(&["a", "b", "c"]),
        "ROLLBACK drops the truncate marker and leaves every row in place"
    );
    assert_eq!(
        server
            .query_text("SELECT v FROM trunc_lc_kv WHERE k = 'a'")
            .await
            .unwrap(),
        vec!["x".to_string()]
    );

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_lc_kv").await.unwrap();
    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        kv_keys(&server, "trunc_lc_kv").await,
        Vec::<String>::new(),
        "COMMIT replays the KV truncate"
    );
    assert_eq!(
        server
            .query_text("SELECT v FROM trunc_lc_kv WHERE k = 'a'")
            .await
            .unwrap(),
        Vec::<String>::new(),
        "a point read of a truncated key returns no row"
    );

    server
        .exec("INSERT INTO trunc_lc_kv (k, v) VALUES ('z', 'new')")
        .await
        .unwrap();
    assert_eq!(kv_keys(&server, "trunc_lc_kv").await, ids(&["z"]));
}
