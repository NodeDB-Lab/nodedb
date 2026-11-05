// SPDX-License-Identifier: BUSL-1.1

//! A database `max_storage_bytes` quota refuses writes that add bytes.
//!
//! The limit is checked at write admission against the cluster total the
//! storage aggregator publishes. The test server runs a one-node Raft
//! cluster, so every write here passes the leader proposal gate. A round that
//! publishes a total at the limit refuses the next INSERT, UPDATE, and
//! explicit-transaction COMMIT that adds rows with `53100`. A DELETE passes.
//! Once a purge and a round lower the total, writes pass again.

use std::time::{Duration, Instant};

use nodedb::control::metrics::storage::{StorageAggregator, sample_storage};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::DatabaseId;

/// SQLSTATE of a write refused by the storage quota.
const DISK_FULL: &str = "53100";
const DATABASE: &str = "storage_quota_db";

fn db_id(server: &TestServer) -> DatabaseId {
    server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(DATABASE)
        .expect("catalog lookup")
        .expect("the database exists")
}

/// Run one storage sample and one round. Returns the database's cluster total.
async fn round(server: &TestServer, db: DatabaseId) -> u64 {
    sample_storage(&server.shared)
        .await
        .expect("storage sample");
    StorageAggregator::new(Duration::from_secs(30))
        .run_round(&server.shared)
        .await;
    let totals = &server.shared.cluster_storage_totals;
    assert!(totals.has_total(), "a lone node's round is complete");
    totals.database_bytes(db)
}

/// The SQLSTATE and message of a refused statement.
async fn refusal(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = client
        .simple_query(sql)
        .await
        .expect_err("the statement must be refused");
    let db_error = error.as_db_error().expect("a server error");
    (
        db_error.code().code().to_owned(),
        db_error.message().to_owned(),
    )
}

/// A server with a database that holds 50 rows and a quota equal to the
/// stored total, so the database is full.
struct FullDatabase {
    server: TestServer,
    db: DatabaseId,
    client: tokio_postgres::Client,
    _handle: tokio::task::JoinHandle<()>,
    written: u64,
}

async fn full_database() -> FullDatabase {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("CREATE DATABASE");
    let db = db_id(&server);
    server.shared.database_metrics.ensure_database(db);
    let (client, handle) = server
        .connect_as_database("nodedb", "nodedb", DATABASE)
        .await
        .expect("connect to the database");

    client
        .simple_query("CREATE COLLECTION quota_rows")
        .await
        .expect("CREATE COLLECTION");
    for i in 0..50u32 {
        client
            .simple_query(&format!(
                "INSERT INTO quota_rows (id, payload) VALUES ('r{i}', 'payload-{i}-{}')",
                "x".repeat(64)
            ))
            .await
            .expect("insert under no limit");
    }
    let written = round(&server, db).await;
    assert!(written > 0, "50 rows raise the database total");

    server
        .exec(&format!(
            "ALTER DATABASE {DATABASE} SET QUOTA (max_storage_bytes = {written})"
        ))
        .await
        .expect("ALTER DATABASE SET QUOTA");
    FullDatabase {
        server,
        db,
        client,
        _handle: handle,
        written,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_database_refuses_inserts_and_updates_but_allows_deletes() {
    let FullDatabase {
        server,
        db,
        client,
        written,
        ..
    } = full_database().await;

    let (code, message) = refusal(
        &client,
        "INSERT INTO quota_rows (id, payload) VALUES ('over', 'x')",
    )
    .await;
    assert_eq!(code, DISK_FULL, "{message}");
    assert!(message.contains(&written.to_string()), "{message}");
    assert!(message.contains("raise max_storage_bytes"), "{message}");

    let (code, message) = refusal(
        &client,
        "UPDATE quota_rows SET payload = 'changed' WHERE id = 'r1'",
    )
    .await;
    assert_eq!(code, DISK_FULL, "{message}");

    client
        .simple_query("DELETE FROM quota_rows WHERE id = 'r0'")
        .await
        .expect("a DELETE passes over the limit");

    client
        .simple_query("DROP COLLECTION quota_rows PURGE")
        .await
        .expect("DROP COLLECTION PURGE passes over the limit");
    client
        .simple_query("CREATE COLLECTION quota_after")
        .await
        .expect("CREATE COLLECTION");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut total = round(&server, db).await;
    while total >= written && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        total = round(&server, db).await;
    }
    assert!(
        total < written,
        "the purge lowers the total below the limit"
    );

    client
        .simple_query("INSERT INTO quota_after (id, payload) VALUES ('ok', 'x')")
        .await
        .expect("an INSERT passes once the total is under the limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_transaction_that_adds_rows_is_refused_at_commit() {
    // The server and the connection task live for the whole test.
    let FullDatabase {
        server: _server,
        client,
        _handle,
        ..
    } = full_database().await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("INSERT INTO quota_rows (id, payload) VALUES ('txn', 'x')")
        .await
        .expect("a staged INSERT stores nothing yet");
    let (code, message) = refusal(&client, "COMMIT").await;
    assert_eq!(code, DISK_FULL, "{message}");
    client.simple_query("ROLLBACK").await.ok();

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("DELETE FROM quota_rows WHERE id = 'r2'")
        .await
        .expect("a staged DELETE");
    client
        .simple_query("COMMIT")
        .await
        .expect("a transaction that only deletes commits over the limit");
}
