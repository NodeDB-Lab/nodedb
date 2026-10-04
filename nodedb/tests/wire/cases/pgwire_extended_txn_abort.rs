// SPDX-License-Identifier: BUSL-1.1

//! An error in an extended-query message inside a transaction block aborts the
//! block, as in PostgreSQL: every later statement gets SQLSTATE 25P02 until
//! ROLLBACK or ROLLBACK TO SAVEPOINT. Transaction control sent through the
//! extended protocol reaches the session handlers.

use crate::harness::TestServer;

/// A read the ungranted user holds a grant for. A collection-less statement
/// such as `SELECT 1` needs tenant-wide Read, which also confers Read on the
/// secret collection. The probe therefore reads a granted collection.
const PROBE: &str = "SELECT id FROM txn_abort_public";

/// Create a user with no grant on a strict collection, so a `prepare` of a
/// read on it fails at Parse with 42501, and connect as that user. The user
/// holds Read on a second collection for the probe statements.
async fn connect_ungranted(server: &TestServer) -> tokio_postgres::Client {
    server
        .exec("CREATE ROLE txn_abort_role")
        .await
        .expect("create custom role");
    server
        .exec("CREATE USER txn_abort_user WITH PASSWORD 'x' ROLE txn_abort_role")
        .await
        .expect("create ungranted user");
    server
        .exec(
            "CREATE COLLECTION txn_abort_secret \
             (id TEXT PRIMARY KEY, secret TEXT NOT NULL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create secret collection");
    server
        .exec(
            "CREATE COLLECTION txn_abort_public \
             (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create public collection");
    server
        .exec("GRANT READ ON txn_abort_public TO txn_abort_user")
        .await
        .expect("grant read on public collection");
    let (client, _connection) = server
        .connect_as("txn_abort_user", "x")
        .await
        .expect("connect ungranted user");
    client
}

async fn fail_parse(client: &tokio_postgres::Client) {
    let error = client
        .prepare("SELECT secret FROM txn_abort_secret")
        .await
        .expect_err("Parse must deny the ungranted collection");
    assert_eq!(
        error.as_db_error().expect("server SQLSTATE").code().code(),
        "42501"
    );
}

fn assert_code(error: &tokio_postgres::Error, code: &str) {
    let db_error = error.as_db_error().expect("server SQLSTATE");
    assert_eq!(
        db_error.code().code(),
        code,
        "unexpected error: {}",
        db_error.message()
    );
}

async fn assert_block_aborted(client: &tokio_postgres::Client) {
    let simple = client
        .simple_query(PROBE)
        .await
        .expect_err("a simple query in an aborted block must fail");
    assert_code(&simple, "25P02");
    let extended = client
        .query(PROBE, &[])
        .await
        .expect_err("an extended query in an aborted block must fail");
    assert_code(&extended, "25P02");
    // The aborted block refuses Parse before authorization runs.
    let denied = client
        .prepare("SELECT secret FROM txn_abort_secret")
        .await
        .expect_err("a Parse in an aborted block must fail");
    assert_code(&denied, "25P02");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parse_error_aborts_block_until_rollback() {
    let server = TestServer::start().await;
    let client = connect_ungranted(&server).await;

    client.simple_query("BEGIN").await.expect("begin");
    fail_parse(&client).await;
    assert_block_aborted(&client).await;

    client.simple_query("ROLLBACK").await.expect("rollback");
    client
        .simple_query(PROBE)
        .await
        .expect("ROLLBACK must leave the session usable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parse_error_aborts_block_until_extended_rollback() {
    let server = TestServer::start().await;
    let client = connect_ungranted(&server).await;

    client.execute("BEGIN", &[]).await.expect("extended begin");
    fail_parse(&client).await;
    assert_block_aborted(&client).await;

    client
        .execute("ROLLBACK", &[])
        .await
        .expect("extended ROLLBACK must end an aborted block");
    client
        .query(PROBE, &[])
        .await
        .expect("ROLLBACK must leave the session usable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parse_error_aborts_block_until_rollback_to_savepoint() {
    let server = TestServer::start().await;
    let client = connect_ungranted(&server).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SAVEPOINT before_parse")
        .await
        .expect("savepoint");
    fail_parse(&client).await;
    assert_block_aborted(&client).await;

    client
        .execute("ROLLBACK TO SAVEPOINT before_parse", &[])
        .await
        .expect("ROLLBACK TO must recover an aborted block");
    client
        .query(PROBE, &[])
        .await
        .expect("the block must accept statements after ROLLBACK TO");
    client.simple_query("COMMIT").await.expect("commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parse_error_outside_block_leaves_session_usable() {
    let server = TestServer::start().await;
    let client = connect_ungranted(&server).await;

    fail_parse(&client).await;
    client
        .query(PROBE, &[])
        .await
        .expect("an autocommit Parse error must not abort later statements");
}
