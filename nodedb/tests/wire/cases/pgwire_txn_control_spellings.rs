// SPDX-License-Identifier: BUSL-1.1

//! Every PostgreSQL spelling of transaction control ends or changes a block,
//! including an aborted one: optional `WORK` / `TRANSACTION` noise words,
//! `ABORT`, `END`, any letter case and a trailing semicolon. An aborted block
//! refuses every other statement with SQLSTATE 25P02, on both protocols.

use crate::harness::TestServer;

const PROBE: &str = "SELECT 1";

fn assert_code(error: &tokio_postgres::Error, code: &str) {
    let db_error = error.as_db_error().expect("server SQLSTATE");
    assert_eq!(
        db_error.code().code(),
        code,
        "unexpected error: {}",
        db_error.message()
    );
}

/// Open a block and abort it with a read of a collection that does not exist.
async fn abort_block(client: &tokio_postgres::Client) {
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SELECT id FROM txn_spelling_missing")
        .await
        .expect_err("a read of a missing collection must fail");
    let refused = client
        .simple_query(PROBE)
        .await
        .expect_err("an aborted block must refuse the probe");
    assert_code(&refused, "25P02");
}

async fn end_aborted_block_with_simple(sql: &str) {
    let server = TestServer::start().await;
    abort_block(&server.client).await;
    server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} must end an aborted block: {e}"));
    server
        .client
        .simple_query(PROBE)
        .await
        .unwrap_or_else(|e| panic!("{sql} must leave the session usable: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_transaction_ends_aborted_block() {
    end_aborted_block_with_simple("ROLLBACK TRANSACTION").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_work_ends_aborted_block() {
    end_aborted_block_with_simple("abort work;").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_rollback_work_ends_aborted_block() {
    let server = TestServer::start().await;
    abort_block(&server.client).await;
    server
        .client
        .execute("Rollback Work", &[])
        .await
        .expect("extended ROLLBACK WORK must end an aborted block");
    server
        .client
        .query(PROBE, &[])
        .await
        .expect("ROLLBACK WORK must leave the session usable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_work_to_savepoint_recovers_aborted_block() {
    let server = TestServer::start().await;
    server
        .client
        .simple_query("BEGIN WORK")
        .await
        .expect("begin");
    server
        .client
        .simple_query("SAVEPOINT Before_Error")
        .await
        .expect("savepoint");
    server
        .client
        .simple_query("SELECT id FROM txn_spelling_missing")
        .await
        .expect_err("a read of a missing collection must fail");
    server
        .client
        .simple_query("ROLLBACK WORK TO before_error")
        .await
        .expect("ROLLBACK WORK TO must recover an aborted block");
    server
        .client
        .simple_query(PROBE)
        .await
        .expect("the block must accept statements after ROLLBACK TO");
    server
        .client
        .simple_query("RELEASE before_error")
        .await
        .expect("RELEASE without the SAVEPOINT keyword");
    server
        .client
        .simple_query("COMMIT WORK")
        .await
        .expect("commit work");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_accepts_every_isolation_level() {
    let server = TestServer::start().await;
    for level in [
        "SERIALIZABLE",
        "REPEATABLE READ",
        "READ COMMITTED",
        "READ UNCOMMITTED",
    ] {
        server
            .client
            .simple_query(&format!("START TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .unwrap_or_else(|e| panic!("{level} runs serializable: {e}"));
        server
            .client
            .simple_query("END TRANSACTION")
            .await
            .expect("end transaction");
    }
    let refused = server
        .client
        .simple_query("START TRANSACTION DEFERRABLE")
        .await
        .expect_err("DEFERRABLE must be refused");
    assert_code(&refused, "0A000");
}

/// A prepared backup COPY executed in an aborted block gets 25P02 instead of
/// streaming a backup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_backup_copy_in_aborted_block_is_refused() {
    let server = TestServer::start().await;
    let backup = server
        .client
        .prepare("COPY (BACKUP TENANT 1) TO STDOUT")
        .await
        .expect("prepare backup COPY outside a block");
    abort_block(&server.client).await;
    let refused = server
        .client
        .copy_out(&backup)
        .await
        .err()
        .expect("a backup COPY in an aborted block must fail");
    assert_code(&refused, "25P02");
    server
        .client
        .simple_query("ROLLBACK")
        .await
        .expect("rollback");
}
