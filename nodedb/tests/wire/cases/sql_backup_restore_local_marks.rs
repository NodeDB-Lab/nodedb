// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's staleness guard on a server with no Raft groups.
//!
//! Such a server has no data group to carry its write marks, so it keeps
//! them in its catalog under a local pseudo-group, durable before each
//! write's WAL record. A write after a backup still refuses a restore of it
//! after a restart.

use super::backup_support::{drain_backup, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;

/// Shut `server` down and reopen it, still standalone, on the same data
/// directory. The directory must outlive the reopened server.
async fn restart(server: TestServer) -> (TestServer, impl Sized) {
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    TestServer::open_on_path_standalone(dir).await
}

async fn exec_all(server: &TestServer, statements: &[&str]) {
    for sql in statements {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn assert_refused(server: &TestServer, backup: Vec<u8>, collection: &str) {
    let error = push_restore(&server.client, TENANT, backup)
        .await
        .expect_err("a write after the backup must refuse the restore after a restart");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains("local write"),
        "the refusal must come from the durable local mark, got: {error}"
    );
    assert!(
        error.contains(collection),
        "the refusal must name the collection of the newer write, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_write_after_the_backup_refuses_the_restore_after_a_restart() {
    let server = TestServer::start_standalone().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION local_mark_docs (id STRING PRIMARY KEY, value STRING) \
             WITH (engine='document_strict')",
            "INSERT INTO local_mark_docs (id, value) VALUES ('a', '1')",
        ],
    )
    .await;
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");
    exec_all(
        &server,
        &["INSERT INTO local_mark_docs (id, value) VALUES ('b', '2')"],
    )
    .await;

    let (server, _dir) = restart(server).await;
    assert_refused(&server, backup, "local_mark_docs").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_commit_after_the_backup_refuses_the_restore_after_a_restart() {
    let server = TestServer::start_standalone().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION local_mark_kv (key STRING PRIMARY KEY, value STRING) \
           WITH (engine='kv')",
        ],
    )
    .await;
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");
    exec_all(
        &server,
        &[
            "BEGIN",
            "INSERT INTO local_mark_kv (key, value) VALUES ('k1', 'x')",
            "INSERT INTO local_mark_kv (key, value) VALUES ('k2', 'y')",
            "COMMIT",
        ],
    )
    .await;

    let (server, _dir) = restart(server).await;
    assert_refused(&server, backup, "local_mark_kv").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_backup_after_the_last_write_restores_after_a_restart() {
    let server = TestServer::start_standalone().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION local_mark_fresh (id STRING PRIMARY KEY, value STRING) \
             WITH (engine='document_strict')",
            "INSERT INTO local_mark_fresh (id, value) VALUES ('a', '1')",
        ],
    )
    .await;
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");

    let (server, _dir) = restart(server).await;
    push_restore(&server.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("a backup newer than every write must restore: {e}"));
}
