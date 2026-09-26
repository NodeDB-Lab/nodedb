// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's staleness guard reads durable, replicated write marks.
//!
//! A server restart clears every in-memory mark. A write committed after a
//! backup still refuses a restore of it after the restart, because each data
//! group persists the marks of the writes it applied. A Calvin commit
//! persists its mark before its COMMIT is acknowledged.

use super::backup_support::{drain_backup, names_on_two_vshards, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;

async fn exec_all(server: &TestServer, statements: &[String]) {
    for sql in statements {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Shut `server` down and reopen it on the same data directory. The
/// directory must outlive the reopened server.
async fn restart(server: TestServer) -> (TestServer, impl Sized) {
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    TestServer::open_on_path(dir).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_after_the_backup_refuses_the_restore_after_a_restart() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION durable_mark_kv (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
                .to_owned(),
            "INSERT INTO durable_mark_kv (key, value) VALUES ('a', '1')".to_owned(),
        ],
    )
    .await;
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");
    exec_all(
        &server,
        &["INSERT INTO durable_mark_kv (key, value) VALUES ('b', '2')".to_owned()],
    )
    .await;

    let (server, _dir) = restart(server).await;

    let error = push_restore(&server.client, TENANT, backup)
        .await
        .expect_err("a write after the backup must refuse the restore after a restart");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains("durable_mark_kv"),
        "the refusal must name the collection of the newer write, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_commit_after_the_backup_refuses_the_restore_after_a_restart() {
    let server = TestServer::start().await;
    let (first, second) = names_on_two_vshards("durable_calvin");
    for name in [&first, &second] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (key STRING PRIMARY KEY, value STRING) \
                 WITH (engine='kv')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");
    exec_all(
        &server,
        &[
            "BEGIN".to_owned(),
            format!("INSERT INTO {first} (key, value) VALUES ('k', 'x')"),
            format!("INSERT INTO {second} (key, value) VALUES ('k', 'y')"),
            "COMMIT".to_owned(),
        ],
    )
    .await;

    let (server, _dir) = restart(server).await;

    let error = push_restore(&server.client, TENANT, backup)
        .await
        .expect_err("a Calvin commit after the backup must refuse the restore after a restart");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains("calvin flush"),
        "the refusal must name the Calvin commit as the newer write, got: {error}"
    );
}
