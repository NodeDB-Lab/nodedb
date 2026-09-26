// SPDX-License-Identifier: BUSL-1.1

//! A backup's consistent cut covers Calvin transactions.
//!
//! A transaction that writes two collections on two vShards commits through
//! the Calvin scheduler. Its install records the transaction's commit HLC on
//! the tenant's write mark before the COMMIT is acknowledged, so a Calvin
//! commit after a backup refuses a restore of it. A Calvin transaction held
//! at its flush while a backup starts was sequenced before the backup's cut,
//! so the backup waits for its install and holds its rows.

use super::backup_support::{drain_backup, names_on_two_vshards, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;

async fn create_kv(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The statements of one Calvin transaction writing `key` into both
/// collections.
fn calvin_commit(first: &str, second: &str, key: &str) -> [String; 4] {
    [
        "BEGIN".to_owned(),
        format!("INSERT INTO {first} (key, value) VALUES ('{key}', 'x')"),
        format!("INSERT INTO {second} (key, value) VALUES ('{key}', 'y')"),
        "COMMIT".to_owned(),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_commit_after_the_backup_refuses_the_restore() {
    let server = TestServer::start().await;
    let (first, second) = names_on_two_vshards("calvin_mark");
    create_kv(&server, &first).await;
    create_kv(&server, &second).await;
    for sql in calvin_commit(&first, &second, "before") {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");

    for sql in calvin_commit(&first, &second, "after") {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let error = push_restore(&server.client, TENANT, backup)
        .await
        .expect_err("a restore older than a Calvin commit must be refused");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains("calvin flush"),
        "the refusal must name the Calvin commit as the newer write, got: {error}"
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_commit_held_at_its_flush_is_in_the_backup_or_refuses_its_restore() {
    use std::time::Duration;

    const PARKED_FOR: Duration = Duration::from_millis(1500);

    let (first, second) = names_on_two_vshards("calvin_cut");
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-flush");
    let server = TestServer::start_with_failpoints(&format!(
        "calvin::before_flush::{first}=wait_file({})",
        release.display()
    ))
    .await;
    create_kv(&server, &first).await;
    create_kv(&server, &second).await;

    // Hold the Calvin transaction at its flush on `first`'s vShard.
    let (committer, committer_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the committer");
    let statements = calvin_commit(&first, &second, "held");
    let commit = tokio::spawn(async move {
        for sql in &statements {
            committer
                .simple_query(sql)
                .await
                .map_err(|e| format!("{sql}: {e}"))?;
        }
        Ok::<(), String>(())
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !commit.is_finished(),
        "the COMMIT was not held at its flush"
    );

    // Back up while the transaction is sequenced but not installed.
    let (backup_client, backup_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the backup client");
    let backup = tokio::spawn(async move { drain_backup(&backup_client, TENANT).await });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !backup.is_finished(),
        "the backup snapshotted before a Calvin transaction sequenced ahead of its cut installed"
    );

    std::fs::write(&release, b"release").expect("release the flush");
    commit
        .await
        .expect("commit task")
        .unwrap_or_else(|e| panic!("commit: {e}"));
    let envelope = backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("backup: {e}"));

    for name in [&first, &second] {
        server
            .exec(&format!("DROP COLLECTION {name} PURGE"))
            .await
            .unwrap_or_else(|e| panic!("purge {name}: {e}"));
    }
    match push_restore(&server.client, TENANT, envelope).await {
        Ok(()) => {
            for (name, value) in [(&first, "x"), (&second, "y")] {
                let rows = server
                    .query_text(&format!("SELECT value FROM {name} WHERE key = 'held'"))
                    .await
                    .unwrap_or_else(|e| panic!("read {name}: {e}"));
                assert_eq!(
                    rows,
                    vec![value.to_owned()],
                    "the restore succeeded, so the held Calvin commit must be in the backup"
                );
            }
        }
        Err(error) => assert!(
            error.contains("restore refused"),
            "a restore may only fail by refusing the newer write, got: {error}"
        ),
    }

    committer_handle.abort();
    backup_handle.abort();
}
