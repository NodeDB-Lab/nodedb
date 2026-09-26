// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's staleness guard.
//!
//! A restore is refused when the tenant took a user data write after the
//! envelope's watermark: restoring would silently overwrite that write. Reads
//! and system bookkeeping after the backup are not writes, and never refuse
//! the restore.

use crate::harness::TestServer;

const TENANT: u64 = 1;

async fn drain_backup(server: &TestServer) -> Vec<u8> {
    super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT")
}

async fn push_restore(server: &TestServer, bytes: Vec<u8>) -> Result<(), String> {
    super::backup_support::push_restore(&server.client, TENANT, bytes).await
}

async fn create_with_row(server: &TestServer, name: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')"
        ),
        format!("INSERT INTO {name} (id, v) VALUES ('a', 1)"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test]
async fn a_user_write_after_the_backup_refuses_the_restore() {
    let server = TestServer::start().await;
    create_with_row(&server, "stale_guard_docs").await;
    let backup = drain_backup(&server).await;

    server
        .exec("INSERT INTO stale_guard_docs (id, v) VALUES ('b', 2)")
        .await
        .expect("a newer user write");

    let error = push_restore(&server, backup)
        .await
        .expect_err("a restore older than a user write must be refused");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains("stale_guard_docs"),
        "the refusal must name the collection of the newer write, got: {error}"
    );
}

#[tokio::test]
async fn reads_and_ddl_after_the_backup_do_not_refuse_the_restore() {
    let server = TestServer::start().await;
    create_with_row(&server, "stale_guard_reads").await;
    let backup = drain_backup(&server).await;

    for _ in 0..3 {
        server
            .query_text("SELECT id FROM stale_guard_reads")
            .await
            .expect("a read after the backup");
    }
    server
        .exec("DROP COLLECTION stale_guard_reads PURGE")
        .await
        .expect("purge before the restore");

    push_restore(&server, backup)
        .await
        .expect("reads and system bookkeeping after the backup must not refuse the restore");
    let rows = server
        .query_text("SELECT id FROM stale_guard_reads")
        .await
        .expect("read the restored collection");
    assert_eq!(rows, vec!["a".to_string()]);
}
