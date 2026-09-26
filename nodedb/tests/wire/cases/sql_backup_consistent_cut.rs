// SPDX-License-Identifier: BUSL-1.1

//! Compiled only with `--features failpoints`.
//!
//! A backup is a consistent cut: a write in flight when the backup starts is
//! either in the backup or refuses a restore of it. It is never silently
//! lost.
//!
//! The fail gate `funnel::before_dispatch::<collection>` parks a write after
//! its record is minted and before a core applies it. The test parks an
//! INSERT there, starts a backup, releases the INSERT, and restores the
//! backup over a purge of the collection. The restore either refuses the
//! newer write or brings the row back.

#[cfg(feature = "failpoints")]
use std::time::Duration;

#[cfg(feature = "failpoints")]
use super::backup_support::{drain_backup, push_restore};
#[cfg(feature = "failpoints")]
use crate::harness::TestServer;

#[cfg(feature = "failpoints")]
const TENANT: u64 = 1;

#[cfg(feature = "failpoints")]
const COLLECTION: &str = "cut_docs";

/// How long the parked INSERT and the backup must stay unfinished.
#[cfg(feature = "failpoints")]
const PARKED_FOR: Duration = Duration::from_millis(1500);

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_in_flight_at_a_backup_is_in_the_backup_or_refuses_its_restore() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-insert");
    std::fs::write(&release, b"").expect("open the gate");
    let server = TestServer::start_with_failpoints(&format!(
        "funnel::before_dispatch::{COLLECTION}=wait_file({})",
        release.display()
    ))
    .await;
    server
        .exec(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')"
        ))
        .await
        .expect("create the collection");

    // Park the INSERT between its record and its apply.
    std::fs::remove_file(&release).expect("close the gate");
    let (writer, writer_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the writer");
    let insert = tokio::spawn(async move {
        writer
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (key, value) VALUES ('in_flight', 'x')"
            ))
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !insert.is_finished(),
        "the INSERT was not parked at the gate"
    );

    // Back up while the INSERT is in flight.
    let (backup_client, backup_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the backup client");
    let backup = tokio::spawn(async move { drain_backup(&backup_client, TENANT).await });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !insert.is_finished(),
        "the INSERT left the gate before its release"
    );
    assert!(
        !backup.is_finished(),
        "the backup snapshotted while a write below its watermark had no outcome"
    );

    std::fs::write(&release, b"").expect("release the INSERT");
    insert
        .await
        .expect("insert task")
        .unwrap_or_else(|e| panic!("insert: {e}"));
    let envelope = backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("backup: {e}"));

    server
        .exec(&format!("DROP COLLECTION {COLLECTION} PURGE"))
        .await
        .expect("purge the collection");
    match push_restore(&server.client, TENANT, envelope).await {
        Ok(()) => {
            let rows = server
                .query_text(&format!(
                    "SELECT value FROM {COLLECTION} WHERE key = 'in_flight'"
                ))
                .await
                .expect("read the restored row");
            assert_eq!(
                rows,
                vec!["x".to_string()],
                "the restore succeeded, so the in-flight write must be in the backup"
            );
        }
        Err(error) => {
            assert!(
                error.contains("restore refused"),
                "a restore may only fail by refusing the newer write, got: {error}"
            );
        }
    }

    writer_handle.abort();
    backup_handle.abort();
}
