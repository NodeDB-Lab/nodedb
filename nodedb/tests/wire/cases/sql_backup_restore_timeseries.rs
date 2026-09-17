// SPDX-License-Identifier: BUSL-1.1

//! BACKUP / RESTORE of a timeseries collection with a declared time key.
//!
//! A row restored into a timeseries collection reads back exactly as the row
//! that was inserted: a declared `TIMESTAMP` / `TIMESTAMPTZ` time key is the
//! same instant, a `BIGINT` time key the same integer, and the other columns
//! are intact. This holds whether the rows sat in the memtable or had been
//! flushed to a partition at backup time, and it survives a restart of the
//! restored server.
//!
//! The restore target is a genuinely clean one: the collection is hard-purged
//! before the restore (Data Plane registration, memtable and partitions all
//! gone), or the restore lands on a fresh server. A restore that inferred the
//! collection shape from the reissued rows, instead of registering the
//! declared one first, would stamp the rows with the restore-time clock and
//! leave the time key as an integer column.

use crate::harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;

const TENANT: u64 = 1;

/// Event time, years in the past, so a restore-time "now" stamp is trivially
/// separable from the inserted value.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` as a declared instant column renders it over pgwire.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";
/// A `BIGINT` time key value in epoch milliseconds.
const EARLY_MS: &str = "1700000000000";

async fn drain_backup(server: &TestServer, tenant: u64) -> Vec<u8> {
    let stream = server
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out: BACKUP TENANT");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy_out chunk"));
    }
    bytes
}

async fn push_restore(server: &TestServer, tenant: u64, bytes: Vec<u8>) {
    let sink = server
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .expect("copy_in: RESTORE TENANT");
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .expect("send backup bytes");
    sink.as_mut()
        .finish()
        .await
        .expect("finish copy_in: RESTORE TENANT");
}

/// Create the collection, insert one row, and return what the time key reads
/// back as before the backup.
async fn create_and_insert(
    srv: &TestServer,
    name: &str,
    time_key_type: &str,
    time: &str,
) -> String {
    srv.exec(&format!(
        "CREATE COLLECTION {name} \
         (captured_at {time_key_type} TIME_KEY, host TEXT, v FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await
    .unwrap_or_else(|e| panic!("CREATE COLLECTION {name}: {e}"));
    srv.exec(&format!(
        "INSERT INTO {name} (captured_at, host, v) VALUES ('{time}', 'h1', 1.5)"
    ))
    .await
    .unwrap_or_else(|e| panic!("INSERT INTO {name}: {e}"));
    let rows = srv
        .query_text(&format!("SELECT captured_at FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT captured_at FROM {name}: {e}"));
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    rows[0].clone()
}

/// The restored row carries the same time key and the same other columns.
async fn assert_restored_row(srv: &TestServer, name: &str, expected_time: &str) {
    let times = srv
        .query_text(&format!("SELECT captured_at FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("post-restore SELECT captured_at FROM {name}: {e}"));
    assert_eq!(
        times,
        vec![expected_time.to_string()],
        "the restored time key must read back as the inserted value"
    );
    let hosts = srv
        .query_text(&format!("SELECT host FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("post-restore SELECT host FROM {name}: {e}"));
    assert_eq!(
        hosts,
        vec!["h1".to_string()],
        "the restored row must carry its other columns intact"
    );
}

/// Backup, hard-purge the collection on the same server, restore, and check
/// the row. The purge removes the catalog row, the Data Plane registration,
/// the memtable and every partition, so the restore lands on a clean target.
async fn backup_purge_restore(srv: &TestServer, name: &str, expected_time: &str) {
    let backup_bytes = drain_backup(srv, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );

    srv.exec(&format!("DROP COLLECTION {name} PURGE"))
        .await
        .unwrap_or_else(|e| panic!("DROP COLLECTION {name} PURGE: {e}"));

    push_restore(srv, TENANT, backup_bytes).await;
    assert_restored_row(srv, name, expected_time).await;
}

#[tokio::test]
async fn a_declared_time_key_survives_backup_and_restore() {
    let srv = TestServer::start().await;
    let before = create_and_insert(&srv, "ts_bk_naive", "TIMESTAMP", EARLY).await;
    assert_eq!(
        before, EARLY_ISO,
        "the inserted instant reads back before the backup"
    );
    backup_purge_restore(&srv, "ts_bk_naive", &before).await;
}

/// The same round trip once every row has been flushed to a partition: the
/// partition schema carries the time kind, and the restore reads it from
/// there rather than from the memtable snapshot.
#[tokio::test]
async fn a_declared_time_key_survives_backup_and_restore_from_a_flushed_partition() {
    let srv = TestServer::start_with_timeseries_memtable_budget(1).await;
    let before = create_and_insert(&srv, "ts_bk_flushed", "TIMESTAMP", EARLY).await;
    assert_eq!(
        before, EARLY_ISO,
        "the inserted instant reads back before the backup"
    );
    backup_purge_restore(&srv, "ts_bk_flushed", &before).await;
}

#[tokio::test]
async fn a_declared_timestamptz_time_key_survives_backup_and_restore() {
    let srv = TestServer::start().await;
    let before = create_and_insert(&srv, "ts_bk_utc", "TIMESTAMPTZ", EARLY).await;
    assert_eq!(
        before, EARLY_ISO,
        "the inserted instant reads back before the backup"
    );
    backup_purge_restore(&srv, "ts_bk_utc", &before).await;
}

/// A `BIGINT` time key is not an instant: it restores as the integer that
/// was inserted.
#[tokio::test]
async fn a_bigint_time_key_stays_an_integer_after_restore() {
    let srv = TestServer::start().await;
    let before = create_and_insert(&srv, "ts_bk_bigint", "BIGINT", EARLY_MS).await;
    assert_eq!(
        before, EARLY_MS,
        "the inserted integer reads back before the backup"
    );
    backup_purge_restore(&srv, "ts_bk_bigint", &before).await;
}

/// Restore into a fresh server, then restart it on the same data directory:
/// the reissued row is WAL-durable and the boot-time registration of the
/// restored collection types it the same way the restore-time one did.
#[tokio::test]
async fn a_declared_time_key_survives_restore_and_restart() {
    let srv_a = TestServer::start().await;
    let before = create_and_insert(&srv_a, "ts_bk_restart", "TIMESTAMP", EARLY).await;
    assert_eq!(
        before, EARLY_ISO,
        "the inserted instant reads back before the backup"
    );
    let backup_bytes = drain_backup(&srv_a, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );
    drop(srv_a);

    let srv_b = TestServer::start().await;
    push_restore(&srv_b, TENANT, backup_bytes).await;
    assert_restored_row(&srv_b, "ts_bk_restart", &before).await;

    let (srv_b, dir) = srv_b.take_dir();
    srv_b.graceful_shutdown().await;
    let (srv_c, _dir) = TestServer::open_on_path(dir).await;
    assert_restored_row(&srv_c, "ts_bk_restart", &before).await;
}
