// SPDX-License-Identifier: BUSL-1.1

//! RESTORE of columnar and timeseries collections beside client writes.
//!
//! A RESTORE re-issues each columnar key and each timeseries chunk as a
//! Calvin transaction. It never truncates the destination:
//!
//! - a key the destination wrote after the backup and before the RESTORE is
//!   set back to the backup's row;
//! - a key a client writes while the RESTORE runs, and the backup does not
//!   hold, survives it;
//! - a timeseries row a TRUNCATE ended before the backup stays ended;
//! - a second RESTORE of the same backup installs no timeseries row twice.

use crate::harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;

const TENANT: u64 = 1;
const BACKED_UP: usize = 20;
const CONCURRENT: usize = 20;

/// Restore `envelope` into `tenant` over `client` with `FORCE`: the server
/// holds writes newer than the backup.
async fn push_restore_force(
    client: &tokio_postgres::Client,
    tenant: u64,
    envelope: Vec<u8>,
) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN FORCE"))
        .await
        .map_err(|e| format!("copy_in: {e:?}"))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| format!("send: {e:?}"))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

#[tokio::test]
async fn a_columnar_restore_keeps_concurrent_writes() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rcw_cols COLUMNS (id TEXT, v BIGINT) WITH (engine='columnar')")
        .await
        .expect("CREATE COLLECTION rcw_cols");
    for i in 0..BACKED_UP {
        server
            .exec(&format!("INSERT INTO rcw_cols (id, v) VALUES ('k{i}', 1)"))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e}"));
    }
    let envelope = super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT");

    // A write after the backup and before the RESTORE: set back.
    server
        .exec("UPDATE rcw_cols SET v = 99 WHERE id = 'k0'")
        .await
        .expect("UPDATE k0");

    let (restorer, _restorer_task) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("second connection");
    let restore =
        tokio::spawn(async move { push_restore_force(&restorer, TENANT, envelope).await });
    // Keys the backup does not hold, written while the RESTORE runs.
    for i in 0..CONCURRENT {
        server
            .exec(&format!("INSERT INTO rcw_cols (id, v) VALUES ('c{i}', 2)"))
            .await
            .unwrap_or_else(|e| panic!("concurrent INSERT c{i}: {e}"));
    }
    restore
        .await
        .expect("restore task")
        .expect("RESTORE TENANT FORCE");

    let backed_up = server
        .query_text("SELECT id FROM rcw_cols WHERE v = 1")
        .await
        .expect("SELECT backed-up keys");
    assert_eq!(
        backed_up.len(),
        BACKED_UP,
        "every backed-up key holds the backup's row, k0 included: {backed_up:?}"
    );
    let concurrent = server
        .query_text("SELECT id FROM rcw_cols WHERE v = 2")
        .await
        .expect("SELECT concurrent keys");
    assert_eq!(
        concurrent.len(),
        CONCURRENT,
        "every key written beside the RESTORE survives it: {concurrent:?}"
    );
    let overwritten = server
        .query_text("SELECT id FROM rcw_cols WHERE v = 99")
        .await
        .expect("SELECT overwritten key");
    assert!(
        overwritten.is_empty(),
        "the write before the RESTORE is set back: {overwritten:?}"
    );
}

#[tokio::test]
async fn a_truncated_timeseries_row_stays_ended_after_restore() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rcw_ts (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION rcw_ts");
    server
        .exec("INSERT INTO rcw_ts (captured_at, host, v) VALUES ('2020-03-05 10:00:00', 'old', 1)")
        .await
        .expect("INSERT old");
    server
        .exec("TRUNCATE rcw_ts")
        .await
        .expect("TRUNCATE rcw_ts");
    server
        .exec("INSERT INTO rcw_ts (captured_at, host, v) VALUES ('2020-03-05 11:00:00', 'new', 2)")
        .await
        .expect("INSERT new");
    let envelope = super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT");
    server
        .exec("DROP COLLECTION rcw_ts PURGE")
        .await
        .expect("DROP COLLECTION rcw_ts PURGE");
    super::backup_support::push_restore(&server.client, TENANT, envelope)
        .await
        .expect("RESTORE TENANT");

    let hosts = server
        .query_text("SELECT host FROM rcw_ts")
        .await
        .expect("SELECT host");
    assert_eq!(
        hosts,
        vec!["new".to_string()],
        "the truncated row stays ended, the current row restores"
    );
}

#[tokio::test]
async fn a_timeseries_restore_is_idempotent() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rcw_ts_twice (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION rcw_ts_twice");
    for (hour, v) in [(10, 1), (11, 2), (12, 2)] {
        server
            .exec(&format!(
                "INSERT INTO rcw_ts_twice (captured_at, host, v) \
                 VALUES ('2020-03-05 {hour}:00:00', 'h1', {v})"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT hour {hour}: {e}"));
    }
    let envelope = super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT");
    server
        .exec("DROP COLLECTION rcw_ts_twice PURGE")
        .await
        .expect("DROP COLLECTION rcw_ts_twice PURGE");

    for round in 0..2 {
        push_restore_force(&server.client, TENANT, envelope.clone())
            .await
            .unwrap_or_else(|e| panic!("RESTORE round {round}: {e}"));
        let hosts = server
            .query_text("SELECT host FROM rcw_ts_twice")
            .await
            .expect("SELECT host");
        assert_eq!(
            hosts.len(),
            3,
            "restore round {round} holds each backed-up row once: {hosts:?}"
        );
    }
}
