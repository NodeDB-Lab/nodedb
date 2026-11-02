// SPDX-License-Identifier: BUSL-1.1

//! RESTORE of a truncated timeseries collection keeps its history.
//!
//! A TRUNCATE ends every partition of the collection. A backup keeps the
//! ended partitions, and a RESTORE installs them as history:
//!
//! - an `AS OF SYSTEM TIME` read before the truncate reads the old rows;
//! - a current read sees none;
//! - `LAST_VALUES` answers as before the backup;
//! - a second RESTORE of the same backup changes nothing.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::SinkExt;

use crate::harness::TestServer;

const TENANT: u64 = 1;
const COLLECTION: &str = "rth_metrics";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Restore `envelope` into `tenant` with `FORCE`: the server holds writes
/// newer than the backup after the first round.
async fn push_restore_force(client: &tokio_postgres::Client, envelope: Vec<u8>) {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN FORCE"))
        .await
        .expect("copy_in: RESTORE TENANT FORCE");
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .expect("send backup bytes");
    sink.as_mut().finish().await.expect("finish RESTORE");
}

/// Every first-column value `sql` returns, sorted.
async fn sorted(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.sort();
    rows
}

/// Every row of `LAST_VALUES`, each column joined, sorted.
async fn last_values(server: &TestServer) -> Vec<String> {
    let mut rows: Vec<String> = server
        .client
        .simple_query(&format!("SELECT LAST_VALUES('{COLLECTION}')"))
        .await
        .expect("LAST_VALUES")
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or_default().to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect();
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_timeseries_restores_its_history() {
    // A one-byte memtable budget flushes every row into a partition, so the
    // TRUNCATE ends partitions the backup keeps.
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    for sql in [
        format!(
            "CREATE COLLECTION {COLLECTION} (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ),
        format!("ALTER COLLECTION {COLLECTION} SET LAST_VALUE_CACHE = TRUE"),
        format!("INSERT INTO {COLLECTION} (ts, host, value) VALUES (1000, 'a', 1.5)"),
        format!("INSERT INTO {COLLECTION} (ts, host, value) VALUES (2000, 'b', 2.5)"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    // Margins on both sides of `before` keep millisecond rounding out.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    server
        .exec(&format!("TRUNCATE {COLLECTION}"))
        .await
        .expect("TRUNCATE");

    let as_of = format!("SELECT host FROM {COLLECTION} AS OF SYSTEM TIME {before}");
    let current = format!("SELECT host FROM {COLLECTION}");
    assert_eq!(
        sorted(&server, &as_of).await,
        ["a", "b"],
        "history before the backup"
    );
    assert!(sorted(&server, &current).await.is_empty());
    let last_before = last_values(&server).await;

    let envelope = super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT");
    server
        .exec(&format!("DROP COLLECTION {COLLECTION} PURGE"))
        .await
        .expect("DROP COLLECTION PURGE");

    for round in 0..2 {
        push_restore_force(&server.client, envelope.clone()).await;
        assert_eq!(
            sorted(&server, &as_of).await,
            ["a", "b"],
            "round {round}: a read before the truncate reads the old rows once"
        );
        assert!(
            sorted(&server, &current).await.is_empty(),
            "round {round}: a current read sees no truncated row"
        );
        assert_eq!(
            last_values(&server).await,
            last_before,
            "round {round}: LAST_VALUES answers as before the backup"
        );
    }
}
