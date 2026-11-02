// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate read inside a transaction block reads the block's
//! snapshot.
//!
//! A row another session commits after the block's first read folds into
//! the aggregate's buckets. The block's later reads of the aggregate return
//! what the first read returned: a bucket holding a row newer than the
//! snapshot is rebuilt from the raw rows visible at the snapshot. After
//! COMMIT, a read sees the new row.

use std::time::Duration;

use crate::harness::TestServer;

const PASSWORD: &str = "cagg-snapshot-5";

/// A memtable budget of one byte flushes every ingest to a partition, and a
/// flush is what an `OnFlush` aggregate folds.
const FLUSH_EVERY_INGEST: usize = 1;

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `(host, s, c)` of every row `sql` returns, sorted by host.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, f64, i64)> {
    let mut rows: Vec<(String, f64, i64)> = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_string(),
                row.get(1)
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_else(|e| panic!("s is not a number: {e}")),
                row.get(2)
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_else(|e| panic!("c is not a number: {e}")),
            )),
            _ => None,
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Rows of `sql`, polled until they equal `want`: the flush that feeds an
/// `OnFlush` aggregate runs on the core after the insert answers.
async fn rows_when(
    client: &tokio_postgres::Client,
    sql: &str,
    want: &[(String, f64, i64)],
) -> Vec<(String, f64, i64)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = rows(client, sql).await;
        if got == want || tokio::time::Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn row(host: &str, s: f64, c: i64) -> (String, f64, i64) {
    (host.to_string(), s, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_reads_the_aggregate_at_its_snapshot() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    exec(
        &server.client,
        "CREATE COLLECTION cagg_snap_src (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;
    exec(
        &server.client,
        "CREATE CONTINUOUS AGGREGATE cagg_snap_view ON cagg_snap_src BUCKET '1m' \
         AGGREGATE SUM(value) AS s, COUNT(*) AS c GROUP BY host",
    )
    .await;
    for (ts, host, value) in [(1_000, "a", 1.5), (2_000, "a", 2.5), (3_000, "b", 10.0)] {
        exec(
            &server.client,
            &format!(
                "INSERT INTO cagg_snap_src (ts, host, value) VALUES ({ts}, '{host}', {value})"
            ),
        )
        .await;
    }
    let read = "SELECT host, s, c FROM cagg_snap_view";
    let seeded = vec![row("a", 4.0, 2), row("b", 10.0, 1)];
    assert_eq!(rows_when(&server.client, read, &seeded).await, seeded);

    server
        .exec(&format!("CREATE USER cagg_writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO cagg_writer")
        .await
        .expect("GRANT ROLE");
    let (writer, handle) = server
        .connect_as("cagg_writer", PASSWORD)
        .await
        .expect("second connection");

    let reader = &server.client;
    exec(reader, "BEGIN").await;
    assert_eq!(rows(reader, read).await, seeded);

    // Same minute and host as the seeded rows: the commit changes bucket
    // `a` of the first minute once its flush folds.
    exec(
        &writer,
        "INSERT INTO cagg_snap_src (ts, host, value) VALUES (5000, 'a', 100)",
    )
    .await;
    let committed = vec![row("a", 104.0, 3), row("b", 10.0, 1)];
    // Outside the block the new row shows once its flush folds.
    assert_eq!(rows_when(&writer, read, &committed).await, committed);

    assert_eq!(
        rows(reader, read).await,
        seeded,
        "the block reads its snapshot, not the row committed after it"
    );
    exec(reader, "COMMIT").await;
    assert_eq!(
        rows(reader, read).await,
        committed,
        "a read after COMMIT sees the new row"
    );
    drop(writer);
    handle.abort();
}
