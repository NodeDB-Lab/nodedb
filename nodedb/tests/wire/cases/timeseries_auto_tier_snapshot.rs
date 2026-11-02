// SPDX-License-Identifier: BUSL-1.1

//! AUTO_TIER reads inside a transaction block read the block's snapshot.
//!
//! A tier bucket folds every flushed row of its range, also rows committed
//! after the block's snapshot. A read at the snapshot uses a tier only while
//! no row it folds is newer than the snapshot. Otherwise the read takes the
//! raw partitions visible at the snapshot.

use crate::harness::TestServer;

const PASSWORD: &str = "tier-snapshot-3";

/// A collection whose rows sit far older than the one-day raw window,
/// under an AUTO_TIER policy with a one-minute tier.
///
/// No retention pass runs while the test does. The server's retention loop
/// waits ten seconds, then sleeps the shortest evaluation interval of the
/// enabled policies, or one hour when none is enabled yet, before its first
/// pass. This server holds one policy, evaluated every seven days, so the
/// first pass comes an hour or more after boot, and the raw partitions stay
/// readable.
async fn seed(server: &TestServer, c: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ),
        format!(
            "CREATE RETENTION POLICY {c}_policy ON {c} (\
             RAW RETAIN '1 day', \
             DOWNSAMPLE TO '1 minute' AGGREGATE (SUM(v), MIN(v), MAX(v), COUNT(*)) \
             RETAIN 'forever') WITH (EVAL_INTERVAL = '7 days')"
        ),
        format!("ALTER RETENTION POLICY {c}_policy ON {c} SET AUTO_TIER = TRUE"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for (ts, host, v) in [(1_000, "a", 1.0), (2_000, "b", 2.0), (61_000, "a", 6.0)] {
        server
            .exec(&format!(
                "INSERT INTO {c} (ts, host, v) VALUES ({ts}, '{host}', {v})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {c} row {ts}: {e}"));
    }
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

/// `COUNT(*)` and `SUM(v)` of the one row `sql` returns.
async fn count_sum(client: &tokio_postgres::Client, sql: &str) -> (String, f64) {
    let rows: Vec<(String, f64)> = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_string(),
                row.get(1)
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_else(|e| panic!("SUM is not a number: {e}")),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "one row: {rows:?}");
    rows[0].clone()
}

/// A row ingested and committed by another session after the block's first
/// read folds into the tier. The block's next read over the tiered range
/// returns what it returned before. After COMMIT, a read sees the new row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_reads_the_tiered_range_at_its_snapshot() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "tier_snap").await;
    server
        .exec(&format!("CREATE USER tier_writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO tier_writer")
        .await
        .expect("GRANT ROLE");
    let (writer, handle) = server
        .connect_as("tier_writer", PASSWORD)
        .await
        .expect("second connection");

    let reader = &server.client;
    let read = "SELECT COUNT(*), SUM(v) FROM tier_snap";
    exec(reader, "BEGIN").await;
    assert_eq!(count_sum(reader, read).await, ("3".to_string(), 9.0));

    exec(
        &writer,
        "INSERT INTO tier_snap (ts, host, v) VALUES (3000, 'b', 10)",
    )
    .await;

    assert_eq!(
        count_sum(reader, read).await,
        ("3".to_string(), 9.0),
        "the block reads its snapshot, not the row committed after it"
    );
    exec(reader, "COMMIT").await;

    assert_eq!(
        count_sum(reader, read).await,
        ("4".to_string(), 19.0),
        "a read after COMMIT sees the new row"
    );
    drop(writer);
    handle.abort();
}
