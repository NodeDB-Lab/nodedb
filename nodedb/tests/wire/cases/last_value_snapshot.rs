// SPDX-License-Identifier: BUSL-1.1

//! `LAST_VALUES` inside a transaction block reads the block's snapshot.
//!
//! A value another session commits after the block's first read stays
//! invisible to the block's later reads. The statement takes the block's
//! snapshot like any other read of the block, and answers from the version
//! of each series current at it.
//!
//! The cache is built from the stored rows, so it answers after a restart
//! as it did before.

use crate::harness::TestServer;

/// `(timestamp_ms, value)` of every row of `LAST_VALUES`, without the
/// series id.
fn values(rows: &[(String, String, String)]) -> Vec<(String, String)> {
    let mut values: Vec<(String, String)> = rows
        .iter()
        .map(|(_, ts, value)| (ts.clone(), value.clone()))
        .collect();
    values.sort();
    values
}

fn row(ts: &str, value: &str) -> Vec<(String, String)> {
    vec![(ts.to_string(), value.to_string())]
}

/// Create collection `c` with its last-value cache on. `host` is a
/// `SYMBOL` column: a series dimension.
async fn create_cached(client: &tokio_postgres::Client, c: &str) {
    create_cached_with_host(client, c, "SYMBOL").await;
}

/// Create collection `c` with its last-value cache on and `host` declared
/// as `host_type`.
async fn create_cached_with_host(client: &tokio_postgres::Client, c: &str, host_type: &str) {
    exec(
        client,
        &format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, host {host_type}, value FLOAT) \
             WITH (engine='timeseries')"
        ),
    )
    .await;
    exec(
        client,
        &format!("ALTER COLLECTION {c} SET LAST_VALUE_CACHE = TRUE"),
    )
    .await;
}

const PASSWORD: &str = "last-value-snapshot-2";

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `(series_id, timestamp_ms, value)` of every row `sql` returns, sorted.
async fn last_values(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, String, String)> {
    let mut rows: Vec<(String, String, String)> = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_string(),
                row.get(1).unwrap_or_default().to_string(),
                row.get(2).unwrap_or_default().to_string(),
            )),
            _ => None,
        })
        .collect();
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_reads_last_values_at_its_snapshot() {
    let server = TestServer::start().await;
    create_cached(&server.client, "lv_snap").await;
    exec(
        &server.client,
        "INSERT INTO lv_snap (ts, host, value) VALUES (1000, 'a', 1.5)",
    )
    .await;
    server
        .exec(&format!("CREATE USER lv_writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO lv_writer")
        .await
        .expect("GRANT ROLE");
    let (writer, handle) = server
        .connect_as("lv_writer", PASSWORD)
        .await
        .expect("second connection");

    let reader = &server.client;
    let read = "SELECT LAST_VALUES('lv_snap')";
    exec(reader, "BEGIN").await;
    let first = last_values(reader, read).await;
    assert_eq!(values(&first), row("1000", "1.500000"), "{first:?}");

    exec(
        &writer,
        "INSERT INTO lv_snap (ts, host, value) VALUES (2000, 'a', 2.5)",
    )
    .await;

    assert_eq!(
        last_values(reader, read).await,
        first,
        "the block reads its snapshot, not the value committed after it"
    );
    exec(reader, "COMMIT").await;
    assert_eq!(
        values(&last_values(reader, read).await),
        row("2000", "2.500000"),
        "a read after COMMIT sees the new value"
    );
    drop(writer);
    handle.abort();
}

/// The cache answers after a kill: a restart rebuilds it from the flushed
/// partitions and the replayed WAL, with the series ids it gave before.
///
/// Host `a`'s newest value lands first, so the flush puts it in a partition
/// before host `b` and an older `a` row land: after the kill, `a`'s answer
/// comes from the partition and `b`'s from a replayed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cache_answers_after_a_kill() {
    // A one-byte memtable budget flushes each value into a partition before
    // the next row lands. A kill keeps no memtable, so the rebuild reads the
    // partitions and the rows the WAL replays.
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    create_cached(&server.client, "lv_kill").await;
    for (ts, host, value) in [(1000, "a", 1.5), (3000, "b", 4.5), (500, "a", 0.5)] {
        exec(
            &server.client,
            &format!("INSERT INTO lv_kill (ts, host, value) VALUES ({ts}, '{host}', {value})"),
        )
        .await;
    }
    let read = "SELECT LAST_VALUES('lv_kill')";
    let before = last_values(&server.client, read).await;
    assert_eq!(
        values(&before),
        vec![
            ("1000".to_string(), "1.500000".to_string()),
            ("3000".to_string(), "4.500000".to_string()),
        ],
        "one row per host, each its newest value: {before:?}"
    );

    let (server, dir) = server.take_dir();
    drop(server);
    let (server, _dir) = TestServer::open_on_path_with_timeseries_memtable_budget(dir, 1).await;
    let after = last_values(&server.client, read).await;
    assert_eq!(
        after, before,
        "the same series ids and values after the restart"
    );
}

/// A row gets the same series whether SQL or ILP wrote it, and whether ILP
/// spelled its value as a tag or as a string field: the series is the
/// collection's measurement and every `SYMBOL` value the row stores. Four
/// writes over two hosts make exactly two series.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_and_ilp_rows_share_their_series() {
    let server = TestServer::start().await;
    let c = "lv_proto";
    create_cached(&server.client, c).await;
    exec(
        &server.client,
        &format!("INSERT INTO {c} (ts, host, value) VALUES (1000, 'a', 1.5)"),
    )
    .await;
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.ilp_port);
    let mut ilp = nodedb_test_support::ilp_client::connect_trust(addr, "nodedb").await;
    // ILP timestamps are nanoseconds: 2_000_000_000 ns stores as ts 2000.
    for line in [
        format!("{c},host=a value=2.5 2000000000"),
        format!("{c} host=\"b\",value=3.5 3000000000"),
    ] {
        nodedb_test_support::ilp_client::send_line(&mut ilp, &line).await;
    }
    let read = format!("SELECT LAST_VALUES('{c}')");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let rows = values(&last_values(&server.client, &read).await);
        if rows.iter().any(|(ts, _)| ts == "3000") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ILP rows never reached the cache: {rows:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    drop(ilp);
    exec(
        &server.client,
        &format!("INSERT INTO {c} (ts, host, value) VALUES (4000, 'b', 4.5)"),
    )
    .await;
    let rows = last_values(&server.client, &read).await;
    assert_eq!(
        values(&rows),
        vec![
            ("2000".to_string(), "2.500000".to_string()),
            ("4000".to_string(), "4.500000".to_string()),
        ],
        "a SQL row and an ILP line of one host are one series: {rows:?}"
    );
}

/// A `TEXT` column is a field, not a series dimension. Rows of two hosts
/// share one series, so the cache holds the newest row of the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_column_is_not_a_series_dimension() {
    let server = TestServer::start().await;
    let c = "lv_text";
    create_cached_with_host(&server.client, c, "TEXT").await;
    for (ts, host, value) in [(1000, "a", 1.5), (3000, "b", 4.5), (2000, "c", 2.5)] {
        exec(
            &server.client,
            &format!("INSERT INTO {c} (ts, host, value) VALUES ({ts}, '{host}', {value})"),
        )
        .await;
    }
    let rows = last_values(&server.client, &format!("SELECT LAST_VALUES('{c}')")).await;
    assert_eq!(
        values(&rows),
        row("3000", "4.500000"),
        "three text values over one measurement are one series: {rows:?}"
    );
}
