// SPDX-License-Identifier: BUSL-1.1

//! Reads inside a transaction block that match a join target, refine a
//! spatial predicate over a timeseries collection, or probe a KV index read
//! the block's snapshot with the block's staged writes merged on top. A
//! second session's commit after the snapshot stays invisible to the block.

use crate::harness::TestServer;

const PASSWORD: &str = "txn-snapshot-join-3";

async fn writer(
    server: &TestServer,
    user: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .expect("GRANT ROLE");
    server
        .connect_as(user, PASSWORD)
        .await
        .expect("second connection")
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The rows `sql` affects, from its command tag.
async fn affected(client: &tokio_postgres::Client, sql: &str) -> u64 {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no command tag"))
}

/// Each row `sql` returns, its columns joined by `=`.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("="),
            ),
            _ => None,
        })
        .collect()
}

fn owned(expected: &[&str]) -> Vec<String> {
    expected.iter().map(|s| s.to_string()).collect()
}

/// A join target with rows `t1` and `t3`, and a source naming both.
async fn join_pair(server: &TestServer, target: &str, source: &str) {
    let client = &server.client;
    exec(
        client,
        &format!(
            "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, n BIGINT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, bump BIGINT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        client,
        &format!("INSERT INTO {target} (id, n) VALUES ('t1', 0)"),
    )
    .await;
    for (id, bump) in [("t1", 5), ("t2", 7), ("t3", 9)] {
        exec(
            client,
            &format!("INSERT INTO {source} (id, bump) VALUES ('{id}', {bump})"),
        )
        .await;
    }
}

/// `UPDATE ... FROM` inside a block matches the target rows of the block's
/// snapshot and its own staged rows. A target row a second session commits
/// after the snapshot is not matched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_matches_the_block_snapshot() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "ufj_snap_b").await;
    join_pair(&server, "ufj_t", "ufj_s").await;
    let a = &server.client;

    exec(a, "BEGIN").await;
    assert_eq!(
        rows(a, "SELECT n FROM ufj_t WHERE id = 't1'").await,
        owned(&["0"])
    );
    exec(&b, "INSERT INTO ufj_t (id, n) VALUES ('t2', 0)").await;
    exec(a, "INSERT INTO ufj_t (id, n) VALUES ('t3', 0)").await;

    let count = affected(
        a,
        "UPDATE ufj_t SET n = ufj_t.n + ufj_s.bump FROM ufj_s WHERE ufj_t.id = ufj_s.id",
    )
    .await;
    assert_eq!(count, 2, "t1 from the snapshot and t3 the block staged");
    assert_eq!(
        rows(a, "SELECT id, n FROM ufj_t ORDER BY id").await,
        owned(&["t1=5", "t3=9"]),
        "the block sees neither the concurrent row nor a write to it"
    );
    exec(a, "ROLLBACK").await;

    assert_eq!(
        rows(a, "SELECT id, n FROM ufj_t ORDER BY id").await,
        owned(&["t1=0", "t2=0"])
    );
    drop(b);
    handle.abort();
}

/// MERGE inside a block classifies the target rows of the block's snapshot
/// and its own staged rows. A target row a second session deletes after the
/// snapshot still matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_classifies_the_block_snapshot() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "merge_snap_b").await;
    join_pair(&server, "msnap_t", "msnap_s").await;
    exec(&server.client, "DELETE FROM msnap_s WHERE id = 't2'").await;
    let a = &server.client;

    exec(a, "BEGIN").await;
    assert_eq!(
        rows(a, "SELECT n FROM msnap_t WHERE id = 't1'").await,
        owned(&["0"])
    );
    exec(&b, "DELETE FROM msnap_t WHERE id = 't1'").await;
    exec(a, "INSERT INTO msnap_t (id, n) VALUES ('t3', 0)").await;

    exec(
        a,
        "MERGE INTO msnap_t t USING msnap_s s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET n = s.bump \
         WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, -1)",
    )
    .await;
    assert_eq!(
        rows(a, "SELECT id, n FROM msnap_t ORDER BY id").await,
        owned(&["t1=5", "t3=9"]),
        "t1 matches at the snapshot and t3 matches the staged row"
    );
    exec(a, "ROLLBACK").await;

    assert!(
        rows(a, "SELECT id FROM msnap_t WHERE id = 't1'")
            .await
            .is_empty(),
        "the concurrent delete committed"
    );
    drop(b);
    handle.abort();
}

const NEAR: &str = "'{\"type\":\"Point\",\"coordinates\":[101.69,3.14]}'";

async fn ts_point(client: &tokio_postgres::Client, ts: i64, id: &str) {
    exec(
        client,
        &format!("INSERT INTO ts_geo (ts, id, loc) VALUES ({ts}, '{id}', {NEAR})"),
    )
    .await;
}

/// A spatial predicate over a timeseries collection inside a block reads
/// the rows at the block's snapshot and the rows the block staged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spatial_read_over_timeseries_reads_the_block_snapshot() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "ts_geo_b").await;
    let a = &server.client;
    exec(
        a,
        "CREATE COLLECTION ts_geo (ts BIGINT TIME_KEY, id TEXT, loc TEXT) \
         WITH (engine='timeseries')",
    )
    .await;
    ts_point(a, 1000, "r1").await;
    let near = format!("SELECT id FROM ts_geo WHERE ST_DWithin(loc, {NEAR}, 1000) ORDER BY id");

    exec(a, "BEGIN").await;
    assert_eq!(rows(a, &near).await, owned(&["r1"]));
    ts_point(&b, 2000, "r2").await;
    assert_eq!(
        rows(a, &near).await,
        owned(&["r1"]),
        "the concurrent row is past the snapshot"
    );
    ts_point(a, 3000, "r3").await;
    assert_eq!(
        rows(a, &near).await,
        owned(&["r1", "r3"]),
        "the block reads its own staged row"
    );
    exec(a, "ROLLBACK").await;

    assert_eq!(rows(a, &near).await, owned(&["r1", "r2"]));
    drop(b);
    handle.abort();
}

/// The integer a single-cell JSON reply carries under `field`.
fn json_field(text: &str, field: &str) -> Option<i64> {
    let needle = format!("\"{field}\":");
    let rest = text.split_once(&needle)?.1;
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    digits.parse().ok()
}

async fn sorted_count(client: &tokio_postgres::Client, index: &str) -> Option<i64> {
    let replies = rows(client, &format!("SELECT SORTED_COUNT({index})")).await;
    json_field(replies.first()?, "count")
}

async fn kv_row(client: &tokio_postgres::Client, id: &str, score: i64) {
    exec(
        client,
        &format!("INSERT INTO kv_ix (id, region, score) VALUES ('{id}', 'eu', {score})"),
    )
    .await;
}

/// A KV index probe inside a block reads the block's snapshot and its staged
/// writes. A TRUNCATE the block rolls back, and an autocommit TRUNCATE, both
/// leave every secondary and sorted index registered and filled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_indexes_read_the_block_snapshot_and_survive_truncate() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "kv_ix_b").await;
    let a = &server.client;
    exec(
        a,
        "CREATE COLLECTION kv_ix (id STRING PRIMARY KEY, region STRING, score INT) \
         WITH (engine='kv')",
    )
    .await;
    exec(a, "CREATE INDEX ON kv_ix (region)").await;
    exec(
        a,
        "CREATE SORTED INDEX kv_ix_lb ON kv_ix (score DESC) KEY id",
    )
    .await;
    kv_row(a, "a", 10).await;
    let in_eu = "SELECT id FROM kv_ix WHERE region = 'eu' ORDER BY id";

    exec(a, "BEGIN").await;
    assert_eq!(rows(a, in_eu).await, owned(&["a"]));
    kv_row(&b, "b", 20).await;
    assert_eq!(
        rows(a, in_eu).await,
        owned(&["a"]),
        "the concurrent row is past the snapshot"
    );
    assert_eq!(sorted_count(a, "kv_ix_lb").await, Some(1));
    kv_row(a, "c", 30).await;
    assert_eq!(rows(a, in_eu).await, owned(&["a", "c"]));
    assert_eq!(sorted_count(a, "kv_ix_lb").await, Some(2));
    exec(a, "TRUNCATE kv_ix").await;
    assert!(
        rows(a, in_eu).await.is_empty(),
        "the staged truncate hides every row"
    );
    exec(a, "ROLLBACK").await;

    assert_eq!(rows(a, in_eu).await, owned(&["a", "b"]));
    assert_eq!(sorted_count(a, "kv_ix_lb").await, Some(2));

    exec(a, "TRUNCATE kv_ix").await;
    assert!(rows(a, in_eu).await.is_empty());
    assert_eq!(
        sorted_count(a, "kv_ix_lb").await,
        Some(0),
        "the sorted index stays registered after TRUNCATE"
    );
    kv_row(a, "d", 5).await;
    assert_eq!(rows(a, in_eu).await, owned(&["d"]));
    assert_eq!(
        sorted_count(a, "kv_ix_lb").await,
        Some(1),
        "a write after TRUNCATE files into the kept sorted index"
    );
    drop(b);
    handle.abort();
}
