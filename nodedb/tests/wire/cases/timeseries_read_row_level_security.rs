// SPDX-License-Identifier: BUSL-1.1

//! Row-level security on timeseries reads.
//!
//! A `FOR READ` policy on a timeseries collection governs every row a
//! `SELECT` returns, from the live memtable and from flushed partitions
//! alike. The raw scan applies the policy after time-range pruning and the
//! query's own WHERE, before computed columns, sort, and limit, so a row the
//! policy excludes never reaches the client, never consumes a `LIMIT` slot,
//! and never joins a partner. The aggregate path pushes the policy into the
//! grouped scan, so an excluded row never reaches an accumulator.

use crate::harness::TestServer;

const PASSWORD: &str = "ts-read-rls-secret-7";

/// Epoch-millisecond time keys, one per seeded row.
const TS_MINE: i64 = 1_700_000_000_000;
const TS_A: i64 = 1_700_000_001_000;
const TS_B: i64 = 1_700_000_002_000;

/// Create a timeseries `collection` holding one row owned by `user` and two
/// owned by someone else, plus `user` with the readwrite role and a read
/// policy admitting only the caller's own rows.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (ts BIGINT TIME_KEY, owner TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (ts, owner, value) in [
        (TS_MINE, user, 1.0),
        (TS_A, "someone_else", 2.0),
        (TS_B, "someone_else", 3.0),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (ts, owner, value) \
                 VALUES ({ts}, '{owner}', {value})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection} row {ts}: {e}"));
    }
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
    server
        .exec(&format!(
            "CREATE RLS POLICY {collection}_owner ON {collection} FOR READ \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create read policy on {collection}: {e}"));
}

/// Run `sql` as `user` and return each row's cells joined by `|`.
async fn rows_as(server: &TestServer, user: &str, sql: &str) -> Vec<String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{user} runs {sql}: {e}"));
    let mut out = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            let mut cells = Vec::new();
            for i in 0..row.len() {
                cells.push(row.get(i).unwrap_or("").to_string());
            }
            out.push(cells.join("|"));
        }
    }
    drop(client);
    handle.abort();
    out
}

/// A `SELECT` over the live memtable returns only the rows the read policy
/// admits for the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_select_returns_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_reader";
    seed(&server, "ts_rls_read", user).await;

    let rows = rows_as(
        &server,
        user,
        "SELECT ts, owner FROM ts_rls_read ORDER BY ts",
    )
    .await;
    assert_eq!(
        rows,
        vec![format!("{TS_MINE}|{user}")],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `SELECT` over flushed partitions returns only the rows the read policy
/// admits for the caller. The policy applies inside the partition reader,
/// so a flushed row is governed the same as a live one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_select_over_flushed_partitions_returns_only_policy_admitted_rows() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    let user = "ts_rls_flushed_reader";
    seed(&server, "ts_rls_read_flushed", user).await;

    let rows = rows_as(
        &server,
        user,
        "SELECT ts, owner FROM ts_rls_read_flushed ORDER BY ts",
    )
    .await;
    assert_eq!(
        rows,
        vec![format!("{TS_MINE}|{user}")],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `LIMIT` counts admitted rows only: with one admitted row and two
/// excluded rows seeded, `LIMIT 1` returns the admitted row, never an
/// excluded row that happened to be scanned first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_select_with_a_limit_counts_only_admitted_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_limiter";
    seed(&server, "ts_rls_limit", user).await;

    let rows = rows_as(
        &server,
        user,
        "SELECT ts, owner FROM ts_rls_limit ORDER BY ts LIMIT 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![format!("{TS_MINE}|{user}")],
        "the limit applies to admitted rows only: {rows:?}"
    );
}

/// A `COUNT(*)` over a governed timeseries collection leaves the metadata
/// fast path and counts only the rows the read policy admits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_count_counts_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_counter";
    seed(&server, "ts_rls_count", user).await;

    let rows = rows_as(&server, user, "SELECT COUNT(*) FROM ts_rls_count").await;
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `time_bucket` aggregate counts only admitted rows: the three seeded
/// rows share one hourly bucket, and the bucket counts one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_time_bucket_aggregate_counts_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_bucketer";
    seed(&server, "ts_rls_bucket", user).await;

    let rows = rows_as(
        &server,
        user,
        "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) FROM ts_rls_bucket GROUP BY b",
    )
    .await;
    assert_eq!(rows.len(), 1, "one bucket holds every seeded row: {rows:?}");
    assert!(
        rows[0].ends_with("|1"),
        "the bucket counts the one admitted row: {rows:?}"
    );
}

/// A `GROUP BY` aggregate groups only admitted rows: the excluded owner's
/// group never appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeseries_group_by_aggregate_groups_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_grouper";
    seed(&server, "ts_rls_group", user).await;

    let rows = rows_as(
        &server,
        user,
        "SELECT owner, AVG(value) FROM ts_rls_group GROUP BY owner",
    )
    .await;
    assert_eq!(rows.len(), 1, "one group is admitted: {rows:?}");
    assert!(
        rows[0].starts_with(&format!("{user}|")),
        "the admitted group is the caller's own: {rows:?}"
    );
}

/// A join reads the governed timeseries side on the caller's behalf, so its
/// policy applies to that side before the join: excluded rows neither match
/// a partner nor reach the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_over_a_governed_timeseries_collection_excludes_policy_filtered_rows() {
    let server = TestServer::start().await;
    let user = "ts_rls_joiner";
    seed(&server, "ts_rls_join_t", user).await;
    server
        .exec(
            "CREATE COLLECTION ts_rls_join_d (id TEXT PRIMARY KEY, tag TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create document side");
    server
        .exec(&format!(
            "INSERT INTO ts_rls_join_d (id, tag) VALUES \
             ('{user}', 't_mine'), ('someone_else', 't_theirs')"
        ))
        .await
        .expect("seed document side");

    let rows = rows_as(
        &server,
        user,
        "SELECT t.ts, d.tag FROM ts_rls_join_t t \
         JOIN ts_rls_join_d d ON t.owner = d.id ORDER BY t.ts",
    )
    .await;
    assert_eq!(
        rows,
        vec![format!("{TS_MINE}|t_mine")],
        "the join surfaced timeseries rows the read policy excludes: {rows:?}"
    );
}
