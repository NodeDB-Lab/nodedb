// SPDX-License-Identifier: BUSL-1.1

//! Row-level security on columnar reads.
//!
//! A `FOR READ` policy on a columnar collection governs every row a `SELECT`
//! returns, from the live memtable and from flushed segments alike. The scan
//! applies the policy after block pruning and the query's own WHERE, before
//! projection, sort, and limit, so a row the policy excludes never reaches
//! the client, never consumes a `LIMIT` slot, and never joins a partner.
//! A spatial collection runs the same scan and is governed the same way.

use crate::harness::TestServer;

const PASSWORD: &str = "probe-secret-99";

/// Create a columnar `collection` holding one row owned by `user` and two
/// owned by someone else, plus `user` with the readwrite role.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, owner, note) VALUES \
             ('r_mine', '{user}', 'mine'), \
             ('r_a', 'someone_else', 'theirs'), \
             ('r_b', 'someone_else', 'theirs too')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
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
async fn a_columnar_select_returns_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    seed(&server, "col_rls_read", "col_rls_reader").await;

    let rows = rows_as(
        &server,
        "col_rls_reader",
        "SELECT id, note FROM col_rls_read ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec!["r_mine|mine".to_string()],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `SELECT` over flushed segments returns only the rows the read policy
/// admits for the caller. The policy applies after block pruning, so a
/// flushed row is governed the same as a live one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_columnar_select_over_flushed_segments_returns_only_policy_admitted_rows() {
    let server = TestServer::start_with_columnar_flush_threshold(2).await;
    seed(&server, "col_rls_read_flushed", "col_rls_flushed_reader").await;

    let rows = rows_as(
        &server,
        "col_rls_flushed_reader",
        "SELECT id, note FROM col_rls_read_flushed ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec!["r_mine|mine".to_string()],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `COUNT(*)` over a governed columnar collection counts only the rows the
/// read policy admits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_columnar_count_counts_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    seed(&server, "col_rls_count", "col_rls_counter").await;

    let rows = rows_as(
        &server,
        "col_rls_counter",
        "SELECT COUNT(*) FROM col_rls_count",
    )
    .await;
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// A `LIMIT` counts admitted rows only: with one admitted row and two
/// excluded rows seeded, `LIMIT 1` returns the admitted row, never an
/// excluded row that happened to be scanned first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_columnar_select_with_a_limit_counts_only_admitted_rows() {
    let server = TestServer::start().await;
    seed(&server, "col_rls_limit", "col_rls_limiter").await;

    let rows = rows_as(
        &server,
        "col_rls_limiter",
        "SELECT id FROM col_rls_limit ORDER BY id LIMIT 1",
    )
    .await;
    assert_eq!(
        rows,
        vec!["r_mine".to_string()],
        "the limit applies to admitted rows only: {rows:?}"
    );
}

/// A join reads the governed columnar side on the caller's behalf, so its
/// policy applies to that side before the join: excluded rows neither match
/// a partner nor reach the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_over_a_governed_columnar_collection_excludes_policy_filtered_rows() {
    let server = TestServer::start().await;
    seed(&server, "col_rls_join_c", "col_rls_joiner").await;
    server
        .exec(
            "CREATE COLLECTION col_rls_join_d (id TEXT PRIMARY KEY, tag TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create document side");
    server
        .exec(
            "INSERT INTO col_rls_join_d (id, tag) VALUES \
             ('r_mine', 't_mine'), ('r_a', 't_a'), ('r_b', 't_b')",
        )
        .await
        .expect("seed document side");

    let rows = rows_as(
        &server,
        "col_rls_joiner",
        "SELECT c.id, d.tag FROM col_rls_join_c c \
         JOIN col_rls_join_d d ON c.id = d.id ORDER BY c.id",
    )
    .await;
    assert_eq!(
        rows,
        vec!["r_mine|t_mine".to_string()],
        "the join surfaced columnar rows the read policy excludes: {rows:?}"
    );
}

/// A plain `SELECT` over a spatial collection runs the same columnar scan,
/// so the read policy governs it the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spatial_select_returns_only_policy_admitted_rows() {
    let server = TestServer::start().await;
    let (collection, user) = ("sp_rls_read", "sp_rls_reader");
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             COLUMNS (id TEXT, owner TEXT, loc GEOMETRY) \
             WITH (engine='spatial')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, owner, wkt) in [
        ("r_mine", user, "POINT(1 1)"),
        ("r_a", "someone_else", "POINT(2 2)"),
        ("r_b", "someone_else", "POINT(3 3)"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, loc) \
                 VALUES ('{id}', '{owner}', ST_GeomFromText('{wkt}'))"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection} row {id}: {e}"));
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

    let rows = rows_as(
        &server,
        user,
        &format!("SELECT id FROM {collection} ORDER BY id"),
    )
    .await;
    assert_eq!(
        rows,
        vec!["r_mine".to_string()],
        "the read policy admits one row for this caller: {rows:?}"
    );
}

/// `2020-03-05T10:00:00Z` as epoch milliseconds: the instant the policies
/// below compare against.
const CUTOFF_MS: i64 = 1_583_402_400_000;
const CUTOFF_TEXT: &str = "2020-03-05 10:00:00";

/// Create a columnar `collection` with a declared `TIMESTAMP` column holding
/// one row before, one at, and one after the cutoff, plus `user` with the
/// readwrite role. No policy: each test creates its own.
async fn seed_instant(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, captured_at TIMESTAMP) \
             WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, owner, captured_at) VALUES \
             ('before', '{user}', '2020-03-05 09:00:00'), \
             ('at', '{user}', '{CUTOFF_TEXT}'), \
             ('after', '{user}', '2020-03-05 11:00:00')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// A numeric policy literal compared against a declared `TIMESTAMP` column
/// is epoch milliseconds, typed at `CREATE RLS POLICY` by the rule a query
/// predicate follows: the governed user sees the rows at and after the
/// cutoff, and the row before it is excluded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_numeric_policy_literal_on_a_timestamp_column_is_an_instant() {
    let server = TestServer::start().await;
    let user = "col_rls_ms_reader";
    seed_instant(&server, "col_rls_ms", user).await;
    server
        .exec(&format!(
            "CREATE RLS POLICY col_rls_ms_recent ON col_rls_ms FOR READ \
             USING (captured_at >= {CUTOFF_MS})"
        ))
        .await
        .expect("a numeric literal on a TIMESTAMP column is epoch milliseconds");

    let rows = rows_as(
        &server,
        user,
        "SELECT id FROM col_rls_ms ORDER BY captured_at",
    )
    .await;
    assert_eq!(
        rows,
        vec!["at".to_string(), "after".to_string()],
        "the policy admits the rows at and after the cutoff: {rows:?}"
    );
}

/// A text policy literal compared against a declared `TIMESTAMP` column is
/// parsed as ISO-8601 and enforced as the same instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_policy_literal_on_a_timestamp_column_is_an_instant() {
    let server = TestServer::start().await;
    let user = "col_rls_text_reader";
    seed_instant(&server, "col_rls_text", user).await;
    server
        .exec(&format!(
            "CREATE RLS POLICY col_rls_text_recent ON col_rls_text FOR READ \
             USING (captured_at >= '{CUTOFF_TEXT}')"
        ))
        .await
        .expect("a text literal on a TIMESTAMP column is parsed as an instant");

    let rows = rows_as(
        &server,
        user,
        "SELECT id FROM col_rls_text ORDER BY captured_at",
    )
    .await;
    assert_eq!(
        rows,
        vec!["at".to_string(), "after".to_string()],
        "the policy admits the rows at and after the cutoff: {rows:?}"
    );
}

/// A policy literal no instant can be read from is refused at
/// `CREATE RLS POLICY`, naming the column, so an unenforceable policy is
/// never stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_instant_policy_literal_on_a_timestamp_column_is_refused() {
    let server = TestServer::start().await;
    seed_instant(&server, "col_rls_bool", "col_rls_bool_reader").await;
    let error = server
        .exec(
            "CREATE RLS POLICY col_rls_bool_bad ON col_rls_bool FOR READ \
             USING (captured_at >= true)",
        )
        .await
        .expect_err("a boolean is not an instant");
    assert!(
        error.contains("captured_at"),
        "the refusal must name the column: {error}"
    );
}
