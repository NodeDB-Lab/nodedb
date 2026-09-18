// SPDX-License-Identifier: BUSL-1.1

//! Pins the `CommandComplete` tag contract every DML statement must honour:
//! ONE statement answers with ONE tag, the tag is a Postgres command tag
//! (`INSERT 0 n` / `UPDATE n` / `DELETE n`), and `n` is the number of rows the
//! statement affected — whatever engine the collection runs on and however
//! many Data-Plane tasks the statement planned to.
//!
//! Drivers turn the tag into `rowcount` / `rows_affected`, and ORMs turn that
//! into "did my write land?" — batch-load accounting and optimistic-locking
//! checks both read it. A statement that answers with several tags makes the
//! driver report the LAST one (or the first, depending on the driver); a bare
//! `OK` tag has no count to parse at all.
//!
//! `tokio_postgres::SimpleQueryMessage::CommandComplete` exposes only a
//! `u64`, derived by `tokio_postgres::query::extract_row_affected` as
//! `tag.rsplit(' ').next()` — the LAST whitespace-separated token, parsed as
//! an integer (falling back to `0` if that fails). This makes the cases
//! below observable:
//!
//! - a bare tag (no trailing integer, e.g. `OK`) always parses to `0`;
//! - a tag with a trailing integer parses to that integer regardless of how
//!   many tokens precede it;
//! - every `CommandComplete` in a simple-query response is surfaced, so a
//!   statement that answers with several tags is countable;
//! - `Client::execute` (extended query) reads to `ReadyForQuery` and returns
//!   the LAST tag's count, so a per-row tag sequence reports the final row's
//!   `1` for the whole statement.
//!
//! It does NOT make the `INSERT` OID observable: `INSERT 1` (malformed, oid
//! omitted) and `INSERT 0 1` (correct) both end in `1`. Distinguishing those
//! requires the raw tag string, which `tokio_postgres` never surfaces.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Every `CommandComplete` count in `sql`'s simple-query response, in wire
/// order, plus the number of `Row` messages that arrived alongside them.
async fn command_tags(server: &TestServer, sql: &str) -> (Vec<u64>, usize) {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    let mut tags = Vec::new();
    let mut rows = 0;
    for m in messages {
        match m {
            SimpleQueryMessage::CommandComplete(n) => tags.push(n),
            SimpleQueryMessage::Row(_) => rows += 1,
            _ => {}
        }
    }
    (tags, rows)
}

/// The row count carried by the first `CommandComplete` in `sql`'s response.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let (tags, _) = command_tags(server, sql).await;
    tags.first()
        .copied()
        .unwrap_or_else(|| panic!("statement reported no command tag: {sql}"))
}

/// Assert that ONE DML statement answered with exactly one command tag whose
/// count is `expected`, and that no row data rode along with it.
async fn assert_single_tag(server: &TestServer, sql: &str, expected: u64) {
    let (tags, rows) = command_tags(server, sql).await;
    assert_eq!(
        tags.len(),
        1,
        "one statement must answer with exactly one CommandComplete, got {tags:?} for: {sql}"
    );
    assert_eq!(
        tags[0], expected,
        "command tag must carry the statement's affected-row count for: {sql}"
    );
    assert_eq!(
        rows, 0,
        "a DML statement without RETURNING must not answer with row data for: {sql}"
    );
}

/// Number of rows a `SELECT count(*)` reports — the observable state the
/// affected count must agree with.
async fn live_rows(server: &TestServer, sql: &str) -> u64 {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("count query should succeed: {sql}: {e}"));
    rows.first()
        .unwrap_or_else(|| panic!("count query returned no row: {sql}"))
        .parse()
        .unwrap_or_else(|e| panic!("count query returned a non-integer: {sql}: {e}"))
}

/// `INSERT INTO t { ... }` (the object-literal insert path,
/// distinct from `INSERT ... VALUES`) must not return `DdlResult::Status {
/// rows_affected: None, .. }`, which pgwire renders as a bare `INSERT` tag.
/// `extract_row_affected` falls back to `0` for a tag with no trailing
/// integer, so returning `None` would misreport a real single-row insert as
/// touching 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_reports_one_row_affected() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_conformance_obj_insert \
             (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    let count = affected(
        &server,
        "INSERT INTO tag_conformance_obj_insert { id: 'row1', v: 'hello' }",
    )
    .await;
    assert_eq!(
        count, 1,
        "object-literal insert must report 1 affected row, not fall back to 0 \
         via a bare CommandComplete tag"
    );
}

/// `TRUNCATE TABLE` must not render `TRUNCATE <rows-removed>` — a
/// count Postgres's `TRUNCATE TABLE` tag never carries. Rendering it that way
/// would report the pre-truncate row count instead of falling back to
/// `0` for the count-less tag a real Postgres server sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_table_reports_no_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_conformance_truncate \
             (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for id in ["a", "b", "c"] {
        server
            .exec(&format!(
                "INSERT INTO tag_conformance_truncate (id, v) VALUES ('{id}', 'x')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }

    let count = affected(&server, "TRUNCATE TABLE tag_conformance_truncate").await;
    assert_eq!(
        count, 0,
        "TRUNCATE TABLE's tag carries no row count, so the driver's fallback \
         parse must read 0 — not the number of rows actually removed"
    );
}

/// A multi-row `INSERT ... VALUES (...), (...), (...)` on a schemaless
/// document collection is one statement and answers with one `INSERT 0 3`,
/// not one `INSERT 0 1` per value tuple. The planner lowers the statement to
/// one task per row; that fan-out is an execution detail the wire must not
/// expose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_multi_doc (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_multi_doc (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        3,
    )
    .await;
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM tag_multi_doc").await,
        3,
        "all three rows must have landed"
    );
}

/// The extended-query path (`Parse`/`Bind`/`Execute`, what psycopg2 and every
/// ORM use) reports the statement's total: `Client::execute` returns 3 for a
/// 3-row insert. A per-row tag sequence makes the driver return the last
/// tag's `1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_extended_query_reports_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_multi_ext (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    let affected = server
        .client
        .execute(
            "INSERT INTO tag_multi_ext (id, v) VALUES (10, 'x'), (11, 'y'), (12, 'z')",
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("extended-query insert: {e:?}"));
    assert_eq!(
        affected, 3,
        "extended-query rows_affected must be the statement total, not the last row's 1"
    );
}

/// The strict document engine takes the same per-row lowering and owes the
/// same single `INSERT 0 3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_multi_strict (id INT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_multi_strict (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        3,
    )
    .await;
}

/// `INSERT ... ON CONFLICT DO NOTHING` over several rows reports how many
/// rows were actually written: with one key already present, `INSERT 0 2`.
/// The count is the sum of per-row outcomes, so a fold that assumes one row
/// per task would report 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_on_conflict_do_nothing_reports_rows_written() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_multi_conflict (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    server
        .exec("INSERT INTO tag_multi_conflict (id, v) VALUES (2, 'existing')")
        .await
        .unwrap_or_else(|e| panic!("seed: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_multi_conflict (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c') \
         ON CONFLICT DO NOTHING",
        2,
    )
    .await;
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM tag_multi_conflict").await,
        3,
        "the two new rows must have landed beside the existing one"
    );
}

/// Inside an explicit transaction a multi-row insert is still one statement:
/// `INSERT 0 3` at statement time, exactly as Postgres reports it. The staged
/// (statement-time) write path must fold its per-row tags the same way the
/// autocommit path does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_transaction_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_multi_txn (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    server
        .exec("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("begin: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_multi_txn (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        3,
    )
    .await;

    server
        .exec("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("commit: {e}"));
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM tag_multi_txn").await,
        3,
        "all three rows must be visible after COMMIT"
    );
}

/// Two statements in one simple-query buffer answer with two tags, one per
/// statement, in order. Folding must stop at the statement boundary — a fold
/// over the whole query buffer would collapse `1` and `2` into one `3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_statement_query_reports_one_tag_per_statement() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_multi_stmt (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    let (tags, _) = command_tags(
        &server,
        "INSERT INTO tag_multi_stmt (id, v) VALUES (1, 'a'); \
         INSERT INTO tag_multi_stmt (id, v) VALUES (2, 'b'), (3, 'c')",
    )
    .await;
    assert_eq!(
        tags,
        vec![1, 2],
        "each statement in the buffer answers with its own tag and its own count"
    );
}

/// A key-value `INSERT` answers `INSERT 0 1`, a Postgres command tag a
/// driver can read a count from — not a bare `OK`, which parses to `0` and
/// tells an ORM the write did not land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_reports_insert_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_kv (k TEXT PRIMARY KEY, v TEXT) WITH (engine='kv')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(&server, "INSERT INTO tag_kv (k, v) VALUES ('a', '1')", 1).await;
}

/// A multi-row key-value `INSERT` is one statement: one `INSERT 0 3`, not
/// three tags of any kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_kv_multi (k TEXT PRIMARY KEY, v TEXT) WITH (engine='kv')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_kv_multi (k, v) VALUES ('a', '1'), ('b', '2'), ('c', '3')",
        3,
    )
    .await;
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM tag_kv_multi").await,
        3,
        "all three keys must have landed"
    );
}

/// Key-value `INSERT ... ON CONFLICT (k) DO UPDATE` reports a count whether
/// the row was inserted or updated: `INSERT 0 1` on first write, `UPDATE 1`
/// on overwrite. Both are countable; a bare `OK` is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_on_conflict_do_update_reports_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_kv_upsert (k TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    let sql = "INSERT INTO tag_kv_upsert (k, n) VALUES ('a', 1) \
               ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n";
    assert_single_tag(&server, sql, 1).await;
    assert_single_tag(&server, sql, 1).await;
}

/// A timeseries `INSERT` answers `INSERT 0 1`. The ingest handler already
/// reports how many rows it accepted; the count must reach the tag instead of
/// being replaced by a bare `OK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_insert_reports_insert_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_ts \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_ts (id, ts, v) VALUES ('a', 1000, 10)",
        1,
    )
    .await;
}

/// A multi-row timeseries `INSERT` lowers to one ingest task and answers
/// `INSERT 0 3` — the accepted-row count the ingest reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_ts_multi \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_ts_multi (id, ts, v) \
         VALUES ('a', 1000, 10), ('b', 2000, 20), ('c', 3000, 30)",
        3,
    )
    .await;
}

/// A multi-row spatial `INSERT` (the shared columnar write path) answers one
/// `INSERT 0 3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_spatial \
             COLUMNS (id TEXT, loc GEOMETRY) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_spatial (id, loc) VALUES \
         ('a', ST_MakePoint(1.0, 1.0)), \
         ('b', ST_MakePoint(2.0, 2.0)), \
         ('c', ST_MakePoint(3.0, 3.0))",
        3,
    )
    .await;
}

/// A vector-primary collection's `INSERT` answers `INSERT 0 1`, not `OK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_insert_reports_insert_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_vec (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO tag_vec (id, vec, owner) VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        1,
    )
    .await;
}

/// A CRDT collection's `INSERT` is a write statement: it answers `INSERT 0 1`
/// and no row data. Answering with the stored document (a `SELECT`-shaped
/// response) makes a driver's `execute` see rows where it expects a tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_insert_reports_insert_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tag_crdt (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    assert_single_tag(&server, "INSERT INTO tag_crdt (id, v) VALUES ('a', 1)", 1).await;
}

/// A multi-row `INSERT INTO ARRAY` answers one `INSERT 0 3`, not `OK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_multi_row_insert_reports_one_tag_with_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE ARRAY tag_arr \
             DIMS (x INT64 [0..15]) \
             ATTRS (v INT64) \
             TILE_EXTENTS (16) \
             CELL_ORDER ROW_MAJOR",
        )
        .await
        .unwrap_or_else(|e| panic!("create array: {e}"));

    assert_single_tag(
        &server,
        "INSERT INTO ARRAY tag_arr \
         COORDS (0) VALUES (10), \
         COORDS (1) VALUES (11), \
         COORDS (2) VALUES (12)",
        3,
    )
    .await;
}
