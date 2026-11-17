// SPDX-License-Identifier: BUSL-1.1

//! An autocommit statement is atomic, as in PostgreSQL: when one of its rows
//! fails, the statement fails with that row's SQLSTATE and writes nothing.
//! Each case runs on the pgwire simple and extended query protocols and over
//! native. A refused statement writes nothing, so the three runs send the
//! same text.

use tokio::net::TcpStream;

use crate::harness::TestServer;

use super::returning_vote_single_shard::{create, native, native_error, rows_of};
use super::returning_vote_single_shard_engines::admitted_txns;
use super::returning_vote_single_shard_pgwire::{extended_error, simple_error};

/// The native session and request sequence the cases of one server share.
pub(super) struct Native {
    stream: TcpStream,
    seq: u64,
}

impl Native {
    pub(super) async fn open(server: &TestServer) -> Self {
        Self {
            stream: native(server).await,
            seq: 1,
        }
    }

    /// The SQLSTATE and message `sql` fails with over native.
    pub(super) async fn error(&mut self, sql: &str) -> (String, String) {
        self.seq += 1;
        native_error(&mut self.stream, self.seq, sql).await
    }

    /// Run `sql` over native, expecting success.
    pub(super) async fn ok(&mut self, sql: &str) {
        self.seq += 1;
        let response =
            nodedb_test_support::native_harness::send_sql(&mut self.stream, self.seq, sql).await;
        assert!(response.error.is_none(), "{sql}: {:?}", response.error);
    }
}

/// Assert `sql` fails with `code` on the simple and extended protocols and
/// over native.
pub(super) async fn fails_everywhere(
    server: &TestServer,
    native: &mut Native,
    sql: &str,
    code: &str,
) {
    let (found, message) = simple_error(&server.client, sql).await;
    assert_eq!(found, code, "simple: {sql}: {message}");
    let (found, message) = extended_error(&server.client, sql).await;
    assert_eq!(found, code, "extended: {sql}: {message}");
    let (found, message) = native.error(sql).await;
    assert_eq!(found, code, "native: {sql}: {message}");
}

/// Assert `sql` fails with `code` on the simple query protocol and over
/// native, for a statement the extended protocol does not carry.
pub(super) async fn fails_on_simple_and_native(
    server: &TestServer,
    native: &mut Native,
    sql: &str,
    code: &str,
) {
    let (found, message) = simple_error(&server.client, sql).await;
    assert_eq!(found, code, "simple: {sql}: {message}");
    let (found, message) = native.error(sql).await;
    assert_eq!(found, code, "native: {sql}: {message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_third_row_that_violates_unique_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_uniq (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    create(&server, "INSERT INTO sa_uniq (id, n) VALUES ('c', 0)").await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sa_uniq (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)",
        "23505",
    )
    .await;
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM sa_uniq").await,
        ["c\t0"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_third_row_with_a_type_error_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_type (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sa_type (id, n) VALUES ('a', 1), ('b', 2), ('c', 'x')",
        "22P02",
    )
    .await;
    assert!(rows_of(&server, "SELECT id FROM sa_type").await.is_empty());
}

/// A text expression of 10^7 bytes: seven nested `replace` calls, each
/// turning every `x` into ten.
fn over_row_limit_text() -> String {
    let mut expr = "'x'".to_owned();
    for _ in 0..7 {
        expr = format!("replace({expr}, 'x', 'xxxxxxxxxx')");
    }
    assert!(10_usize.pow(7) > nodedb_types::MAX_ROW_IMAGE_BYTES);
    expr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_third_row_over_the_row_limit_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_big (id TEXT PRIMARY KEY, b TEXT)",
    )
    .await;
    let mut native = Native::open(&server).await;

    // The planner folds the nested `replace` calls into one value of 10^7
    // bytes, past the 8 MiB row limit, from a statement of a few hundred
    // bytes.
    let big = over_row_limit_text();
    fails_everywhere(
        &server,
        &mut native,
        &format!("INSERT INTO sa_big (id, b) VALUES ('a', 'p'), ('b', 'q'), ('c', {big})"),
        "54000",
    )
    .await;
    assert!(rows_of(&server, "SELECT id FROM sa_big").await.is_empty());
}

/// An `UPDATE` of several keys lowers to one predicate write that judges
/// every matched row before the first write. The second key's claim on the
/// value the first key takes is a unique violation, and no key changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_several_key_update_that_violates_unique_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_upd (id TEXT PRIMARY KEY, code TEXT)",
    )
    .await;
    create(&server, "CREATE UNIQUE INDEX sa_upd_code ON sa_upd (code)").await;
    create(
        &server,
        "INSERT INTO sa_upd (id, code) VALUES ('a', 'A'), ('b', 'B')",
    )
    .await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "UPDATE sa_upd SET code = 'Z' WHERE id IN ('a', 'b')",
        "23505",
    )
    .await;
    assert_eq!(
        rows_of(&server, "SELECT id, code FROM sa_upd").await,
        ["a\tA", "b\tB"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_kv_write_with_a_repeated_key_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_kv (id TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sa_kv (id, n) VALUES ('a', 1), ('b', 2), ('a', 3)",
        "23505",
    )
    .await;
    assert!(rows_of(&server, "SELECT id FROM sa_kv").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_columnar_or_timeseries_write_writes_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_col (id TEXT PRIMARY KEY, n INT) WITH (engine='columnar')",
    )
    .await;
    create(&server, "INSERT INTO sa_col (id, n) VALUES ('c', 0)").await;
    create(
        &server,
        "CREATE COLLECTION sa_ts (ts TIMESTAMP TIME_KEY, n INT) WITH (engine='timeseries')",
    )
    .await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sa_col (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)",
        "23505",
    )
    .await;
    assert_eq!(rows_of(&server, "SELECT id, n FROM sa_col").await, ["c\t0"]);

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sa_ts (ts, n) VALUES (1000, 1), (2000, 2), (3000, 'x')",
        "22P02",
    )
    .await;
    assert!(rows_of(&server, "SELECT n FROM sa_ts").await.is_empty());
}

/// A single-row insert keeps the direct path: no Calvin transaction commits
/// it. A `DELETE` of several keys in one vShard lowers to one predicate
/// delete, so it keeps the direct path too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_write_and_a_several_key_delete_keep_the_direct_path() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sa_direct (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    let mut native = Native::open(&server).await;

    let before = admitted_txns(&server).await;
    create(&server, "INSERT INTO sa_direct (id, n) VALUES ('a', 1)").await;
    native
        .ok("INSERT INTO sa_direct (id, n) VALUES ('b', 1)")
        .await;
    create(&server, "INSERT INTO sa_direct (id, n) VALUES ('c', 1)").await;
    assert_eq!(
        admitted_txns(&server).await,
        before,
        "a single-row insert takes the direct path"
    );

    create(&server, "DELETE FROM sa_direct WHERE id IN ('a', 'b')").await;
    assert_eq!(
        admitted_txns(&server).await,
        before,
        "a several-key delete is one task and admits no Calvin transaction"
    );
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM sa_direct").await,
        ["c\t1"]
    );
}

/// A statement that emits several tasks commits as one Calvin transaction.
/// A `DELETE` of several keys on a collection with implicit edges removes
/// its rows and their mirrored edges, so it takes the Calvin route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_with_several_tasks_commits_as_one_calvin_transaction() {
    let server = TestServer::start().await;
    create(&server, "CREATE COLLECTION sa_calvin").await;
    for sql in [
        "INSERT INTO sa_calvin { id: 'e1', _from: 'a', _to: 'b', _type: 'l' }",
        "INSERT INTO sa_calvin { id: 'e2', _from: 'b', _to: 'c', _type: 'l' }",
        "INSERT INTO sa_calvin { id: 'e3', _from: 'c', _to: 'd', _type: 'l' }",
    ] {
        create(&server, sql).await;
    }

    let before = admitted_txns(&server).await;
    create(&server, "DELETE FROM sa_calvin WHERE id IN ('e1', 'e2')").await;
    assert_eq!(
        admitted_txns(&server).await,
        before + 1,
        "a row delete plus its edge deletes commit as one Calvin transaction"
    );
    assert_eq!(rows_of(&server, "SELECT id FROM sa_calvin").await, ["e3"]);
}
