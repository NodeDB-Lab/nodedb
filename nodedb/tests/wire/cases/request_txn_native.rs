// SPDX-License-Identifier: BUSL-1.1

//! Transaction control inside one native request, and the `[server.sql]`
//! switches that turn each transaction-control policy off.
//!
//! Native follows PostgreSQL rules for a request of several statements:
//! `BEGIN` opens the session block, `COMMIT` and `ROLLBACK` end it, and a
//! statement outside a block runs in an implicit transaction. The answer
//! folds every statement that is not transaction control into one frame:
//! rows in statement order and one summed tag. With the native switch off,
//! native answers SQLSTATE 0A000 to a request that holds transaction
//! control.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::protocol::NativeResponse;
use tokio::net::TcpStream;

use crate::harness::TestServer;

/// A one-core server with `tn (id, n)`.
async fn fixture(server: TestServer) -> TestServer {
    server
        .exec("CREATE COLLECTION tn (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')")
        .await
        .unwrap_or_else(|e| panic!("create tn: {e}"));
    server
}

fn insert(id: &str) -> String {
    format!("INSERT INTO tn (id, n) VALUES ('{id}', 1)")
}

/// The ids of `tn` among `ids` that a row holds, read over pgwire.
async fn stored(server: &TestServer, ids: &[&str]) -> Vec<String> {
    let list = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>();
    let sql = format!(
        "SELECT id FROM tn WHERE id IN ({}) ORDER BY id",
        list.join(", ")
    );
    server
        .query_text(&sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"))
}

/// A native session. Seq 1 is the handshake, so requests start at seq 2.
struct Native {
    stream: TcpStream,
    seq: u64,
}

impl Native {
    async fn open(server: &TestServer) -> Self {
        Self {
            stream: open_trust_session(server.native_port, "nodedb").await,
            seq: 1,
        }
    }

    async fn sql(&mut self, sql: &str) -> NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }

    /// Send `sql` and check that it answers no error.
    async fn ok(&mut self, sql: &str) -> NativeResponse {
        let answer = self.sql(sql).await;
        if let Some(error) = &answer.error {
            panic!("{sql} must succeed, got {}: {}", error.code, error.message);
        }
        answer
    }

    /// Send `sql` and return the SQLSTATE of its error.
    async fn refused(&mut self, sql: &str) -> String {
        let answer = self.sql(sql).await;
        let error = answer
            .error
            .unwrap_or_else(|| panic!("{sql} must be refused, got {:?}", answer.status));
        error.code
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_begin_insert_commit_in_one_request_applies_the_insert() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    let answer = native
        .ok(&format!(
            "BEGIN; {}; {}; COMMIT",
            insert("c1a"),
            insert("c1b")
        ))
        .await;
    assert_eq!(stored(&server, &["c1a", "c1b"]).await, ["c1a", "c1b"]);
    // The answer is the answer of the two inserts, not the COMMIT row.
    assert_eq!(answer.command.as_deref(), Some("INSERT"), "{answer:?}");
    assert_eq!(answer.rows_affected, Some(2), "{answer:?}");
    // The block ended: a lone write applies at once.
    native.ok(&insert("c1c")).await;
    assert_eq!(stored(&server, &["c1c"]).await, ["c1c"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_begin_insert_rollback_in_one_request_applies_nothing() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    native
        .ok(&format!("BEGIN; {}; ROLLBACK", insert("r1a")))
        .await;
    assert!(stored(&server, &["r1a"]).await.is_empty());
    // Statements before a `BEGIN` join the block, so the ROLLBACK drops them.
    native
        .ok(&format!(
            "{}; BEGIN; {}; ROLLBACK",
            insert("r1b"),
            insert("r1c")
        ))
        .await;
    assert!(stored(&server, &["r1b", "r1c"]).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_commit_in_the_middle_commits_each_part_on_its_own() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    native
        .ok(&format!("{}; COMMIT; {}", insert("m1a"), insert("m1b")))
        .await;
    assert_eq!(stored(&server, &["m1a", "m1b"]).await, ["m1a", "m1b"]);

    // The part after COMMIT is its own transaction: its error drops its own
    // write and keeps the committed part.
    let sql = format!("{}; COMMIT; {}; SELEC 1", insert("m2a"), insert("m2b"));
    native.refused(&sql).await;
    assert_eq!(stored(&server, &["m2a", "m2b"]).await, ["m2a"]);
}

/// A read inside the block answers its rows beside the write's tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_block_answers_the_rows_and_tag_of_its_statements() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    let answer = native
        .ok(&format!(
            "BEGIN; {}; SELECT id FROM tn WHERE id = 'b1a'; COMMIT",
            insert("b1a")
        ))
        .await;
    assert_eq!(answer.command.as_deref(), Some("INSERT"), "{answer:?}");
    assert_eq!(answer.rows_affected, Some(1), "{answer:?}");
    let rows = answer.rows.unwrap_or_default();
    assert_eq!(rows.len(), 1, "the SELECT reads the staged row");
}

/// Transaction control alone answers its last statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_begin_commit_alone_answers_the_commit() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    let answer = native.ok("BEGIN; COMMIT").await;
    assert_eq!(answer.command, None, "{answer:?}");
    assert!(answer.rows.is_some(), "the COMMIT status row: {answer:?}");
}

/// A block an earlier request opened takes the statements of a later one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_commit_ends_a_block_an_earlier_request_opened() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    native.ok("BEGIN").await;
    native
        .ok(&format!("{}; COMMIT; {}", insert("e1a"), insert("e1b")))
        .await;
    assert_eq!(stored(&server, &["e1a", "e1b"]).await, ["e1a", "e1b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_error_inside_a_block_leaves_the_session_failed() {
    let server = fixture(TestServer::start().await).await;
    let mut native = Native::open(&server).await;
    native
        .refused(&format!("BEGIN; {}; SELEC 1; COMMIT", insert("f1a")))
        .await;
    // The COMMIT after the error never ran. The block refuses every other
    // statement until the client ends it.
    assert_eq!(native.refused(&insert("f1b")).await, "25P02");
    native.ok("ROLLBACK").await;
    assert!(stored(&server, &["f1a", "f1b"]).await.is_empty());
    native.ok(&insert("f1c")).await;
    assert_eq!(stored(&server, &["f1c"]).await, ["f1c"]);
}

// ─── Switch off ──────────────────────────────────────────────────────

fn wrapper(id: &str) -> String {
    format!("BEGIN; {}; COMMIT", insert(id))
}

async fn off_fixture() -> TestServer {
    fixture(TestServer::start_with_txn_control_native_off().await).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_wrapper_with_the_switch_off_is_refused() {
    let server = off_fixture().await;
    let mut native = Native::open(&server).await;
    let sql = wrapper("o_native");
    assert_eq!(native.refused(&sql).await, "0A000", "{sql}");
    assert!(stored(&server, &["o_native"]).await.is_empty(), "{sql}");
    // A lone BEGIN keeps working with the switch off.
    native.ok("BEGIN").await;
    native.ok(&insert("o_native_b")).await;
    native.ok("COMMIT").await;
    assert_eq!(stored(&server, &["o_native_b"]).await, ["o_native_b"]);
}
