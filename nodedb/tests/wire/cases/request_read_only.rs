// SPDX-License-Identifier: BUSL-1.1

//! A request of reads alone runs with no transaction on every transport. It
//! answers the rows of every statement and never ends in a commit-time
//! 40001, even while another client writes the rows it reads.
//!
//! HTTP, NDJSON, WebSocket RPC and native plan a read-only request as one
//! task set. It takes no implicit transaction and no Calvin route, so its
//! per-task loop runs with no read set. pgwire opens its implicit block only
//! for a run that holds a statement that is not a read. A `WITH` or `EXPLAIN`
//! read with no write verb is a read too. A leading read before a write runs
//! in that block, and the write commits.
//!
//! Levers:
//! - Open the pgwire implicit block at every statement (`opens_implicit_block`
//!   in `sql_exec_implicit.rs` returns true). The read-only pgwire cases then
//!   run in a block whose COMMIT checks the read set. Under the concurrent
//!   writer they answer 40001 on some iterations. The lever is timing-based:
//!   one run can pass by chance.
//! - Treat `WITH` as a write head again. The `WITH` pgwire case then opens a
//!   block and can answer 40001 the same way.
//! - Run a read-only HTTP-family request in an implicit transaction
//!   (`statement_needs_implicit_txn` true for reads). Those cases can then
//!   answer 40001 under the writer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;
use tokio_postgres::SimpleQueryMessage;

use super::wire_support::{exec, post, ws_frame};
use crate::harness::TestServer;

/// The collection the reads read and the writer writes.
const TABLE: &str = "ro_t";
/// The read-only requests each transport runs.
const ITERATIONS: usize = 20;
/// The SQLSTATE of a serialization error.
const SERIALIZATION_ERROR: &str = "40001";

/// A one-core server with [`TABLE`] holding rows `a` and `b`.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    exec(&server, &format!("CREATE COLLECTION {TABLE}")).await;
    for id in ["a", "b"] {
        exec(
            &server,
            &format!("INSERT INTO {TABLE} (id, n) VALUES ('{id}', 0)"),
        )
        .await;
    }
    server
}

/// Two reads of rows the writer moves.
fn two_reads() -> String {
    format!("SELECT id, n FROM {TABLE} WHERE id = 'a'; SELECT id, n FROM {TABLE} WHERE id = 'b'")
}

/// A writer that updates rows `a` and `b` over HTTP until it is stopped.
struct Writer {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<usize>,
}

impl Writer {
    fn start(server: &TestServer) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let port = server.http_port;
        let flag = Arc::clone(&stop);
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut written = 0usize;
            while !flag.load(Ordering::Relaxed) {
                for id in ["a", "b"] {
                    let sql = format!("UPDATE {TABLE} SET n = {written} WHERE id = '{id}'");
                    let sent = client
                        .post(format!("http://127.0.0.1:{port}/v1/query"))
                        .json(&serde_json::json!({ "sql": sql }))
                        .send()
                        .await;
                    if sent.is_ok_and(|response| response.status().is_success()) {
                        written += 1;
                    }
                }
            }
            written
        });
        Self { stop, handle }
    }

    /// Stop the writer and check it wrote at least one row.
    async fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        let written = self.handle.await.unwrap_or_else(|e| panic!("writer: {e}"));
        assert!(written > 0, "the concurrent writer must land a write");
    }
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    Http,
    Ndjson,
    Ws,
    Native,
    Pgwire,
}

const TRANSPORTS: [Transport; 5] = [
    Transport::Http,
    Transport::Ndjson,
    Transport::Ws,
    Transport::Native,
    Transport::Pgwire,
];

fn json(sql: &str, text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("a JSON answer for {sql}: {e}: {text}"))
}

/// Run `sql` on `transport`. Returns the number of rows it answered, or the
/// number of statements on pgwire, or the error text.
async fn answered(server: &TestServer, transport: Transport, sql: &str) -> Result<usize, String> {
    match transport {
        Transport::Http => {
            let (status, raw) = post(server, "/v1/query", sql).await;
            if status != 200 {
                return Err(format!("{status}: {raw}"));
            }
            let body = json(sql, &raw);
            body["rows"]
                .as_array()
                .map(Vec::len)
                .ok_or_else(|| format!("no `rows` array: {raw}"))
        }
        Transport::Ndjson => {
            let (status, raw) = post(server, "/v1/query/stream", sql).await;
            let lines: Vec<Value> = raw
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| json(sql, line))
                .collect();
            if status != 200 || lines.iter().any(|line| line.get("error").is_some()) {
                return Err(format!("{status}: {raw}"));
            }
            Ok(lines.len())
        }
        Transport::Ws => {
            let frame = ws_frame(server, sql).await;
            if frame.get("error").is_some() {
                return Err(frame.to_string());
            }
            Ok(match &frame["result"] {
                Value::Null => 0,
                Value::Array(rows) => rows.len(),
                _ => 1,
            })
        }
        Transport::Native => {
            let mut stream = open_trust_session(server.native_port, "nodedb").await;
            let response = send_sql(&mut stream, 2, sql).await;
            match response.error {
                Some(error) => Err(format!("{}: {}", error.code, error.message)),
                None => Ok(response.rows.as_ref().map_or(0, Vec::len)),
            }
        }
        Transport::Pgwire => match server.client.simple_query(sql).await {
            Ok(messages) => Ok(messages
                .iter()
                .filter(|message| matches!(message, SimpleQueryMessage::CommandComplete(_)))
                .count()),
            Err(error) => Err(format!(
                "{}: {error}",
                error.code().map(|code| code.code()).unwrap_or_default()
            )),
        },
    }
}

/// What a request answers: its rows on HTTP, NDJSON, WebSocket RPC and
/// native, and one command per statement on pgwire.
#[derive(Clone, Copy)]
struct Expected {
    rows: usize,
    statements: usize,
}

/// Check that `sql` on `transport` answers `expected` with no error, and
/// never 40001.
async fn assert_answers(server: &TestServer, transport: Transport, sql: &str, expected: Expected) {
    let want = match transport {
        Transport::Pgwire => expected.statements,
        Transport::Http | Transport::Ndjson | Transport::Ws | Transport::Native => expected.rows,
    };
    match answered(server, transport, sql).await {
        Ok(count) => assert_eq!(count, want, "{transport:?}: `{sql}`"),
        Err(error) => {
            assert!(
                !error.contains(SERIALIZATION_ERROR),
                "{transport:?}: read-only `{sql}` must never answer 40001, got {error}"
            );
            panic!("{transport:?}: `{sql}` must answer no error, got {error}");
        }
    }
}

/// `SELECT 1; SELECT 2` answers both rows on every transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_constant_selects_answer_both_on_every_transport() {
    let server = fixture().await;
    for transport in TRANSPORTS {
        let expected = Expected {
            rows: 2,
            statements: 2,
        };
        assert_answers(&server, transport, "SELECT 1; SELECT 2", expected).await;
    }
}

/// Two reads under a concurrent writer answer both rows, with no 40001, on
/// every transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_under_a_concurrent_writer_never_answer_40001_on_every_transport() {
    let server = fixture().await;
    let writer = Writer::start(&server);
    let sql = two_reads();
    for transport in TRANSPORTS {
        for _ in 0..ITERATIONS {
            let expected = Expected {
                rows: 2,
                statements: 2,
            };
            assert_answers(&server, transport, &sql, expected).await;
        }
    }
    writer.stop().await;
}

/// On pgwire a `WITH` read opens no implicit block. Under a concurrent writer
/// it never answers 40001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_with_read_opens_no_block_over_pgwire() {
    let server = fixture().await;
    let writer = Writer::start(&server);
    let sql = format!(
        "WITH r AS (SELECT id, n FROM {TABLE} WHERE id = 'a') SELECT id, n FROM r; \
         SELECT id, n FROM {TABLE} WHERE id = 'b'"
    );
    for _ in 0..ITERATIONS {
        let expected = Expected {
            rows: 2,
            statements: 2,
        };
        assert_answers(&server, Transport::Pgwire, &sql, expected).await;
    }
    writer.stop().await;
}

/// A leading read before a write still runs, and the write commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leading_read_before_a_write_works_on_every_transport() {
    let server = fixture().await;
    for (index, transport) in TRANSPORTS.into_iter().enumerate() {
        let id = format!("w{index}");
        let sql = format!(
            "SELECT id FROM {TABLE} WHERE id = 'a'; \
             INSERT INTO {TABLE} (id, n) VALUES ('{id}', 1)"
        );
        // The SELECT row answers. A row in the reply drops the INSERT
        // count from it.
        let expected = Expected {
            rows: 1,
            statements: 2,
        };
        assert_answers(&server, transport, &sql, expected).await;
        let stored = server
            .query_text(&format!("SELECT id FROM {TABLE} WHERE id = '{id}'"))
            .await
            .unwrap_or_else(|e| panic!("read {id}: {e}"));
        assert_eq!(
            stored,
            vec![id.clone()],
            "{transport:?}: the write after the read must commit"
        );
    }
}

/// A leading read, an insert and an update that matches the inserted row run
/// as one request on the HTTP family and native. The update follows a write,
/// so the request runs in an implicit transaction that holds the read. The
/// request answers the read's row, and the update lands on the inserted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leading_read_with_an_insert_and_an_update_commits_on_every_set_transport() {
    let server = TestServer::start().await;
    for (index, transport) in [
        Transport::Http,
        Transport::Ndjson,
        Transport::Ws,
        Transport::Native,
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("one_set_{index}");
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {table} (id TEXT PRIMARY KEY, v INT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
        exec(
            &server,
            &format!("INSERT INTO {table} (id, v) VALUES ('a', 1)"),
        )
        .await;
        let sql = format!(
            "SELECT v FROM {table} WHERE id = 'a'; \
             INSERT INTO {table} (id, v) VALUES ('b', 2); \
             UPDATE {table} SET v = 3 WHERE v >= 2"
        );
        // The SELECT row answers. A row in the reply drops the write counts
        // from it.
        let expected = Expected {
            rows: 1,
            statements: 3,
        };
        assert_answers(&server, transport, &sql, expected).await;
        let stored = server
            .query_text(&format!("SELECT v FROM {table} WHERE id = 'b'"))
            .await
            .unwrap_or_else(|e| panic!("read b: {e}"));
        assert_eq!(
            stored,
            vec!["3".to_string()],
            "{transport:?}: the update must land on the inserted row"
        );
    }
}
