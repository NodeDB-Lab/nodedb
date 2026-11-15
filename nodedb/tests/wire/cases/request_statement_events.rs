// SPDX-License-Identifier: BUSL-1.1

//! Statement-level triggers and `ON DENY` are read per statement in a
//! request of several statements, on every transport.
//!
//! A SYNC AFTER STATEMENT trigger fires once per statement, as in
//! PostgreSQL. `INSERT a; INSERT a` fires it twice. One statement that
//! writes two rows fires it once. Each statement reads its own `ON DENY`
//! clause. A clause on statement 1 never swallows statement 2.
//!
//! Levers:
//! - One shared `StatementEvents` for the whole request in `run_statement_loop`
//!   (`task_loop.rs`). The two-statement cases on HTTP, NDJSON, WS and native
//!   then count 1 firing, not 2.
//! - `extract_on_deny` reads the clause off the joined text again (drop its
//!   several-statement check). The HTTP `ON DENY ERROR` case then answers one
//!   row, not two. The `ON DENY SILENT` case answers an error.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;
use tokio_postgres::SimpleQueryMessage;

use super::wire_support::{exec, post, ws_frame};
use crate::harness::TestServer;

/// The collection whose inserts fire the statement trigger.
const ITEMS: &str = "rse_items";
/// The collection the statement trigger writes one row into per firing.
const LOG: &str = "rse_log";

/// A one-core server with [`ITEMS`], [`LOG`] and a SYNC AFTER INSERT
/// statement trigger on [`ITEMS`]. Each firing writes one row of [`LOG`].
///
/// The body writes no generated id: a volatile call in an `INSERT`'s VALUES
/// list is refused at plan time, and these cells are about how often the
/// trigger fires, not about that gap.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    for name in [ITEMS, LOG] {
        exec(&server, &format!("CREATE COLLECTION {name}")).await;
    }
    exec(
        &server,
        &format!(
            "CREATE SYNC TRIGGER rse_count AFTER INSERT ON {ITEMS} FOR EACH STATEMENT \
             BEGIN INSERT INTO {LOG} (op) VALUES ('fired'); END"
        ),
    )
    .await;
    server
}

/// The number of times the statement trigger fired so far.
async fn firings(server: &TestServer) -> usize {
    server
        .query_text(&format!("SELECT op FROM {LOG}"))
        .await
        .unwrap_or_else(|e| panic!("read {LOG}: {e}"))
        .len()
}

fn insert(id: &str) -> String {
    format!("INSERT INTO {ITEMS} (id, n) VALUES ('{id}', 1)")
}

/// Two statements that insert into [`ITEMS`], each one row.
fn two_inserts(prefix: &str) -> String {
    format!(
        "{}; {}",
        insert(&format!("{prefix}1")),
        insert(&format!("{prefix}2"))
    )
}

/// One statement that inserts two rows into [`ITEMS`].
fn one_insert_of_two_rows(prefix: &str) -> String {
    format!("INSERT INTO {ITEMS} (id, n) VALUES ('{prefix}1', 1), ('{prefix}2', 2)")
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

/// Run `sql` on `transport` and check it answers with no error.
async fn run_ok(server: &TestServer, transport: Transport, sql: &str) {
    match transport {
        Transport::Http | Transport::Ndjson => {
            let path = match transport {
                Transport::Http => "/v1/query",
                _ => "/v1/query/stream",
            };
            let (status, body) = post(server, path, sql).await;
            assert_eq!(
                status, 200,
                "{transport:?} {sql} must answer 200, got {status}: {body}"
            );
            assert!(
                !body.contains("\"error\""),
                "{transport:?} {sql} must answer no error, got {body}"
            );
        }
        Transport::Ws => {
            let frame = ws_frame(server, sql).await;
            assert!(
                frame.get("error").is_none(),
                "WS {sql} must answer no error, got {frame}"
            );
        }
        Transport::Native => {
            let mut stream = open_trust_session(server.native_port, "nodedb").await;
            let response = send_sql(&mut stream, 2, sql).await;
            assert!(
                response.error.is_none(),
                "native {sql} must answer no error, got {:?}",
                response.error
            );
        }
        Transport::Pgwire => exec(server, sql).await,
    }
}

/// Two statements fire the statement trigger twice, once per statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_statements_fire_the_statement_trigger_twice_on_every_transport() {
    let server = fixture().await;
    for (index, transport) in TRANSPORTS.into_iter().enumerate() {
        let before = firings(&server).await;
        let sql = two_inserts(&format!("two{index}-"));
        run_ok(&server, transport, &sql).await;
        assert_eq!(
            firings(&server).await,
            before + 2,
            "{transport:?}: `{sql}` must fire the statement trigger once per statement"
        );
    }
}

/// One statement fires the statement trigger once, whatever its row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_statement_fires_the_statement_trigger_once_on_every_transport() {
    let server = fixture().await;
    for (index, transport) in TRANSPORTS.into_iter().enumerate() {
        for sql in [
            insert(&format!("one{index}")),
            one_insert_of_two_rows(&format!("rows{index}-")),
        ] {
            let before = firings(&server).await;
            run_ok(&server, transport, &sql).await;
            assert_eq!(
                firings(&server).await,
                before + 1,
                "{transport:?}: `{sql}` must fire the statement trigger once"
            );
        }
    }
}

/// The rows of an HTTP body.
fn http_rows(sql: &str, status: u16, body: &str) -> usize {
    assert_eq!(status, 200, "{sql} must answer 200, got {status}: {body}");
    let parsed: Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("a JSON body for {sql}: {e}: {body}"));
    assert!(parsed.get("statements").is_none(), "{sql}: {body}");
    parsed["rows"].as_array().map_or(0, Vec::len)
}

/// An `ON DENY` clause on statement 1 covers statement 1 alone. Statement 2
/// still runs and answers its own row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_deny_is_read_per_statement_over_http() {
    let server = fixture().await;
    run_ok(&server, Transport::Pgwire, &insert("d1")).await;
    for sql in [
        format!("SELECT id FROM {ITEMS} ON DENY ERROR 'E1' MESSAGE 'no'; SELECT id FROM {ITEMS}"),
        format!("SELECT id FROM {ITEMS} ON DENY SILENT; SELECT id FROM {ITEMS}"),
        format!("SELECT id FROM {ITEMS}; SELECT id FROM {ITEMS} ON DENY SILENT"),
    ] {
        let (status, body) = server.http_query(&sql).await;
        assert_eq!(
            http_rows(&sql, status, &body),
            2,
            "{sql} must answer the row of each statement: {body}"
        );
    }
}

/// A request of one statement keeps its `ON DENY` clause handling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_deny_of_one_statement_is_unchanged_over_http() {
    let server = fixture().await;
    run_ok(&server, Transport::Pgwire, &insert("d1")).await;
    let sql = format!("SELECT id FROM {ITEMS} ON DENY SILENT");
    let (status, body) = server.http_query(&sql).await;
    assert_eq!(
        http_rows(&sql, status, &body),
        1,
        "{sql} must answer as one statement: {body}"
    );
    assert!(body.contains("d1"), "{sql} must return row d1: {body}");
}

/// pgwire splits the text before it reads `ON DENY`. Each statement answers
/// its own CommandComplete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_deny_is_read_per_statement_over_pgwire() {
    let server = fixture().await;
    run_ok(&server, Transport::Pgwire, &insert("d1")).await;
    let sql =
        format!("SELECT id FROM {ITEMS} ON DENY ERROR 'E1' MESSAGE 'no'; SELECT id FROM {ITEMS}");
    let messages = server
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let completes = messages
        .iter()
        .filter(|message| matches!(message, SimpleQueryMessage::CommandComplete(_)))
        .count();
    assert_eq!(completes, 2, "{sql} must answer one tag per statement");
}
