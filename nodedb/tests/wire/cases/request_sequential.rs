// SPDX-License-Identifier: BUSL-1.1

//! A request of several statements that holds DDL or a statement the DDL
//! router runs takes the sequential request runner on every transport.
//!
//! The statements run in order inside one transaction. Statement k sees
//! what statements 1 to k-1 created and wrote. The request commits whole or
//! rolls back whole. A command that cannot run in a transaction block
//! answers SQLSTATE 25001. A request of DML and queries alone keeps the
//! one-set path.
//!
//! The reply is the sum of every statement's answer, as any request of
//! several statements answers. A DDL status adds no row and no count.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;

use super::wire_support::{exec, post, rows, ws_frame};
use crate::harness::TestServer;

/// The transports a request can arrive on.
#[derive(Debug, Clone, Copy)]
enum Transport {
    Http,
    Ndjson,
    Ws,
    Native,
}

impl Transport {
    /// A prefix for the collections a test on this transport creates.
    fn prefix(self) -> &'static str {
        match self {
            Self::Http => "sq_http",
            Self::Ndjson => "sq_ndjson",
            Self::Ws => "sq_ws",
            Self::Native => "sq_native",
        }
    }
}

/// What a transport answered to a request.
#[derive(Debug)]
struct Answer {
    /// The error text. `None` when the request succeeded.
    error: Option<String>,
    /// The SQLSTATE of the error, when the transport reports one.
    sqlstate: Option<String>,
    /// How many data rows the reply holds.
    rows: usize,
    /// The raw answer, for messages.
    raw: String,
}

impl Answer {
    fn ok(&self) -> &Self {
        assert!(
            self.error.is_none(),
            "the request must succeed: {}",
            self.raw
        );
        self
    }
}

async fn send(server: &TestServer, transport: Transport, sql: &str) -> Answer {
    match transport {
        Transport::Http => {
            let (status, body) = server.http_query(sql).await;
            let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let error = (!(200..300).contains(&status)).then(|| {
                value["error"]
                    .as_str()
                    .map_or_else(|| body.clone(), str::to_owned)
            });
            Answer {
                error,
                sqlstate: value["sqlstate"].as_str().map(str::to_owned),
                rows: value["rows"].as_array().map_or(0, Vec::len),
                raw: format!("{status} {body}"),
            }
        }
        Transport::Ndjson => {
            let (_, body) = post(server, "/v1/query/stream", sql).await;
            let lines: Vec<Value> = body
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| serde_json::from_str(line).unwrap_or(Value::Null))
                .collect();
            let error_line = lines.iter().find(|line| line.get("error").is_some());
            Answer {
                error: error_line.map(|line| line["error"].to_string()),
                sqlstate: error_line.and_then(|line| line["sqlstate"].as_str().map(str::to_owned)),
                rows: lines
                    .iter()
                    .filter(|line| line.get("error").is_none())
                    .count(),
                raw: body,
            }
        }
        Transport::Ws => {
            let frame = ws_frame(server, sql).await;
            let error = (!frame["error"].is_null()).then(|| frame["error"].to_string());
            let rows = match &frame["result"] {
                Value::Null => 0,
                Value::Array(rows) => rows.len(),
                _ => 1,
            };
            Answer {
                error,
                sqlstate: frame["sqlstate"].as_str().map(str::to_owned),
                rows,
                raw: frame.to_string(),
            }
        }
        Transport::Native => {
            let mut stream = open_trust_session(server.native_port, "nodedb").await;
            let response = send_sql(&mut stream, 2, sql).await;
            Answer {
                error: response.error.as_ref().map(|error| error.message.clone()),
                sqlstate: response.error.as_ref().map(|error| error.code.clone()),
                rows: response.rows.as_ref().map_or(0, Vec::len),
                raw: format!("{response:?}"),
            }
        }
    }
}

/// Whether a collection named `name` exists, read over pgwire.
async fn exists(server: &TestServer, name: &str) -> bool {
    server
        .query_rows("SHOW COLLECTIONS")
        .await
        .unwrap_or_else(|e| panic!("SHOW COLLECTIONS: {e}"))
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .any(|collection| collection == name)
}

fn create(name: &str) -> String {
    format!(
        "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
    )
}

// ─── CREATE, INSERT, SELECT ──────────────────────────────────────────

/// `CREATE COLLECTION t; INSERT INTO t ..; SELECT .. FROM t` answers the
/// row the SELECT reads and stores it.
async fn assert_create_insert_select(transport: Transport) {
    let server = TestServer::start().await;
    let table = format!("{}_made", transport.prefix());
    let sql = format!(
        "{}; INSERT INTO {table} (id, v) VALUES ('a', 'x'); SELECT id, v FROM {table}",
        create(&table)
    );
    let answer = send(&server, transport, &sql).await;
    answer.ok();
    assert_eq!(answer.rows, 1, "{transport:?} {sql}: {}", answer.raw);
    assert!(
        exists(&server, &table).await,
        "{transport:?}: {table} commits"
    );
    assert_eq!(rows(&server, &table).await, [["a", "x"]], "{transport:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_create_insert_select_answers_the_selected_row() {
    assert_create_insert_select(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_create_insert_select_answers_the_selected_row() {
    assert_create_insert_select(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_create_insert_select_answers_the_selected_row() {
    assert_create_insert_select(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_create_insert_select_answers_the_selected_row() {
    assert_create_insert_select(Transport::Native).await;
}

/// HTTP answers `CREATE t; INSERT INTO t ..` with the INSERT count alone:
/// the DDL status adds no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_create_then_insert_answers_the_insert_count() {
    let server = TestServer::start().await;
    let sql = format!(
        "{}; INSERT INTO sq_shape_req (id, v) VALUES ('a', 'x')",
        create("sq_shape_req")
    );
    let (status, body) = server.http_query(&sql).await;
    assert_eq!(status, 200, "{body}");
    let body: Value = serde_json::from_str(&body).expect("a JSON body");
    assert_eq!(
        body["rows"],
        serde_json::json!([{ "affected": 1 }]),
        "{body}"
    );
    assert!(body.get("statements").is_none(), "{body}");
}

/// Native answers `CREATE t; INSERT; INSERT` with one summed INSERT tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_create_then_inserts_answers_one_summed_tag() {
    let server = TestServer::start().await;
    let sql = format!(
        "{}; INSERT INTO sq_tag (id, v) VALUES ('a', 'x'); \
         INSERT INTO sq_tag (id, v) VALUES ('b', 'y')",
        create("sq_tag")
    );
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let response = send_sql(&mut stream, 2, &sql).await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(response.command.as_deref(), Some("INSERT"), "{response:?}");
    assert_eq!(response.rows_affected, Some(2), "{response:?}");
}

// ─── Rollback ────────────────────────────────────────────────────────

/// `CREATE fresh; INSERT ok; CREATE bad` stores no row and no collection.
async fn assert_error_rolls_back_everything(transport: Transport) {
    let server = TestServer::start().await;
    let base = format!("{}_base", transport.prefix());
    let fresh = format!("{}_fresh", transport.prefix());
    exec(&server, &create(&base)).await;
    // The last statement creates a collection that exists.
    let sql = format!(
        "{}; INSERT INTO {base} (id, v) VALUES ('r1', 'x'); {}",
        create(&fresh),
        create(&base)
    );
    let answer = send(&server, transport, &sql).await;
    assert!(
        answer.error.is_some(),
        "{transport:?} {sql}: {}",
        answer.raw
    );
    assert!(
        rows(&server, &base).await.is_empty(),
        "{transport:?}: the INSERT rolls back"
    );
    assert!(
        !exists(&server, &fresh).await,
        "{transport:?}: the first CREATE rolls back"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_error_rolls_back_every_statement() {
    assert_error_rolls_back_everything(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_error_rolls_back_every_statement() {
    assert_error_rolls_back_everything(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_error_rolls_back_every_statement() {
    assert_error_rolls_back_everything(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_error_rolls_back_every_statement() {
    assert_error_rolls_back_everything(Transport::Native).await;
}

// ─── Claimed writes alone and in a request ───────────────────────────

/// Create `{name}_lone`, `{name}_first` and `{name}_second`, each keyed by
/// its own sequence.
async fn sequence_keyed(server: &TestServer, name: &str) {
    for role in ["lone", "first", "second"] {
        let table = format!("{name}_{role}");
        exec(server, &format!("CREATE SEQUENCE {table}_seq")).await;
        exec(
            server,
            &format!(
                "CREATE COLLECTION {table} (\
                    id BIGINT DEFAULT nextval('{table}_seq') PRIMARY KEY, \
                    v TEXT)"
            ),
        )
        .await;
    }
}

/// Run `claimed` alone over HTTP into `{name}_lone`, then over `transport`
/// as statement 1 and 2 of a request into `{name}_first` and
/// `{name}_second`. `claimed` names its target as `{t}`. All three store
/// the same row.
async fn assert_same_row_alone_or_in_request(
    server: &TestServer,
    transport: Transport,
    name: &str,
    claimed: &str,
) {
    let on = |role: &str| claimed.replace("{t}", &format!("{name}_{role}"));
    let plain = format!("{}_plain", transport.prefix());

    server.http_exec(&on("lone")).await;
    let first = format!(
        "{}; INSERT INTO {plain} (id, v) VALUES ('{name}_p1', 'b')",
        on("first")
    );
    send(server, transport, &first).await.ok();
    let second = format!(
        "INSERT INTO {plain} (id, v) VALUES ('{name}_p2', 'b'); {}",
        on("second")
    );
    send(server, transport, &second).await.ok();

    let lone = rows(server, &format!("{name}_lone")).await;
    assert_eq!(
        lone,
        [["1", "a"]],
        "the router fills the key from the sequence"
    );
    for role in ["first", "second"] {
        assert_eq!(
            rows(server, &format!("{name}_{role}")).await,
            lone,
            "{transport:?} {claimed} as the {role} statement"
        );
    }
}

async fn assert_claimed_writes(transport: Transport) {
    let server = TestServer::start().await;
    let prefix = transport.prefix();
    exec(&server, &create(&format!("{prefix}_plain"))).await;
    let upsert = format!("{prefix}_up");
    let literal = format!("{prefix}_lit");
    sequence_keyed(&server, &upsert).await;
    sequence_keyed(&server, &literal).await;
    assert_same_row_alone_or_in_request(
        &server,
        transport,
        &upsert,
        "UPSERT INTO {t} (v) VALUES ('a')",
    )
    .await;
    assert_same_row_alone_or_in_request(&server, transport, &literal, "INSERT INTO {t} { v: 'a' }")
        .await;
    assert_eq!(
        rows(&server, &format!("{prefix}_plain")).await.len(),
        4,
        "{transport:?}: every plain statement beside a claimed one applies"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_claimed_writes_store_the_same_row_alone_or_in_a_request() {
    assert_claimed_writes(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_claimed_writes_store_the_same_row_alone_or_in_a_request() {
    assert_claimed_writes(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_claimed_writes_store_the_same_row_alone_or_in_a_request() {
    assert_claimed_writes(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_claimed_writes_store_the_same_row_alone_or_in_a_request() {
    assert_claimed_writes(Transport::Native).await;
}

// ─── A command that refuses a transaction block ─────────────────────

/// `INSERT ok; BACKUP DATABASE ..` answers 25001 and stores no row.
async fn assert_block_refusal(transport: Transport) {
    let server = TestServer::start().await;
    let base = format!("{}_r13", transport.prefix());
    exec(&server, &create(&base)).await;
    let sql = format!(
        "INSERT INTO {base} (id, v) VALUES ('b1', 'x'); \
         BACKUP DATABASE default TO 'file:///tmp/{base}_backup'"
    );
    let answer = send(&server, transport, &sql).await;
    let error = answer
        .error
        .as_deref()
        .unwrap_or_else(|| panic!("{transport:?} {sql} must be refused: {}", answer.raw));
    assert!(
        error.contains("cannot run inside a transaction block"),
        "{transport:?}: {}",
        answer.raw
    );
    // HTTP answers the status its SQLSTATE maps to. The other transports
    // carry the SQLSTATE itself.
    if !matches!(transport, Transport::Http) {
        assert_eq!(
            answer.sqlstate.as_deref(),
            Some("25001"),
            "{transport:?}: {}",
            answer.raw
        );
    }
    assert!(
        rows(&server, &base).await.is_empty(),
        "{transport:?}: the INSERT rolls back"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_block_refusal_answers_inside_the_request() {
    assert_block_refusal(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_block_refusal_answers_25001() {
    assert_block_refusal(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_block_refusal_answers_25001() {
    assert_block_refusal(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_block_refusal_answers_25001() {
    assert_block_refusal(Transport::Native).await;
}

// ─── DML alone keeps the one-set path ────────────────────────────────

/// A plain INSERT and SELECT pair keeps the one-set path: the INSERT
/// applies, the SELECT reads it, and no DDL row joins the reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_insert_and_select_pair_keeps_the_one_set_answer() {
    let server = TestServer::start().await;
    exec(&server, &create("sq_pair")).await;
    let sql = "INSERT INTO sq_pair (id, v) VALUES ('p1', 'b'); SELECT id, v FROM sq_pair";
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 200, "{body}");
    let body: Value = serde_json::from_str(&body).expect("a JSON body");
    let replied = body["rows"].as_array().expect("a row array");
    assert!(
        replied.iter().all(|row| row.get("type").is_none()),
        "no row is a DDL row: {body}"
    );
    assert_eq!(rows(&server, "sq_pair").await, [["p1", "b"]]);
}
