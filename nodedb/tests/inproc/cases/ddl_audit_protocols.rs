// SPDX-License-Identifier: BUSL-1.1

//! A DDL statement's audit row names the user and the statement text on every
//! client protocol, and a credential statement's text stays redacted.
//!
//! Native and `/v1/query` run the whole DDL router. NDJSON and WebSocket RPC
//! run planned statements only, so `CREATE ARRAY` is their DDL. Their planner
//! refuses `CREATE CREDENTIAL`, so no row and no secret can come from them.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::control::security::audit::{AuditEntry, AuditEvent, AuditWalSink, lock_audit_log};
use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode, TextFields};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

/// A statement that creates array `name`.
fn create_array(name: &str) -> String {
    format!(
        "CREATE ARRAY {name} DIMS (row INT64, col INT64) ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)"
    )
}

/// The DDL audit row that holds `needle`. The applier emits it off its apply loop.
async fn ddl_row(server: &TestServer, needle: &str) -> AuditEntry {
    let matches =
        |entry: &AuditEntry| entry.event == AuditEvent::DdlChange && entry.detail.contains(needle);
    for _ in 0..200 {
        let rows = server
            .shared
            .recent_audit_entries(usize::MAX, matches)
            .expect("read the audit log");
        if let Some(row) = rows.into_iter().next() {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no DDL audit row holds {needle}");
}

/// Every audit row's detail, joined.
fn all_audit_text(server: &TestServer) -> String {
    server
        .shared
        .recent_audit_entries(usize::MAX, |_| true)
        .expect("read the audit log")
        .iter()
        .map(|entry| entry.detail.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

async fn assert_names_user_and_text(server: &TestServer, text: &str) {
    let row = ddl_row(server, text).await;
    assert!(
        !row.auth_user_name.is_empty(),
        "the row must name the user: {row:?}"
    );
    assert!(
        !row.auth_user_id.is_empty(),
        "the row must name the user id: {row:?}"
    );
}

async fn assert_credential_redacted(server: &TestServer, name: &str, secret: &str) {
    let row = ddl_row(server, &format!("CREATE CREDENTIAL {name}")).await;
    assert!(
        row.detail.contains("<redacted>"),
        "the text must be redacted: {row:?}"
    );
    assert!(
        !row.auth_user_name.is_empty(),
        "the row must name the user: {row:?}"
    );
    let all = all_audit_text(server);
    assert!(
        !all.contains(secret),
        "the audit log shows the secret: {all}"
    );
}

async fn native_session(server: &TestServer) -> TcpStream {
    let username = server
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in AuthMode::Trust");
    let addr = format!("127.0.0.1:{}", server.native_port)
        .parse()
        .expect("native addr");
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust { username }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native auth: {auth:?}");
    stream
}

/// POST `sql` to an HTTP SQL route. Returns the status code and the body.
async fn http_post(server: &TestServer, route: &str, sql: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}{route}", server.http_port))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {route}: {e}"));
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| panic!("read {route} body: {e}"));
    (status, body)
}

/// Send `sql` over WebSocket RPC. Returns the answer frame.
async fn ws_rpc(server: &TestServer, sql: &str) -> serde_json::Value {
    let url = format!("ws://127.0.0.1:{}/v1/ws", server.http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .expect("WebSocket connect");
    let frame = serde_json::json!({ "id": 1, "method": "query", "params": { "sql": sql } });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("WebSocket send");
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .expect("an answer in time")
            .expect("the socket stays open")
            .expect("read a frame");
        let Message::Text(text) = message else {
            continue;
        };
        let answer: serde_json::Value = serde_json::from_str(&text).expect("a JSON frame");
        if answer["id"] == 1 {
            return answer;
        }
    }
}

/// An audit WAL that takes no entry, as a full disk does.
pub(super) struct RefusingSink;

impl AuditWalSink for RefusingSink {
    fn append_batch(&self, _entries: &[AuditEntry]) -> nodedb::Result<Vec<u64>> {
        Err(nodedb::Error::Storage {
            engine: "audit_wal".into(),
            detail: "injected audit WAL error".into(),
        })
    }

    fn sync_through(&self, _position: u64) -> nodedb::Result<()> {
        Ok(())
    }

    fn truncate_through(&self, _position: u64) -> nodedb::Result<()> {
        Ok(())
    }
}

/// With the audit WAL refusing, each audited mutation is refused and does not land.
/// With it back, the same statements succeed, which proves nothing landed before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_audit_sink_refuses_the_mutation() {
    let server = TestServer::start().await;
    let audit = &server.shared.audit;

    lock_audit_log(audit).set_wal_sink(Arc::new(RefusingSink) as Arc<dyn AuditWalSink>);
    let statements = [
        "CREATE COLLECTION refused_docs (id TEXT PRIMARY KEY)",
        "CREATE ARRAY refused_grid DIMS (row INT64, col INT64) ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)",
        "CREATE USER refused_user WITH PASSWORD 'refused-password-1' ROLE readwrite",
        "CREATE ORG 'refused_org'",
    ];
    for statement in statements {
        let refused = server.exec(statement).await;
        assert!(refused.is_err(), "{statement} must be refused");
    }

    // A transaction's DDL is refused at its COMMIT.
    server.exec("BEGIN").await.expect("BEGIN");
    let staged = server
        .exec("CREATE COLLECTION refused_txn (id TEXT PRIMARY KEY)")
        .await;
    let refusal = match staged {
        Err(error) => error,
        Ok(()) => server.exec("COMMIT").await.expect_err("COMMIT is refused"),
    };
    assert!(!refusal.is_empty());
    let _ = server.exec("ROLLBACK").await;

    let tables = server.query_text("SHOW COLLECTIONS").await;
    assert!(
        !format!("{tables:?}").contains("refused_docs"),
        "a refused statement must not land: {tables:?}"
    );

    lock_audit_log(audit).set_wal_sink(Arc::clone(&server.shared.wal) as Arc<dyn AuditWalSink>);
    for statement in statements {
        server
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement} after the sink is back: {e}"));
    }
    server
        .exec("CREATE COLLECTION refused_txn (id TEXT PRIMARY KEY)")
        .await
        .expect("the refused transaction left nothing behind");

    let intents = server
        .shared
        .recent_audit_entries(usize::MAX, |entry| {
            entry.event == AuditEvent::ActionIntent && entry.detail.contains("refused_docs")
        })
        .expect("read the audit log");
    assert!(
        intents.iter().any(|row| !row.auth_user_name.is_empty()),
        "the intent row names the user: {intents:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_ddl_names_the_user_and_the_text() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;

    let sql = "CREATE COLLECTION native_ddl_audit (id TEXT PRIMARY KEY)";
    let response = send_sql(&mut stream, 2, sql).await;
    assert_eq!(response.status, ResponseStatus::Ok, "{response:?}");
    assert_names_user_and_text(&server, sql).await;

    let secret = "native-token-5f1c";
    let sql = format!("CREATE CREDENTIAL native_cred (TYPE 'bearer', TOKEN '{secret}')");
    let response = send_sql(&mut stream, 3, &sql).await;
    assert_eq!(response.status, ResponseStatus::Ok, "{response:?}");
    assert_credential_redacted(&server, "native_cred", secret).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_query_ddl_names_the_user_and_the_text() {
    let server = TestServer::start().await;

    let sql = "CREATE COLLECTION http_ddl_audit (id TEXT PRIMARY KEY)";
    let (status, body) = http_post(&server, "/v1/query", sql).await;
    assert!((200..300).contains(&status), "{status}: {body}");
    assert_names_user_and_text(&server, sql).await;

    let sql = create_array("http_ddl_grid");
    let (status, body) = http_post(&server, "/v1/query", &sql).await;
    assert!((200..300).contains(&status), "{status}: {body}");
    assert_names_user_and_text(&server, &sql).await;

    let secret = "http-token-8a2e";
    let sql = format!("CREATE CREDENTIAL http_cred (TYPE 'bearer', TOKEN '{secret}')");
    let (status, body) = http_post(&server, "/v1/query", &sql).await;
    assert!((200..300).contains(&status), "{status}: {body}");
    assert_credential_redacted(&server, "http_cred", secret).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_ddl_names_the_user_and_the_text() {
    let server = TestServer::start().await;

    let sql = create_array("ndjson_ddl_grid");
    let (status, body) = http_post(&server, "/v1/query/stream", &sql).await;
    assert!((200..300).contains(&status), "{status}: {body}");
    assert!(!body.contains("\"error\""), "{body}");
    assert_names_user_and_text(&server, &sql).await;

    let secret = "ndjson-token-3d77";
    let sql = format!("CREATE CREDENTIAL ndjson_cred (TYPE 'bearer', TOKEN '{secret}')");
    let (_status, body) = http_post(&server, "/v1/query/stream", &sql).await;
    assert!(body.contains("error"), "NDJSON runs no router DDL: {body}");
    let all = all_audit_text(&server);
    assert!(
        !all.contains(secret),
        "the audit log shows the secret: {all}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_ddl_names_the_user_and_the_text() {
    let server = TestServer::start().await;

    let sql = create_array("ws_ddl_grid");
    let answer = ws_rpc(&server, &sql).await;
    assert!(answer["error"].is_null(), "{answer}");
    assert_names_user_and_text(&server, &sql).await;

    let secret = "ws-token-61b0";
    let sql = format!("CREATE CREDENTIAL ws_cred (TYPE 'bearer', TOKEN '{secret}')");
    let answer = ws_rpc(&server, &sql).await;
    assert!(
        !answer["error"].is_null(),
        "WebSocket RPC runs no router DDL: {answer}"
    );
    let all = all_audit_text(&server);
    assert!(
        !all.contains(secret),
        "the audit log shows the secret: {all}"
    );
}
