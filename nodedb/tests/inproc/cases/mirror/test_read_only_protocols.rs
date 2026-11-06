// SPDX-License-Identifier: BUSL-1.1

//! A mirror that is not promoted refuses writes and object DDL on every client
//! protocol, and reads by the session's consistency level.
//!
//! The fixture creates its objects first and turns the database into a mirror
//! after, so each statement reaches a mirror gate and not a missing-object
//! refusal.

use futures::{SinkExt, StreamExt};
use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::MirrorStatus;
use nodedb_types::error::sqlstate;
use nodedb_types::id::NodeId;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{
    ArrayCellWrite, ArrayPutBatch, AuthMethod, EdgeDeleteBatch, EdgeKey, EdgePutBatch, EdgeWrite,
    HelloFrame, NativeResponse, OpCode, TextFields,
};
use nodedb_types::value::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use super::helpers::{assert_sqlstate, mark_database_mirror};

pub(super) const READ_ONLY: &str = "read-only mirror";
const STRONG_READ: &str = "redirect strong reads";
const INSERT: &str = "INSERT INTO docs (id, v) VALUES ('b', 2)";
pub(super) const SELECT: &str = "SELECT v FROM docs WHERE id = 'a'";
pub(super) const DATABASE_HEADER: &str = "x-nodedb-database";

/// A server whose client session uses a mirror holding `docs`, `grid` and
/// `events`. `docs` holds the row `a`.
pub(super) async fn fixture() -> (TestServer, String) {
    let (server, name) = TestServer::with_database("mirror_ro").await;
    for statement in [
        "CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)",
        "CREATE ARRAY grid DIMS (row INT64, col INT64) ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)",
        "CREATE TOPIC events WITH (RETENTION = '1 hour')",
        "INSERT INTO docs (id, v) VALUES ('a', 1)",
    ] {
        server
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    mark_database_mirror(&server, &name, MirrorStatus::Following).await;
    (server, name)
}

/// A native session authenticated into `database`.
pub(super) async fn native_session(server: &TestServer, database: &str) -> TcpStream {
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
            database: Some(database.to_string()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native auth: {auth:?}");
    stream
}

pub(super) fn assert_native_refused(response: &NativeResponse, expected: &str) {
    assert_eq!(response.status, ResponseStatus::Error, "{response:?}");
    let text = format!("{response:?}");
    assert!(text.contains(expected), "{text}");
}

/// POST `sql` to an HTTP SQL route in `database` and return the response body.
async fn http_body(server: &TestServer, route: &str, database: &str, sql: &str) -> String {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}{route}", server.http_port))
        .header(DATABASE_HEADER, database)
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {route}: {e}"))
        .text()
        .await
        .unwrap_or_else(|e| panic!("read {route} body: {e}"))
}

/// Send `sql` over WebSocket RPC in `database` and return the error frame's
/// text.
async fn ws_error(server: &TestServer, database: &str, sql: &str) -> String {
    let mut request = format!("ws://127.0.0.1:{}/v1/ws", server.http_port)
        .into_client_request()
        .expect("WebSocket request");
    request
        .headers_mut()
        .insert(DATABASE_HEADER, database.parse().expect("header value"));
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("WebSocket connect");
    let frame = serde_json::json!({ "id": 1, "method": "query", "params": { "sql": sql } });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("WebSocket send");
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), ws.next())
            .await
            .expect("an answer in time")
            .expect("the socket stays open")
            .expect("read a frame");
        let Message::Text(text) = message else {
            continue;
        };
        let answer: serde_json::Value = serde_json::from_str(&text).expect("a JSON frame");
        if answer["id"] == 1 {
            return answer["error"].to_string();
        }
    }
}

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("valid node id")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_writes_are_refused_with_the_read_only_sqlstate() {
    let (server, _name) = fixture().await;
    for statement in [
        INSERT,
        "INSERT INTO docs (id, v) VALUES ('c1', 1), ('c2', 2), ('c3', 3), ('c4', 4), ('c5', 5)",
        "UPDATE docs SET v = 2 WHERE v = 1",
        "DELETE FROM docs WHERE v = 1",
        "TRUNCATE docs",
        "INSERT INTO ARRAY grid COORDS (1, 2) VALUES (0.5)",
    ] {
        assert_sqlstate(
            &server.client,
            statement,
            sqlstate::READ_ONLY_SQL_TRANSACTION,
        )
        .await;
    }
}

/// The refused INSERT draws no surrogate, so no binding stays behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_insert_binds_no_surrogate() {
    let (server, _name) = fixture().await;
    let before = server.shared.surrogate_assigner.current_hwm();
    assert_sqlstate(&server.client, INSERT, sqlstate::READ_ONLY_SQL_TRANSACTION).await;
    assert_eq!(server.shared.surrogate_assigner.current_hwm(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_on_a_mirror_commits_nothing() {
    let (server, _name) = fixture().await;
    server.exec("BEGIN").await.expect("BEGIN");
    let staged = server.exec(INSERT).await;
    let refusal = match staged {
        Err(error) => error,
        Ok(()) => server.exec("COMMIT").await.expect_err("COMMIT is refused"),
    };
    assert!(refusal.contains(READ_ONLY), "{refusal}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_and_topic_publish_are_refused() {
    let (server, _name) = fixture().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rows.ndjson");
    std::fs::write(&path, "{\"id\":\"f\",\"v\":1}\n").expect("write rows");
    let copy = format!("COPY docs FROM '{}'", path.display());
    server.expect_error(&copy, READ_ONLY).await;
    assert_sqlstate(&server.client, &copy, sqlstate::READ_ONLY_SQL_TRANSACTION).await;
    assert_sqlstate(
        &server.client,
        "PUBLISH TO events 'hello'",
        sqlstate::READ_ONLY_SQL_TRANSACTION,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_crdt_apply_refuses_a_valid_delta() {
    let (server, name) = fixture().await;
    let doc = loro::LoroDoc::new();
    let row = doc
        .get_map("docs")
        .insert_container("c1", loro::LoroMap::new())
        .expect("row container");
    row.insert("v", 1).expect("insert field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");

    let body = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/collections/docs/crdt/apply",
            server.http_port
        ))
        .header(DATABASE_HEADER, name.as_str())
        .json(&serde_json::json!({ "doc_id": "c1", "delta": hex::encode(delta) }))
        .send()
        .await
        .expect("POST crdt/apply")
        .text()
        .await
        .expect("read crdt/apply body");
    assert!(body.contains(READ_ONLY), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_routes_refuse_writes() {
    let (server, name) = fixture().await;
    let query = http_body(&server, "/v1/query", &name, INSERT).await;
    assert!(query.contains(READ_ONLY), "{query}");
    let ndjson = http_body(&server, "/v1/query/stream", &name, INSERT).await;
    assert!(ndjson.contains(READ_ONLY), "{ndjson}");
    let ws = ws_error(&server, &name, INSERT).await;
    assert!(ws.contains(READ_ONLY), "{ws}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_and_direct_ops_refuse_writes() {
    let (server, name) = fixture().await;
    let mut stream = native_session(&server, &name).await;

    let sql = send_sql(&mut stream, 2, INSERT).await;
    assert_native_refused(&sql, READ_ONLY);

    let put = send_request(
        &mut stream,
        3,
        OpCode::PointPut,
        TextFields {
            collection: Some("docs".to_string()),
            document_id: Some("g".to_string()),
            data: Some(serde_json::json!({ "v": 3 }).to_string().into_bytes()),
            ..Default::default()
        },
    )
    .await;
    assert_native_refused(&put, READ_ONLY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_batches_refuse_writes() {
    let (server, name) = fixture().await;
    let mut stream = native_session(&server, &name).await;

    let cells = ArrayPutBatch::from_cells(&[ArrayCellWrite {
        coords: vec![Value::Integer(1), Value::Integer(2)],
        attrs: vec![Value::Float(0.5)],
    }])
    .expect("cell batch");
    let array = send_request(
        &mut stream,
        2,
        OpCode::ArrayPutBatch,
        TextFields {
            collection: Some("grid".to_string()),
            payload: Some(zerompk::to_msgpack_vec(&cells).expect("encode cells")),
            ..Default::default()
        },
    )
    .await;
    assert_native_refused(&array, READ_ONLY);

    let put = EdgePutBatch {
        edges: vec![EdgeWrite::new(node("a"), node("b"), "L")],
    };
    let edge_put = send_request(
        &mut stream,
        3,
        OpCode::EdgePutBatch,
        TextFields {
            collection: Some("docs".to_string()),
            payload: Some(zerompk::to_msgpack_vec(&put).expect("encode edges")),
            ..Default::default()
        },
    )
    .await;
    assert_native_refused(&edge_put, READ_ONLY);

    let delete = EdgeDeleteBatch {
        edges: vec![EdgeKey {
            src: node("a"),
            dst: node("b"),
            label: "L".to_string(),
        }],
    };
    let edge_delete = send_request(
        &mut stream,
        4,
        OpCode::EdgeDeleteBatch,
        TextFields {
            collection: Some("docs".to_string()),
            payload: Some(zerompk::to_msgpack_vec(&delete).expect("encode edges")),
            ..Default::default()
        },
    )
    .await;
    assert_native_refused(&edge_delete, READ_ONLY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_ddl_is_refused_on_every_protocol() {
    let (server, name) = fixture().await;
    server
        .expect_error("CREATE COLLECTION more (id TEXT PRIMARY KEY)", READ_ONLY)
        .await;
    server.expect_error("DROP COLLECTION docs", READ_ONLY).await;
    server
        .expect_error("CONVERT COLLECTION docs TO document_schemaless", READ_ONLY)
        .await;

    let http = http_body(
        &server,
        "/v1/query",
        &name,
        "CREATE COLLECTION more (id TEXT PRIMARY KEY)",
    )
    .await;
    assert!(http.contains(READ_ONLY), "{http}");

    let mut stream = native_session(&server, &name).await;
    let native = send_sql(
        &mut stream,
        2,
        "CREATE COLLECTION more (id TEXT PRIMARY KEY)",
    )
    .await;
    assert_native_refused(&native, READ_ONLY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mirror_can_be_dropped_with_its_objects() {
    let (server, name) = fixture().await;
    server.exec("USE DATABASE default").await.expect("USE");
    server
        .exec(&format!("DROP DATABASE {name} CASCADE"))
        .await
        .expect("DROP DATABASE on a mirror");
    let catalog = server.shared.credentials.catalog();
    assert!(
        catalog
            .get_database_id_by_name(&name)
            .expect("catalog lookup")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_opens_every_protocol() {
    let (server, name) = fixture().await;
    server
        .exec(&format!("ALTER DATABASE {name} PROMOTE"))
        .await
        .expect("PROMOTE");

    server.exec(INSERT).await.expect("pgwire write");
    server
        .exec("CREATE COLLECTION more (id TEXT PRIMARY KEY)")
        .await
        .expect("object DDL");
    let http = http_body(
        &server,
        "/v1/query",
        &name,
        "INSERT INTO docs (id, v) VALUES ('h', 1)",
    )
    .await;
    assert!(!http.contains(READ_ONLY), "{http}");
    let mut stream = native_session(&server, &name).await;
    let native = send_sql(&mut stream, 2, "INSERT INTO docs (id, v) VALUES ('n', 1)").await;
    assert_eq!(native.status, ResponseStatus::Ok, "{native:?}");
    let read = send_sql(&mut stream, 3, SELECT).await;
    assert_eq!(read.status, ResponseStatus::Ok, "{read:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strong_reads_are_refused_on_every_protocol() {
    let (server, name) = fixture().await;
    assert_sqlstate(&server.client, SELECT, sqlstate::STALE_READ_NOT_LEADER).await;

    let http = http_body(&server, "/v1/query", &name, SELECT).await;
    assert!(http.contains(STRONG_READ), "{http}");
    let ws = ws_error(&server, &name, SELECT).await;
    assert!(ws.contains(STRONG_READ), "{ws}");

    let mut stream = native_session(&server, &name).await;
    let native = send_sql(&mut stream, 2, SELECT).await;
    assert_native_refused(&native, STRONG_READ);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_eventual_session_reads_a_mirror() {
    let (server, name) = fixture().await;
    server
        .exec("SET default_read_consistency = 'eventual'")
        .await
        .expect("SET on pgwire");
    let rows = server.query_text(SELECT).await.expect("pgwire read");
    assert_eq!(rows.len(), 1, "{rows:?}");

    let mut stream = native_session(&server, &name).await;
    let set = send_sql(&mut stream, 2, "SET default_read_consistency = 'eventual'").await;
    assert_eq!(set.status, ResponseStatus::Ok, "{set:?}");
    let read = send_sql(&mut stream, 3, SELECT).await;
    assert_eq!(read.status, ResponseStatus::Ok, "{read:?}");
}
