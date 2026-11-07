// SPDX-License-Identifier: BUSL-1.1

//! Every client write path into a database with a sync mirror waits for it.
//!
//! The fixture holds a sync pin whose link confirms nothing, so a path that waits
//! answers `MIRROR_SYNC_TIMEOUT` with its write committed. A path that skips the
//! wait answers success, and its test fails.

use futures::SinkExt;
use nodedb::control::promql::remote_proto::{Label, Sample, TimeSeries, WriteRequest};
use nodedb_test_support::native_harness::{send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::error::sqlstate;
use nodedb_types::id::NodeId;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{
    ArrayCellWrite, ArrayPutBatch, EdgeDeleteBatch, EdgeKey, EdgePutBatch, EdgeWrite, OpCode,
    TextFields,
};
use nodedb_types::value::Value;
use nodedb_types::{MirrorMode, MirrorPin, MirrorPinState};
use prost::Message;

use super::test_read_only_protocols::{DATABASE_HEADER, assert_native_refused, native_session};

const COMMITTED: &str = "committed";
const SYNC_TIMEOUT_MS: u64 = 200;

/// A server whose client session uses a database with a sync mirror that
/// confirms nothing. The objects exist before the pin, so their DDL waits on
/// nothing either way.
async fn fixture() -> (TestServer, String) {
    let server = TestServer::start_with_mirror_sync_timeout(SYNC_TIMEOUT_MS).await;
    let name = "sync_paths".to_string();
    for statement in [
        format!("CREATE DATABASE {name}"),
        format!("USE DATABASE {name}"),
        "CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)".to_string(),
        "CREATE COLLECTION notes (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')".to_string(),
        "CREATE COLLECTION counters (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')".to_string(),
        "CREATE COLLECTION scores (key TEXT PRIMARY KEY, n FLOAT) WITH (engine='kv')".to_string(),
        "CREATE COLLECTION tokens (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')"
            .to_string(),
        "CREATE COLLECTION accounts (key TEXT PRIMARY KEY, balance INT) WITH (engine='kv')"
            .to_string(),
        "INSERT INTO accounts (key, balance) VALUES ('a', 100)".to_string(),
        "INSERT INTO accounts (key, balance) VALUES ('b', 0)".to_string(),
        "CREATE ARRAY grid DIMS (row INT64, col INT64) ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)"
            .to_string(),
        "CREATE TOPIC events WITH (RETENTION = '1 hour')".to_string(),
        "CREATE PROCEDURE fill_docs() AS BEGIN INSERT INTO docs (id, v) VALUES ('proc', 1); END"
            .to_string(),
    ] {
        server
            .exec(&statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let database = server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(&name)
        .expect("catalog lookup")
        .expect("database exists");
    server.shared.mirror.pins.apply(&MirrorPin {
        link_id: 1,
        client: "sync-paths".to_string(),
        database: database.as_u64(),
        positions: Vec::new(),
        acked_ms: 0,
        state: MirrorPinState::Live,
        mode: MirrorMode::Sync,
        acked_ordinal: 1,
    });
    (server, name)
}

fn assert_committed_timeout(what: &str, error: &tokio_postgres::Error) {
    let error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{what}: expected a typed error, got {error:?}"));
    assert_eq!(
        error.code().code(),
        sqlstate::MIRROR_SYNC_TIMEOUT,
        "{what}: {error:?}"
    );
    assert!(error.message().contains(COMMITTED), "{what}: {error:?}");
}

async fn assert_times_out(server: &TestServer, sql: &str) {
    let error = server
        .client
        .simple_query(sql)
        .await
        .expect_err(&format!("{sql} must wait for the sync mirror"));
    assert_committed_timeout(sql, &error);
}

async fn ids(server: &TestServer) -> Vec<String> {
    server
        .query_text("SELECT id FROM docs")
        .await
        .expect("read docs")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_document_write_form_waits_and_its_row_stays() {
    let (server, _) = fixture().await;
    for (sql, id) in [
        ("UPSERT INTO docs (id, v) VALUES ('values', 1)", "values"),
        ("UPSERT INTO docs { id: 'upsert', v: 1 }", "upsert"),
        ("INSERT INTO docs { id: 'insert', v: 1 }", "insert"),
    ] {
        assert_times_out(&server, sql).await;
        assert!(
            ids(&server).await.contains(&id.to_string()),
            "{sql}: the write stays committed"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_graph_write_waits_and_a_graph_read_does_not() {
    let (server, _) = fixture().await;
    for sql in [
        "GRAPH INSERT EDGE IN 'docs' FROM 'a' TO 'b' TYPE 'knows'",
        "GRAPH LABEL 'a' AS 'Person'",
        "GRAPH DELETE EDGE IN 'docs' FROM 'a' TO 'b' TYPE 'knows'",
    ] {
        assert_times_out(&server, sql).await;
    }
    server
        .query_text("GRAPH NEIGHBORS IN 'docs' OF 'a'")
        .await
        .expect("a graph read waits for nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_atomic_function_waits() {
    let (server, _) = fixture().await;
    for sql in [
        "SELECT KV_INCR('counters', 'k', 1)",
        "SELECT KV_DECR('counters', 'k', 1)",
        "SELECT KV_INCR_FLOAT('scores', 's', 2.5)",
        "SELECT KV_GETSET('tokens', 'k', 'first')",
        "SELECT KV_CAS('tokens', 'k', 'first', 'second')",
        "SELECT TRANSFER('accounts', 'a', 'b', 'balance', 10)",
    ] {
        assert_times_out(&server, sql).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_and_a_procedure_call_wait() {
    let (server, _) = fixture().await;
    assert_times_out(&server, "PUBLISH TO events 'hello'").await;
    assert_times_out(&server, "CALL fill_docs()").await;
    assert!(ids(&server).await.contains(&"proc".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_waits_at_its_commit() {
    let (server, _) = fixture().await;
    server.exec("BEGIN").await.expect("BEGIN");
    server
        .exec("UPSERT INTO docs { id: 'block', v: 1 }")
        .await
        .expect("a staged write answers at once");
    assert_times_out(&server, "COMMIT").await;
    assert!(ids(&server).await.contains(&"block".to_string()));

    // A topic message is the block's only write.
    server.exec("BEGIN").await.expect("BEGIN");
    server
        .exec("PUBLISH TO events 'held'")
        .await
        .expect("a held message answers at once");
    assert_times_out(&server, "COMMIT").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_a_file_waits() {
    let (server, _) = fixture().await;
    let file = tempfile::Builder::new()
        .suffix(".ndjson")
        .tempfile()
        .expect("create the import file");
    std::fs::write(file.path(), "{\"id\": \"file\", \"v\": 1}\n").expect("write the import file");
    let path = file.path().to_string_lossy();

    assert_times_out(&server, &format!("COPY docs FROM '{path}'")).await;
    assert!(ids(&server).await.contains(&"file".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_into_an_array_from_stdin_waits() {
    let (server, _) = fixture().await;
    let sink = server
        .client
        .copy_in::<_, bytes::Bytes>("COPY grid FROM STDIN WITH (FORMAT csv)")
        .await
        .expect("open the COPY");
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(bytes::Bytes::from_static(b"1,2,0.5\n"))
        .await
        .expect("send the row");
    let error = sink
        .as_mut()
        .finish()
        .await
        .expect_err("COPY must wait for the sync mirror");
    assert_committed_timeout("COPY FROM STDIN", &error);
}

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("valid node id")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_batches_and_router_writes_wait() {
    let (server, name) = fixture().await;
    let mut stream = native_session(&server, &name).await;

    let cells = ArrayPutBatch::from_cells(&[ArrayCellWrite {
        coords: vec![Value::Integer(3), Value::Integer(4)],
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
    assert_native_refused(&array, COMMITTED);

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
    assert_native_refused(&edge_put, COMMITTED);

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
    assert_native_refused(&edge_delete, COMMITTED);

    let sql = send_sql(&mut stream, 5, "UPSERT INTO docs { id: 'native', v: 1 }").await;
    assert_native_refused(&sql, COMMITTED);

    let read = send_sql(&mut stream, 6, "SELECT id FROM docs").await;
    assert_eq!(read.status, ResponseStatus::Ok, "{read:?}");
}

/// A Loro delta that writes row `c1` of `notes`.
fn note_delta_hex() -> String {
    let doc = loro::LoroDoc::new();
    let row = doc
        .get_map("notes")
        .insert_container("c1", loro::LoroMap::new())
        .expect("row container");
    row.insert("id", "c1").expect("id field");
    row.insert("v", 1).expect("v field");
    doc.commit();
    hex::encode(
        doc.export(loro::ExportMode::Snapshot)
            .expect("export loro snapshot"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_crdt_apply_waits() {
    let (server, _) = fixture().await;
    let delta = note_delta_hex();
    assert_times_out(
        &server,
        &format!("SELECT crdt_apply('notes', 'c1', '{delta}')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_crdt_apply_waits() {
    let (server, name) = fixture().await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/collections/notes/crdt/apply",
            server.http_port
        ))
        .header(DATABASE_HEADER, name.as_str())
        .json(&serde_json::json!({ "doc_id": "c1", "delta": note_delta_hex() }))
        .send()
        .await
        .expect("POST crdt/apply");
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let body = response.text().await.expect("read crdt/apply body");
    assert!(body.contains(COMMITTED), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prometheus_remote_write_waits_with_a_status_it_does_not_retry() {
    let (server, name) = fixture().await;
    let timestamp = i64::try_from(super::helpers::now_ms()).expect("the clock fits an i64");
    let mut encoded = Vec::new();
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: "sync_metric".into(),
            }],
            samples: vec![Sample {
                value: 1.0,
                timestamp,
            }],
            exemplars: vec![],
        }],
    }
    .encode(&mut encoded)
    .expect("encode write request");
    let body = snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .expect("snappy compress");
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/obsv/api/v1/write",
            server.http_port
        ))
        .header(DATABASE_HEADER, name.as_str())
        .header("Content-Encoding", "snappy")
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("POST remote write");
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let body = response.text().await.expect("read remote write body");
    assert!(body.contains(COMMITTED), "{body}");
}
