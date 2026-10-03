// SPDX-License-Identifier: BUSL-1.1

//! A native document update keeps the row's identity column.
//!
//! The row stays stored under its document id, so an `id` assignment naming
//! another value splits SQL reads (which read the column) from key reads
//! (which read the key). The update is refused; assigning the current id is
//! accepted.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::Value;
use nodedb_types::error::sqlstate;
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::{OpCode, ResponseStatus};
use nodedb_types::protocol::text_fields::TextFields;
use tokio::net::TcpStream;

fn packed(value: &str) -> Vec<u8> {
    nodedb_types::value_to_msgpack(&Value::String(value.into())).expect("encode")
}

async fn seeded_session(server: &NativeTestServer, collection: &str) -> TcpStream {
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let create = send_sql(&mut stream, 1, &format!("CREATE COLLECTION {collection}")).await;
    assert_ne!(create.status, ResponseStatus::Error, "create {collection}");
    let insert = send_sql(
        &mut stream,
        2,
        &format!("INSERT INTO {collection} (id, v) VALUES ('k1', 1)"),
    )
    .await;
    assert_ne!(insert.status, ResponseStatus::Error, "seed {collection}");
    stream
}

async fn update_id(stream: &mut TcpStream, seq: u64, collection: &str, id: &str) -> ResponseStatus {
    let resp = send_request(
        stream,
        seq,
        OpCode::DocumentUpdate,
        TextFields {
            collection: Some(collection.to_string()),
            document_id: Some("k1".to_string()),
            updates: Some(vec![("id".to_string(), packed(id))]),
            ..Default::default()
        },
    )
    .await;
    if resp.status == ResponseStatus::Error {
        let err = resp.error.expect("error payload expected");
        assert_eq!(
            err.code,
            sqlstate::INTEGRITY_CONSTRAINT_VIOLATION,
            "an identity change is an integrity violation, got {}: {}",
            err.code,
            err.message
        );
    }
    resp.status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_update_refuses_an_id_naming_another_document() {
    let server = NativeTestServer::start().await;
    let mut stream = seeded_session(&server, "native_id_move").await;

    assert_eq!(
        update_id(&mut stream, 3, "native_id_move", "k2").await,
        ResponseStatus::Error,
        "an id differing from the document id must be refused"
    );

    let read = send_sql(&mut stream, 4, "SELECT id FROM native_id_move").await;
    assert_eq!(
        read.rows,
        Some(vec![vec![Value::String("k1".into())]]),
        "the stored id must still name the key: {read:?}"
    );
    let by_key = send_sql(
        &mut stream,
        5,
        "SELECT v FROM native_id_move WHERE id = 'k1'",
    )
    .await;
    assert_eq!(
        by_key.rows,
        Some(vec![vec![Value::Integer(1)]]),
        "the row stays addressable by its key: {by_key:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_update_accepts_the_current_id() {
    let server = NativeTestServer::start().await;
    let mut stream = seeded_session(&server, "native_id_keep").await;

    assert_ne!(
        update_id(&mut stream, 3, "native_id_keep", "k1").await,
        ResponseStatus::Error,
        "assigning the document's own id keeps its identity"
    );
}
