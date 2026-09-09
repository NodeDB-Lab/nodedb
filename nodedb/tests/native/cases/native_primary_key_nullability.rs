// SPDX-License-Identifier: BUSL-1.1

//! A declared `PRIMARY KEY` implies `NOT NULL` on the native protocol too.
//!
//! The native direct-op builders construct a plan without going through SQL
//! planning, so they need the same guard the SQL path carries. An UPDATE that
//! nulls a declared key leaves a row whose own key no longer identifies it.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::error::sqlstate;
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::{OpCode, ResponseStatus};
use nodedb_types::protocol::text_fields::TextFields;
use tokio::net::TcpStream;

/// MessagePack encoding of `null`.
const MSGPACK_NULL: u8 = 0xC0;

/// MessagePack encoding of an empty array — a filter list matching every row.
const MSGPACK_EMPTY_ARRAY: u8 = 0x90;

async fn seeded_session(server: &NativeTestServer, collection: &str) -> TcpStream {
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let create = send_sql(
        &mut stream,
        1,
        &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v INT)"),
    )
    .await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_point_update_refuses_a_null_primary_key() {
    let server = NativeTestServer::start().await;
    let mut stream = seeded_session(&server, "native_pk_point").await;

    let resp = send_request(
        &mut stream,
        3,
        OpCode::DocumentUpdate,
        TextFields {
            collection: Some("native_pk_point".to_string()),
            document_id: Some("k1".to_string()),
            updates: Some(vec![("id".to_string(), vec![MSGPACK_NULL])]),
            ..Default::default()
        },
    )
    .await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "nulling a declared primary key must be refused"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::NOT_NULL_VIOLATION,
        "expected not_null_violation, got {}",
        err.code
    );

    let read = send_sql(&mut stream, 4, "SELECT id FROM native_pk_point").await;
    assert_ne!(read.status, ResponseStatus::Error, "read back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_bulk_update_refuses_a_null_primary_key() {
    let server = NativeTestServer::start().await;
    let mut stream = seeded_session(&server, "native_pk_bulk").await;

    let resp = send_request(
        &mut stream,
        3,
        OpCode::DocumentBulkUpdate,
        TextFields {
            collection: Some("native_pk_bulk".to_string()),
            filters: Some(vec![MSGPACK_EMPTY_ARRAY]),
            updates: Some(vec![("id".to_string(), vec![MSGPACK_NULL])]),
            ..Default::default()
        },
    )
    .await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "nulling a declared primary key must be refused"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::NOT_NULL_VIOLATION,
        "expected not_null_violation, got {}",
        err.code
    );
}
