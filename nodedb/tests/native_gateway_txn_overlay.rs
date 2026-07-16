// SPDX-License-Identifier: BUSL-1.1

//! Native gateway transaction-overlay regressions:
//!
//! 1. `OpCode::Begin` as the first post-handshake frame must create a session
//!    and enter `InBlock`, so the following write is staged (not autocommitted).
//! 2. Planned SQL reads inside an explicit transaction must resolve the
//!    session staging overlay when dispatch goes through the gateway (local
//!    SPSC with `txn_id`), matching the direct-op path already covered by
//!    `native_direct_op_txn_overlay.rs`.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use common::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, OpCode};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Open a native session against the server's native listener.
async fn native_session(srv: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// `OpCode::Begin` with empty fields — first application frame after handshake.
async fn send_begin_opcode(
    stream: &mut TcpStream,
    seq: u64,
) -> nodedb_types::protocol::NativeResponse {
    send_request(stream, seq, OpCode::Begin, TextFields::default()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_first_frame_begin_buffers_following_write() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    // First application frame: binary/protocol BEGIN, not SQL text.
    // Pre-fix, `handle_begin` called `run_begin` without `ensure_session`,
    // so SessionStore::begin silently no-oped and the following INSERT
    // autocommitted outside a transaction block.
    let begin = send_begin_opcode(&mut stream, 1).await;
    assert_ne!(
        begin.status,
        ResponseStatus::Error,
        "first-frame OpCode::Begin must succeed: {begin:?}"
    );

    let create = send_sql(
        &mut stream,
        2,
        "CREATE COLLECTION native_ff_begin (id STRING PRIMARY KEY, n INT) \
         WITH (engine='document_schemaless')",
    )
    .await;
    assert_ne!(
        create.status,
        ResponseStatus::Error,
        "CREATE must succeed: {create:?}"
    );

    let insert = send_sql(
        &mut stream,
        3,
        "INSERT INTO native_ff_begin (id, n) VALUES ('a', 1)",
    )
    .await;
    assert_ne!(
        insert.status,
        ResponseStatus::Error,
        "in-block INSERT must succeed: {insert:?}"
    );

    // Mid-transaction SELECT must see the staged row.
    let select = send_sql(
        &mut stream,
        4,
        "SELECT n FROM native_ff_begin WHERE id = 'a'",
    )
    .await;
    assert_eq!(
        select.status,
        ResponseStatus::Ok,
        "in-txn SELECT must succeed: {select:?}"
    );
    let rows = select
        .rows
        .expect("in-txn SELECT must return rows for the staged write");
    assert_eq!(
        rows.first().and_then(|r| r.first()).cloned(),
        Some(Value::Integer(1)),
        "first-frame BEGIN must leave the session InBlock so the write is staged \
         and visible to the same-connection SELECT"
    );

    let rollback = send_sql(&mut stream, 5, "ROLLBACK").await;
    assert_ne!(
        rollback.status,
        ResponseStatus::Error,
        "ROLLBACK must succeed: {rollback:?}"
    );

    // After ROLLBACK the staged row must not be durable.
    let after = send_sql(
        &mut stream,
        6,
        "SELECT n FROM native_ff_begin WHERE id = 'a'",
    )
    .await;
    server.shutdown().await;
    assert_eq!(
        after.status,
        ResponseStatus::Ok,
        "post-rollback SELECT: {after:?}"
    );
    let after_rows = after.rows.unwrap_or_default();
    assert!(
        after_rows.is_empty()
            || after_rows
                .iter()
                .all(|r| r.iter().all(|v| matches!(v, Value::Null))),
        "ROLLBACK must discard the staged write, got: {after_rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_in_txn_sql_select_reads_own_write() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_gw_ryow (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin.status, ResponseStatus::Ok, "BEGIN: {begin:?}");

    seq += 1;
    let insert = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_gw_ryow (id, n) VALUES ('x', 42)",
    )
    .await;
    assert_eq!(
        insert.status,
        ResponseStatus::Ok,
        "in-tx INSERT: {insert:?}"
    );

    // Gateway-routed planned SQL SELECT must resolve the staging overlay.
    seq += 1;
    let select = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_gw_ryow WHERE id = 'x'",
    )
    .await;
    assert_eq!(
        select.status,
        ResponseStatus::Ok,
        "in-tx SELECT: {select:?}"
    );
    let rows = select
        .rows
        .expect("in-tx SELECT must return the staged row");
    assert_eq!(
        rows.first().and_then(|r| r.first()).cloned(),
        Some(Value::Integer(42)),
        "native SQL SELECT inside a transaction must see its own staged write \
         when dispatch goes through the gateway"
    );

    seq += 1;
    let rollback = send_sql(&mut stream, seq, "ROLLBACK").await;
    assert_eq!(
        rollback.status,
        ResponseStatus::Ok,
        "ROLLBACK: {rollback:?}"
    );

    seq += 1;
    let after = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_gw_ryow WHERE id = 'x'",
    )
    .await;
    assert_eq!(after.status, ResponseStatus::Ok);
    let after_rows = after.rows.unwrap_or_default();
    assert!(
        after_rows.is_empty()
            || after_rows
                .iter()
                .all(|r| r.iter().all(|v| matches!(v, Value::Null))),
        "ROLLBACK must hide the staged write, got: {after_rows:?}"
    );
}
