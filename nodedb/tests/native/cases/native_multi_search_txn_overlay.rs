// SPDX-License-Identifier: BUSL-1.1

//! A native multi-field vector search (`OpCode::VectorMultiSearch`) inside a
//! transaction block sees the block's own staged writes, as a single-field
//! vector search does: a staged insert ranks by its staged vector, and a
//! staged delete hides its row. After COMMIT a search outside the block
//! reads the committed state.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

const COLL: &str = "native_mvo";

async fn multi_search(stream: &mut TcpStream, seq: u64) -> NativeResponse {
    let response = send_request(
        stream,
        seq,
        OpCode::VectorMultiSearch,
        TextFields {
            collection: Some(COLL.to_string()),
            query_vector: Some(vec![1.0, 0.0, 0.0]),
            top_k: Some(5),
            ..Default::default()
        },
    )
    .await;
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "multi-search must succeed: {response:?}"
    );
    response
}

/// Whether any cell of the response is the string `id`.
fn holds(response: &NativeResponse, id: &str) -> bool {
    response
        .rows
        .iter()
        .flatten()
        .flatten()
        .any(|v| *v == Value::String(id.into()))
}

async fn sql_ok(stream: &mut TcpStream, seq: u64, sql: &str) {
    let response = send_sql(stream, seq, sql).await;
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "`{sql}` must succeed: {response:?}"
    );
}

#[tokio::test]
async fn native_multi_search_merges_the_transaction_overlay() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    sql_ok(&mut stream, 1, &format!("CREATE COLLECTION {COLL}")).await;
    sql_ok(
        &mut stream,
        2,
        &format!("CREATE VECTOR INDEX idx_{COLL}_emb ON {COLL} METRIC l2 DIM 3"),
    )
    .await;
    sql_ok(
        &mut stream,
        3,
        &format!("INSERT INTO {COLL} (id, embedding) VALUES ('a', ARRAY[1.0, 0.0, 0.0])"),
    )
    .await;

    let baseline = multi_search(&mut stream, 4).await;
    assert!(
        holds(&baseline, "a"),
        "the committed row ranks: {baseline:?}"
    );

    sql_ok(&mut stream, 5, "BEGIN").await;
    sql_ok(
        &mut stream,
        6,
        &format!("INSERT INTO {COLL} (id, embedding) VALUES ('b', ARRAY[0.9, 0.1, 0.0])"),
    )
    .await;
    sql_ok(
        &mut stream,
        7,
        &format!("DELETE FROM {COLL} WHERE id = 'a'"),
    )
    .await;

    let in_txn = multi_search(&mut stream, 8).await;
    assert!(
        holds(&in_txn, "b"),
        "the staged insert ranks inside the block: {in_txn:?}"
    );
    assert!(
        !holds(&in_txn, "a"),
        "the staged delete hides the row inside the block: {in_txn:?}"
    );

    sql_ok(&mut stream, 9, "COMMIT").await;

    let after = multi_search(&mut stream, 10).await;
    server.shutdown().await;
    assert!(holds(&after, "b"), "the committed insert ranks: {after:?}");
    assert!(!holds(&after, "a"), "the committed delete holds: {after:?}");
}
