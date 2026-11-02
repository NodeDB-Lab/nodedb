// SPDX-License-Identifier: BUSL-1.1

//! Native direct ops inside a transaction block.
//!
//! A direct write stages into the block: a read in the block sees it,
//! ROLLBACK removes it, and COMMIT persists it. A direct read reads the
//! block's snapshot with the block's staged writes merged.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{BatchDocument, HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

async fn connect(server: &NativeTestServer) -> TcpStream {
    let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    stream
}

async fn ok_sql(stream: &mut TcpStream, seq: u64, sql: &str) -> NativeResponse {
    let resp = send_sql(stream, seq, sql).await;
    assert_ne!(resp.status, ResponseStatus::Error, "{sql}: {resp:?}");
    resp
}

fn ok(resp: NativeResponse, what: &str) -> NativeResponse {
    assert_ne!(resp.status, ResponseStatus::Error, "{what}: {resp:?}");
    resp
}

fn row_count(resp: &NativeResponse) -> usize {
    resp.rows.as_ref().map_or(0, Vec::len)
}

async fn count_rows(stream: &mut TcpStream, seq: u64, collection: &str) -> usize {
    let resp = ok_sql(stream, seq, &format!("SELECT id FROM {collection}")).await;
    row_count(&resp)
}

async fn insert_select(stream: &mut TcpStream, seq: u64, target: &str, source: &str) {
    let resp = send_request(
        stream,
        seq,
        OpCode::DocumentInsertSelect,
        TextFields {
            collection: Some(target.to_string()),
            source_collection: Some(source.to_string()),
            ..Default::default()
        },
    )
    .await;
    ok(resp, "DocumentInsertSelect");
}

/// A direct `INSERT ... SELECT` inside a block stages its rows. ROLLBACK
/// removes them, and COMMIT persists them.
#[tokio::test]
async fn native_insert_select_in_a_block_stages() {
    let server = NativeTestServer::start().await;
    let mut s = connect(&server).await;
    ok_sql(&mut s, 1, "CREATE COLLECTION nis_src").await;
    ok_sql(&mut s, 2, "CREATE COLLECTION nis_dst").await;
    ok_sql(&mut s, 3, "INSERT INTO nis_src (id, v) VALUES ('a', 1)").await;
    ok_sql(&mut s, 4, "INSERT INTO nis_src (id, v) VALUES ('b', 2)").await;

    ok_sql(&mut s, 5, "BEGIN").await;
    insert_select(&mut s, 6, "nis_dst", "nis_src").await;
    assert_eq!(
        count_rows(&mut s, 7, "nis_dst").await,
        2,
        "the block sees its rows"
    );
    ok_sql(&mut s, 8, "ROLLBACK").await;
    assert_eq!(
        count_rows(&mut s, 9, "nis_dst").await,
        0,
        "ROLLBACK removes the staged rows"
    );

    ok_sql(&mut s, 10, "BEGIN").await;
    insert_select(&mut s, 11, "nis_dst", "nis_src").await;
    assert_eq!(count_rows(&mut s, 12, "nis_dst").await, 2);
    ok_sql(&mut s, 13, "COMMIT").await;
    assert_eq!(
        count_rows(&mut s, 14, "nis_dst").await,
        2,
        "COMMIT persists the rows"
    );
    server.shutdown().await;
}

async fn batch_insert(stream: &mut TcpStream, seq: u64, collection: &str, ids: &[&str]) {
    let documents = ids
        .iter()
        .map(|id| BatchDocument {
            id: id.to_string(),
            fields: serde_json::json!({ "v": 1 }),
        })
        .collect();
    let resp = send_request(
        stream,
        seq,
        OpCode::DocumentBatchInsert,
        TextFields {
            collection: Some(collection.to_string()),
            documents: Some(documents),
            ..Default::default()
        },
    )
    .await;
    ok(resp, "DocumentBatchInsert");
}

/// A direct batch insert inside a block stages each row. ROLLBACK removes
/// them, and COMMIT persists them.
#[tokio::test]
async fn native_batch_insert_in_a_block_stages() {
    let server = NativeTestServer::start().await;
    let mut s = connect(&server).await;
    ok_sql(&mut s, 1, "CREATE COLLECTION nbi").await;

    ok_sql(&mut s, 2, "BEGIN").await;
    batch_insert(&mut s, 3, "nbi", &["a", "b"]).await;
    assert_eq!(
        count_rows(&mut s, 4, "nbi").await,
        2,
        "the block sees its rows"
    );
    ok_sql(&mut s, 5, "ROLLBACK").await;
    assert_eq!(
        count_rows(&mut s, 6, "nbi").await,
        0,
        "ROLLBACK removes the staged rows"
    );

    ok_sql(&mut s, 7, "BEGIN").await;
    batch_insert(&mut s, 8, "nbi", &["c", "d"]).await;
    assert_eq!(count_rows(&mut s, 9, "nbi").await, 2);
    ok_sql(&mut s, 10, "COMMIT").await;
    assert_eq!(
        count_rows(&mut s, 11, "nbi").await,
        2,
        "COMMIT persists the rows"
    );
    server.shutdown().await;
}

async fn graph_match(stream: &mut TcpStream, seq: u64) -> usize {
    let resp = send_request(
        stream,
        seq,
        OpCode::GraphMatch,
        TextFields {
            match_query: Some("MATCH (x)-[:knows]->(y) IN 'ngm' RETURN x, y".to_string()),
            ..Default::default()
        },
    )
    .await;
    row_count(&ok(resp, "GraphMatch"))
}

/// A direct MATCH inside a block reads the block's snapshot and the edges
/// the block staged. An edge a second connection commits after the snapshot
/// stays invisible to the block.
#[tokio::test]
async fn native_match_in_a_block_reads_its_snapshot() {
    let server = NativeTestServer::start().await;
    let mut a = connect(&server).await;
    let mut b = connect(&server).await;
    ok_sql(&mut a, 1, "CREATE COLLECTION ngm").await;
    ok_sql(
        &mut a,
        2,
        "GRAPH INSERT EDGE IN 'ngm' FROM 'p1' TO 'p2' TYPE 'knows'",
    )
    .await;

    ok_sql(&mut a, 3, "BEGIN").await;
    assert_eq!(graph_match(&mut a, 4).await, 1);
    ok_sql(
        &mut b,
        1,
        "GRAPH INSERT EDGE IN 'ngm' FROM 'p3' TO 'p4' TYPE 'knows'",
    )
    .await;
    assert_eq!(
        graph_match(&mut a, 5).await,
        1,
        "the concurrent edge is past the block's snapshot"
    );
    ok_sql(
        &mut a,
        6,
        "GRAPH INSERT EDGE IN 'ngm' FROM 'p5' TO 'p6' TYPE 'knows'",
    )
    .await;
    assert_eq!(
        graph_match(&mut a, 7).await,
        2,
        "the block reads its own staged edge"
    );
    ok_sql(&mut a, 8, "ROLLBACK").await;
    assert_eq!(
        graph_match(&mut a, 9).await,
        2,
        "the committed edges remain and the staged edge is gone"
    );
    server.shutdown().await;
}
