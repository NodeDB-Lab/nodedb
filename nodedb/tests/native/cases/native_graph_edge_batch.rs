// SPDX-License-Identifier: BUSL-1.1

//! The native `EdgePutBatch` and `EdgeDeleteBatch` opcodes, end to end.
//!
//! Each request runs the `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` handler:
//! one unit, its count in `rows_affected`, every edge on both of its homes,
//! and the per-statement edge cap refusing an oversized request whole.

use std::collections::{BTreeSet, HashMap};

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::id::NodeId;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{
    EdgeDeleteBatch, EdgeKey, EdgePutBatch, EdgeWrite, HelloFrame, MAX_EDGES_PER_STATEMENT,
    NativeResponse, OpCode,
};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

struct Client {
    stream: TcpStream,
    seq: u64,
}

impl Client {
    async fn connect(server: &NativeTestServer) -> Self {
        let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
            .await
            .expect("handshake");
        Self { stream, seq: 0 }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    async fn sql(&mut self, sql: &str) -> NativeResponse {
        let seq = self.next_seq();
        let response = send_sql(&mut self.stream, seq, sql).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{sql} must succeed: {response:?}"
        );
        response
    }

    async fn send(&mut self, op: OpCode, collection: &str, payload: Vec<u8>) -> NativeResponse {
        let seq = self.next_seq();
        let fields = TextFields {
            collection: Some(collection.to_string()),
            payload: Some(payload),
            ..TextFields::default()
        };
        send_request(&mut self.stream, seq, op, fields).await
    }

    async fn put(&mut self, collection: &str, edges: Vec<EdgeWrite>) -> NativeResponse {
        let payload = zerompk::to_msgpack_vec(&EdgePutBatch { edges }).expect("encode");
        self.send(OpCode::EdgePutBatch, collection, payload).await
    }

    async fn delete(&mut self, collection: &str, edges: Vec<EdgeKey>) -> NativeResponse {
        let payload = zerompk::to_msgpack_vec(&EdgeDeleteBatch { edges }).expect("encode");
        self.send(OpCode::EdgeDeleteBatch, collection, payload)
            .await
    }

    /// The node ids `GRAPH NEIGHBORS` returns.
    async fn neighbors(
        &mut self,
        collection: &str,
        node: &str,
        direction: &str,
    ) -> BTreeSet<String> {
        let response = self
            .sql(&format!(
                "GRAPH NEIGHBORS IN '{collection}' OF '{node}' LABEL 'L' DIRECTION {direction}"
            ))
            .await;
        let mut out = BTreeSet::new();
        for cell in response.rows.unwrap_or_default().into_iter().flatten() {
            let Value::String(text) = cell else { continue };
            let entries: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap_or_default();
            for entry in entries {
                if let Some(id) = entry.get("node").and_then(|value| value.as_str()) {
                    out.insert(id.to_string());
                }
            }
        }
        out
    }
}

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("valid node id")
}

/// `n` edges `src<i> -> dst<i>`, labelled `L`, endpoints over many vShards.
fn writes(n: usize) -> Vec<EdgeWrite> {
    (0..n)
        .map(|i| {
            let mut edge = EdgeWrite::new(node(&format!("src{i}")), node(&format!("dst{i}")), "L");
            edge.properties = HashMap::from([("w".to_string(), Value::Integer(i as i64))]);
            edge
        })
        .collect()
}

fn keys(n: usize) -> Vec<EdgeKey> {
    (0..n)
        .map(|i| EdgeKey {
            src: node(&format!("src{i}")),
            dst: node(&format!("dst{i}")),
            label: "L".to_string(),
        })
        .collect()
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

fn assert_ok_count(response: &NativeResponse, count: u64, what: &str) {
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "{what} must succeed: {response:?}"
    );
    assert_eq!(response.rows_affected, Some(count), "{what}: {response:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_batch_lands_every_edge_and_delete_batch_removes_them() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.sql("CREATE COLLECTION nb_edges").await;

    let put = client.put("nb_edges", writes(30)).await;
    assert_ok_count(&put, 30, "a put batch of 30 edges");
    for i in [0, 13, 29] {
        assert_eq!(
            client
                .neighbors("nb_edges", &format!("src{i}"), "out")
                .await,
            set(&[&format!("dst{i}")]),
            "forward row of edge {i}"
        );
        assert_eq!(
            client.neighbors("nb_edges", &format!("dst{i}"), "in").await,
            set(&[&format!("src{i}")]),
            "reverse row of edge {i}"
        );
    }

    // One key names no stored edge: the count is the edges that were live.
    let mut doomed = keys(30);
    doomed.push(EdgeKey {
        src: node("nobody"),
        dst: node("nowhere"),
        label: "L".to_string(),
    });
    let delete = client.delete("nb_edges", doomed).await;
    assert_ok_count(&delete, 30, "a delete batch over 30 live edges");
    for i in [0, 13, 29] {
        assert!(
            client
                .neighbors("nb_edges", &format!("src{i}"), "out")
                .await
                .is_empty(),
            "forward row of edge {i} is gone"
        );
        assert!(
            client
                .neighbors("nb_edges", &format!("dst{i}"), "in")
                .await
                .is_empty(),
            "reverse row of edge {i} is gone"
        );
    }

    let again = client.delete("nb_edges", keys(30)).await;
    assert_ok_count(&again, 0, "deleting the same edges again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_over_the_edge_cap_is_refused_whole() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.sql("CREATE COLLECTION nb_cap").await;

    let over = MAX_EDGES_PER_STATEMENT + 1;
    for (op, response) in [
        ("EdgePutBatch", client.put("nb_cap", writes(over)).await),
        ("EdgeDeleteBatch", client.delete("nb_cap", keys(over)).await),
    ] {
        assert_eq!(
            response.status,
            ResponseStatus::Error,
            "{op} over the cap must be refused: {response:?}"
        );
        let error = response.error.expect("a refusal carries its error");
        assert_eq!(error.code, "54000", "{op}: {error:?}");
        assert!(
            error.message.contains(&format!("{over} edges"))
                && error
                    .message
                    .contains(&format!("cap is {MAX_EDGES_PER_STATEMENT}"))
                && error.message.contains("split the batch"),
            "{op}: {error:?}"
        );
    }
    assert!(
        client.neighbors("nb_cap", "src0", "out").await.is_empty(),
        "a refused put batch writes none of its edges"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_request_names_its_fault() {
    let server = NativeTestServer::start().await;
    let mut client = Client::connect(&server).await;

    let seq = client.next_seq();
    let missing = send_request(
        &mut client.stream,
        seq,
        OpCode::EdgePutBatch,
        TextFields {
            payload: Some(Vec::new()),
            ..TextFields::default()
        },
    )
    .await;
    let error = missing
        .error
        .expect("a request with no collection is refused");
    assert_eq!(error.code, "42601", "{error:?}");
    assert!(error.message.contains("'collection'"), "{error:?}");

    let garbage = client
        .send(OpCode::EdgeDeleteBatch, "nb_bad", vec![0xc1])
        .await;
    let error = garbage.error.expect("an undecodable payload is refused");
    assert_eq!(error.code, "22P02", "{error:?}");
}
