// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_graph_edges_written_total` and `nodedb_graph_edges_deleted_total`
//! on `/metrics` count the edges a write applied, once per edge.
//!
//! The statements cover a single edge, several edges in one statement, a
//! delete of a live and of an absent edge, a committed transaction, and a
//! rolled-back one.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

const WRITTEN: &str = "nodedb_graph_edges_written_total";
const DELETED: &str = "nodedb_graph_edges_deleted_total";

/// Fetch one unlabelled metric value from `/metrics` by exact name.
async fn fetch_metric(http_port: u16, name: &str) -> u64 {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("connect to /metrics");
    let req = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.expect("write metrics request");
    let mut body = String::new();
    stream
        .read_to_string(&mut body)
        .await
        .expect("read metrics response");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix(name)
            && let Some(value) = rest.strip_prefix(' ')
            && let Ok(v) = value.trim().parse::<u64>()
        {
            return v;
        }
    }
    panic!("metric {name} not found in /metrics response: {body}");
}

/// Wait until `name` reaches `expected`, then check it stays there. A
/// committed transaction's homes apply before the statement answers, so the
/// wait covers only the scrape.
async fn expect_metric(http_port: u16, name: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut value = fetch_metric(http_port, name).await;
    while value < expected && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        value = fetch_metric(http_port, name).await;
    }
    assert_eq!(value, expected, "{name}");
}

fn edges(prefix: &str, n: usize) -> String {
    (0..n)
        .map(|i| format!("FROM '{prefix}src{i}' TO '{prefix}dst{i}' TYPE 'L'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_write_counters_count_each_applied_edge_once() {
    let server = TestServer::start().await;
    let port = server.http_port;
    server.exec("CREATE COLLECTION gm").await.unwrap();

    let written = fetch_metric(port, WRITTEN).await;
    let deleted = fetch_metric(port, DELETED).await;

    // One edge.
    server
        .exec("GRAPH INSERT EDGE IN 'gm' FROM 'a' TO 'b' TYPE 'L'")
        .await
        .unwrap();
    expect_metric(port, WRITTEN, written + 1).await;

    // Five edges in one statement, spread over many vShards.
    server
        .exec(&format!("GRAPH INSERT EDGE IN 'gm' {}", edges("m", 5)))
        .await
        .unwrap();
    expect_metric(port, WRITTEN, written + 6).await;

    // A live edge, then the same edge once it is gone.
    server
        .exec("GRAPH DELETE EDGE IN 'gm' FROM 'a' TO 'b' TYPE 'L'")
        .await
        .unwrap();
    expect_metric(port, DELETED, deleted + 1).await;
    server
        .exec("GRAPH DELETE EDGE IN 'gm' FROM 'a' TO 'b' TYPE 'L'")
        .await
        .unwrap();
    expect_metric(port, DELETED, deleted + 1).await;

    // A committed transaction counts its edges once it commits.
    server.exec("BEGIN").await.unwrap();
    for i in 0..3 {
        server
            .exec(&format!(
                "GRAPH DELETE EDGE IN 'gm' FROM 'msrc{i}' TO 'mdst{i}' TYPE 'L'"
            ))
            .await
            .unwrap();
    }
    server
        .exec(&format!("GRAPH INSERT EDGE IN 'gm' {}", edges("t", 2)))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    expect_metric(port, DELETED, deleted + 4).await;
    expect_metric(port, WRITTEN, written + 8).await;

    // A rolled-back transaction applies nothing and counts nothing.
    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("GRAPH INSERT EDGE IN 'gm' {}", edges("r", 3)))
        .await
        .unwrap();
    server
        .exec("GRAPH DELETE EDGE IN 'gm' FROM 'msrc3' TO 'mdst3' TYPE 'L'")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();
    expect_metric(port, WRITTEN, written + 8).await;
    expect_metric(port, DELETED, deleted + 4).await;
}
