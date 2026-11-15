// SPDX-License-Identifier: BUSL-1.1

//! A graph `MATCH` before a write in one request, on every transport.
//!
//! The request runs in one transaction. The write applies and a later
//! request reads it. When a statement after the write errors, the write is
//! absent. NDJSON and WebSocket RPC refuse a lone `MATCH` and run one in a
//! request of several statements, so the cells read back through HTTP.

use super::atomic_transport::Transport;
use crate::harness::TestServer;

/// Bytes the server lets one query result carry.
const LIMIT: u64 = 4096;
/// The collection whose full read exceeds [`LIMIT`].
const BIG: &str = "mo_big";
const MATCH_ALL: &str = "MATCH (x)-[:l]->(y) RETURN x, y";

/// A server with one edge `s1` to `s2`, the document collection `mo_d` and
/// [`BIG`].
async fn fixture() -> TestServer {
    let server = TestServer::start_multicores_with_result_limit(1, LIMIT).await;
    for ddl in [
        "CREATE COLLECTION mo_g".to_owned(),
        "GRAPH INSERT EDGE IN 'mo_g' FROM 's1' TO 's2' TYPE 'l'".to_owned(),
        "CREATE COLLECTION mo_d (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
            .to_owned(),
        format!(
            "CREATE COLLECTION {BIG} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ),
    ] {
        server
            .exec(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    let rows: Vec<String> = (0..40)
        .map(|n| format!("('r{n}', '{}')", "x".repeat(200)))
        .collect();
    server
        .exec(&format!(
            "INSERT INTO {BIG} (id, v) VALUES {}",
            rows.join(", ")
        ))
        .await
        .expect("seed the big collection");
    server
}

/// Every edge, read by a lone `MATCH` through HTTP.
async fn edges(server: &TestServer) -> String {
    Transport::Http.send(server, MATCH_ALL).await.text
}

/// Every `mo_d` id, read by a lone statement through HTTP.
async fn documents(server: &TestServer) -> String {
    Transport::Http
        .send(server, "SELECT id FROM mo_d")
        .await
        .text
}

async fn match_then_graph_write(transport: Transport) {
    let server = fixture().await;
    let write = "GRAPH INSERT EDGE IN 'mo_g' FROM 'n1' TO 'n2' TYPE 'l'";
    let sent = transport
        .send(&server, &format!("{MATCH_ALL}; {write}"))
        .await;
    assert!(!sent.errored, "{transport:?}: {}", sent.text);
    assert!(
        !sent.text.contains("n1"),
        "{transport:?}: the MATCH reads the state before the write: {}",
        sent.text
    );
    assert!(
        edges(&server).await.contains("n1"),
        "{transport:?}: the write applies"
    );

    let failing = "GRAPH INSERT EDGE IN 'mo_g' FROM 'u1' TO 'u2' TYPE 'l'";
    let sent = transport
        .send(
            &server,
            &format!("{MATCH_ALL}; {failing}; SELECT * FROM {BIG}"),
        )
        .await;
    assert!(
        sent.errored,
        "{transport:?}: the trailing read must error: {}",
        sent.text
    );
    assert!(
        !edges(&server).await.contains("u1"),
        "{transport:?}: an errored request applies no write"
    );
}

async fn match_then_doc_write(transport: Transport) {
    let server = fixture().await;
    let sent = transport
        .send(
            &server,
            &format!("{MATCH_ALL}; INSERT INTO mo_d (id, v) VALUES ('d1', 'x')"),
        )
        .await;
    assert!(!sent.errored, "{transport:?}: {}", sent.text);
    assert!(
        documents(&server).await.contains("d1"),
        "{transport:?}: the write applies"
    );

    let sent = transport
        .send(
            &server,
            &format!(
                "{MATCH_ALL}; INSERT INTO mo_d (id, v) VALUES ('d2', 'x'); SELECT * FROM {BIG}"
            ),
        )
        .await;
    assert!(
        sent.errored,
        "{transport:?}: the trailing read must error: {}",
        sent.text
    );
    assert!(
        !documents(&server).await.contains("d2"),
        "{transport:?}: an errored request applies no write"
    );
}

macro_rules! per_transport {
    ($case:ident) => {
        mod $case {
            use super::*;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn http() {
                super::$case(Transport::Http).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ndjson() {
                super::$case(Transport::Ndjson).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ws() {
                super::$case(Transport::Ws).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn native() {
                super::$case(Transport::Native).await;
            }
        }
    };
}

per_transport!(match_then_graph_write);
per_transport!(match_then_doc_write);
