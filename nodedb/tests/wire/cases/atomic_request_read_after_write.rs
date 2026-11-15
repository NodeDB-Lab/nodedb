// SPDX-License-Identifier: BUSL-1.1

//! A read after a write in one request of several statements answers with the
//! state the write left, on every engine.
//!
//! A request of several statements with a write runs in one implicit
//! transaction. Its reads then see the writes before them through the staging
//! overlay. A read the overlay does not cover answers the state before the
//! write.
//!
//! Each cell sends its requests to a server with a lowered
//! `max_query_result_bytes`:
//!
//! - Read: a write, then a read of that write. The read answers the write.
//! - Undo: a second write, then a read that exceeds the limit. The request
//!   errors, so the second write must be absent. On the per-task route the
//!   write applies before the read errors, and it stays.
//!
//! Each cell runs on HTTP, NDJSON, WebSocket RPC and native SQL. The graph
//! cell reads its lone `MATCH` statements through HTTP on NDJSON and
//! WebSocket RPC. `GRAPH INSERT EDGE` and `MATCH` are DDL to the request
//! route, so the graph requests take the sequential runner.

use super::atomic_transport::Transport;
use crate::harness::TestServer;

/// Bytes the server lets one query result carry.
const LIMIT: u64 = 4096;
/// The collection whose full read exceeds [`LIMIT`].
const BIG: &str = "ra_big";

/// One engine under test.
struct Case {
    engine: &'static str,
    ddl: &'static [&'static str],
    /// A write, a `;`, then a read of that write.
    read_request: &'static str,
    /// The text the read answers.
    read_marker: &'static str,
    /// A later read of the first write, in its own request. It holds
    /// `read_marker` when the write is committed and visible.
    control: &'static str,
    /// A second write. The undo request appends the erroring read.
    undo_write: &'static str,
    /// A read that would show the second write.
    probe: &'static str,
    /// The text the probe holds only when the second write applied.
    undo_marker: &'static str,
}

async fn run(case: Case, transport: Transport) {
    let engine = case.engine;
    // NDJSON and WebSocket RPC refuse a lone `MATCH`. A request of several
    // statements runs it. So the later reads, which are lone statements, go
    // through HTTP for the graph engine.
    let reader = if engine == "graph" && matches!(transport, Transport::Ndjson | Transport::Ws) {
        Transport::Http
    } else {
        transport
    };
    let server = TestServer::start_multicores_with_result_limit(1, LIMIT).await;
    server
        .exec(&format!(
            "CREATE COLLECTION {BIG} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("create the big collection");
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
    for statement in case.ddl {
        server
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{engine}: {statement}: {e}"));
    }

    let (status, body) = transport.query(&server, case.read_request).await;
    eprintln!(
        "RA {transport:?} {engine} read status={status} body={}",
        &body[..body.len().min(400)]
    );
    assert_eq!(status, 200, "{engine}: {}: {body}", case.read_request);
    assert!(
        body.contains(case.read_marker),
        "{engine}: the read must answer {:?}, got {body}",
        case.read_marker
    );

    let (status, body) = reader.query(&server, case.control).await;
    assert_eq!(status, 200, "{engine}: {}: {body}", case.control);
    assert!(
        body.contains(case.read_marker),
        "{engine}: the committed write must be visible to a later request, got {body}"
    );

    let undo = format!("{}; SELECT * FROM {BIG}", case.undo_write);
    let (status, body) = transport.query(&server, &undo).await;
    eprintln!(
        "RA {transport:?} {engine} undo status={status} body={}",
        &body[..body.len().min(300)]
    );
    assert_ne!(
        status, 200,
        "{engine}: the trailing read must error: {body}"
    );
    let (status, body) = reader.query(&server, case.probe).await;
    eprintln!(
        "RA {transport:?} {engine} probe status={status} body={}",
        &body[..body.len().min(300)]
    );
    assert_eq!(status, 200, "{engine}: {}: {body}", case.probe);
    assert!(
        !body.contains(case.undo_marker),
        "{engine}: an errored request must not apply its write, got {body}"
    );
}

fn document_strict_point_read_sees_the_write() -> Case {
    Case {
        engine: "document_strict point",
        ddl: &[
            "CREATE COLLECTION ra_ds (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')",
        ],
        read_request: "INSERT INTO ra_ds (id, v) VALUES ('k1', 'mark-ds'); \
                       SELECT v FROM ra_ds WHERE id = 'k1'",
        read_marker: "mark-ds",
        control: "SELECT v FROM ra_ds WHERE id = 'k1'",
        undo_write: "INSERT INTO ra_ds (id, v) VALUES ('k2', 'undo-ds')",
        probe: "SELECT v FROM ra_ds WHERE id = 'k2'",
        undo_marker: "undo-ds",
    }
}

fn document_strict_scan_sees_the_write() -> Case {
    Case {
        engine: "document_strict scan",
        ddl: &[
            "CREATE COLLECTION ra_dss (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')",
        ],
        read_request: "INSERT INTO ra_dss (id, v) VALUES ('k1', 'mark-dss'); \
                       SELECT id FROM ra_dss WHERE v = 'mark-dss'",
        read_marker: "k1",
        control: "SELECT id FROM ra_dss WHERE v = 'mark-dss'",
        undo_write: "INSERT INTO ra_dss (id, v) VALUES ('k2', 'undo-dss')",
        probe: "SELECT id FROM ra_dss WHERE v = 'undo-dss'",
        undo_marker: "k2",
    }
}

fn document_schemaless_point_read_sees_the_write() -> Case {
    Case {
        engine: "document_schemaless point",
        ddl: &["CREATE COLLECTION ra_dl WITH (engine='document_schemaless')"],
        read_request: "INSERT INTO ra_dl (id, v) VALUES ('k1', 'mark-dl'); \
                       SELECT v FROM ra_dl WHERE id = 'k1'",
        read_marker: "mark-dl",
        control: "SELECT v FROM ra_dl WHERE id = 'k1'",
        undo_write: "INSERT INTO ra_dl (id, v) VALUES ('k2', 'undo-dl')",
        probe: "SELECT v FROM ra_dl WHERE id = 'k2'",
        undo_marker: "undo-dl",
    }
}

fn document_schemaless_scan_sees_the_write() -> Case {
    Case {
        engine: "document_schemaless scan",
        ddl: &["CREATE COLLECTION ra_dls WITH (engine='document_schemaless')"],
        read_request: "INSERT INTO ra_dls (id, v) VALUES ('k1', 'mark-dls'); \
                       SELECT id FROM ra_dls WHERE v = 'mark-dls'",
        read_marker: "k1",
        control: "SELECT id FROM ra_dls WHERE v = 'mark-dls'",
        undo_write: "INSERT INTO ra_dls (id, v) VALUES ('k2', 'undo-dls')",
        probe: "SELECT id FROM ra_dls WHERE v = 'undo-dls'",
        undo_marker: "k2",
    }
}

fn fts_search_sees_the_write() -> Case {
    Case {
        engine: "fts",
        ddl: &["CREATE COLLECTION ra_fts WITH (engine='document_schemaless')"],
        read_request: "INSERT INTO ra_fts (id, body) VALUES ('fts-hit', 'an elephant walks'); \
                       SELECT id FROM ra_fts WHERE text_match(body, 'elephant')",
        read_marker: "fts-hit",
        control: "SELECT id FROM ra_fts WHERE text_match(body, 'elephant')",
        undo_write: "INSERT INTO ra_fts (id, body) VALUES ('fts-undo', 'a giraffe walks')",
        probe: "SELECT id FROM ra_fts WHERE text_match(body, 'giraffe')",
        undo_marker: "fts-undo",
    }
}

fn vector_search_sees_the_write() -> Case {
    Case {
        engine: "vector",
        ddl: &[
            "CREATE COLLECTION ra_vec TYPE document",
            "CREATE VECTOR INDEX ra_vec_idx ON ra_vec (embedding) METRIC cosine DIM 4",
        ],
        read_request: "INSERT INTO ra_vec (id, embedding) VALUES ('vec-hit', ARRAY[1.0,0.0,0.0,0.0]); \
                       SELECT id FROM ra_vec \
                       ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 1",
        read_marker: "vec-hit",
        control: "SELECT id FROM ra_vec ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 1",
        undo_write: "INSERT INTO ra_vec (id, embedding) VALUES ('vec-undo', ARRAY[0.0,1.0,0.0,0.0])",
        probe: "SELECT id FROM ra_vec \
                ORDER BY vector_distance(embedding, ARRAY[0.0,1.0,0.0,0.0]) LIMIT 1",
        undo_marker: "vec-undo",
    }
}

fn graph_match_sees_the_write() -> Case {
    Case {
        engine: "graph",
        ddl: &["CREATE COLLECTION ra_g"],
        read_request: "GRAPH INSERT EDGE IN 'ra_g' FROM 'graph-hit' TO 'graph-hit' TYPE 'l'; \
                       MATCH (x)-[:l]->(y) RETURN x, y",
        read_marker: "graph-hit",
        control: "MATCH (x)-[:l]->(y) RETURN x, y",
        undo_write: "GRAPH INSERT EDGE IN 'ra_g' FROM 'graph-undo' TO 'graph-undo' TYPE 'u'",
        probe: "MATCH (x)-[:u]->(y) RETURN x, y",
        undo_marker: "graph-undo",
    }
}

fn aggregate_sees_the_write() -> Case {
    Case {
        engine: "aggregate",
        ddl: &[
            "CREATE COLLECTION ra_agg (id STRING NOT NULL PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
            "INSERT INTO ra_agg (id, n) VALUES ('a', 1)",
            "INSERT INTO ra_agg (id, n) VALUES ('b', 2)",
        ],
        read_request: "INSERT INTO ra_agg (id, n) VALUES ('c', 100); \
                       SELECT COUNT(*), SUM(n) FROM ra_agg",
        read_marker: "103",
        control: "SELECT COUNT(*), SUM(n) FROM ra_agg",
        undo_write: "INSERT INTO ra_agg (id, n) VALUES ('d', 1000)",
        probe: "SELECT COUNT(*), SUM(n) FROM ra_agg",
        undo_marker: "1103",
    }
}

fn timeseries_scan_sees_the_write() -> Case {
    Case {
        engine: "timeseries",
        ddl: &[
            "CREATE COLLECTION ra_ts COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
                WITH (engine='timeseries')",
        ],
        read_request: "INSERT INTO ra_ts (id, ts, v) VALUES ('s4', 4000, 4444); \
                       SELECT v FROM ra_ts",
        read_marker: "4444",
        control: "SELECT v FROM ra_ts",
        undo_write: "INSERT INTO ra_ts (id, ts, v) VALUES ('s5', 5000, 5555)",
        probe: "SELECT v FROM ra_ts",
        undo_marker: "5555",
    }
}

macro_rules! cells {
    ($($case:ident),* $(,)?) => {$(
        mod $case {
            use super::*;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn http() {
                run(super::$case(), Transport::Http).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ndjson() {
                run(super::$case(), Transport::Ndjson).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ws() {
                run(super::$case(), Transport::Ws).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn native() {
                run(super::$case(), Transport::Native).await;
            }
        }
    )*};
}

cells!(
    document_strict_point_read_sees_the_write,
    document_strict_scan_sees_the_write,
    document_schemaless_point_read_sees_the_write,
    document_schemaless_scan_sees_the_write,
    fts_search_sees_the_write,
    vector_search_sees_the_write,
    graph_match_sees_the_write,
    aggregate_sees_the_write,
    timeseries_scan_sees_the_write,
);
