//! Integration coverage for `GRAPH ALGO` output contracts.
//!
//! The Data Plane stores nodes under tenant-scoped keys
//! (`<tid>:<name>`) so that each tenant's subgraph is isolated in the
//! shared CSR index. That scoping is an internal addressing concern —
//! it must not cross the Data Plane → Control Plane boundary. Every
//! algorithm result emitter in `engine/graph/algo/*.rs` currently
//! calls `csr.node_name(...)` (which returns the scoped key) and
//! pushes it straight into `AlgoResultBatch`, leaking the prefix to
//! clients.
//!
//! This is the same design flaw the MATCH path had and fixed: the
//! API boundary is responsible for returning user-visible ids. These
//! tests assert that contract for every algorithm whose result schema
//! has a `node_id` text column:
//!
//! PAGERANK, COMMUNITY (LabelPropagation), WCC, LOUVAIN, DEGREE,
//! KCORE, CLOSENESS — and a round-trip test that a PAGERANK result
//! row is directly usable as an id in `WHERE id = ...` against the
//! home collection (the reported downstream-impact scenario).
//!
//! Diameter is intentionally excluded (global scalar, no node_id).
//! Triangles global mode emits a `__global__` sentinel that is not a
//! node and therefore excluded from the prefix check.

mod common;

use common::pgwire_harness::TestServer;

fn assert_first_col_unscoped(rows: &[String], algo: &str) {
    assert!(!rows.is_empty(), "{algo} must return at least one row");
    for v in rows {
        assert!(
            !v.contains(':'),
            "{algo}: node_id must be unscoped (no `<tid>:` prefix); got {v:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION memories").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'bob' TO 'carol' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO PAGERANK ON memories DAMPING 0.85 ITERATIONS 10 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    assert_first_col_unscoped(&rows, "PAGERANK");
    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    for expected in ["alice", "bob", "carol"] {
        assert!(
            got.contains(expected),
            "PAGERANK must return unscoped id {expected:?}; got {got:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_community_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION memories").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'bob' TO 'carol' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO COMMUNITY ON memories ITERATIONS 5 RESOLUTION 1.0")
        .await
        .expect("COMMUNITY must succeed");
    assert_first_col_unscoped(&rows, "COMMUNITY");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_wcc_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wcc_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO WCC ON wcc_nodes")
        .await
        .expect("WCC must succeed");
    assert_first_col_unscoped(&rows, "WCC");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_louvain_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION louv_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO LOUVAIN ON louv_nodes ITERATIONS 5 RESOLUTION 1.0")
        .await
        .expect("LOUVAIN must succeed");
    assert_first_col_unscoped(&rows, "LOUVAIN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_degree_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION deg_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO DEGREE ON deg_nodes")
        .await
        .expect("DEGREE must succeed");
    assert_first_col_unscoped(&rows, "DEGREE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_kcore_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION kc_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'c' TO 'a' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO KCORE ON kc_nodes")
        .await
        .expect("KCORE must succeed");
    assert_first_col_unscoped(&rows, "KCORE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_closeness_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION cl_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO CLOSENESS ON cl_nodes")
        .await
        .expect("CLOSENESS must succeed");
    assert_first_col_unscoped(&rows, "CLOSENESS");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_preserves_digit_prefixed_user_id() {
    // The Data Plane prepends `<tid>:` to store keys; the unscoper at
    // the exit boundary must strip *exactly that prefix*, not any
    // leading `\d+:` run. A user id that legitimately begins with
    // digits + `:` (e.g. `"77:node"` — a stringified composite key,
    // common when ids encode `shard:slug` or `year:slug`) must round-
    // trip intact. A heuristic stripper corrupts it.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION dp_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM '77:node' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO PAGERANK ON dp_nodes ITERATIONS 5 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    assert!(
        got.contains("77:node"),
        "PAGERANK must preserve digit-prefixed user id '77:node'; got {got:?}"
    );
    assert!(
        !got.contains("node"),
        "PAGERANK must not corrupt '77:node' into 'node' via heuristic strip; got {got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_node_id_is_joinable_with_collection_id() {
    // The reported downstream impact: consumers cannot feed `node_id`
    // into `WHERE id = '...'` because the scoped prefix makes it
    // non-comparable to the row id. This test drives the full
    // round-trip: INSERT rows keyed by the same ids used in edges, run
    // PAGERANK, then a SELECT on that id must resolve the real row.
    // Passes iff the algo output is directly usable as an id set.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rank_join_docs")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rank_join_docs (id, label) VALUES ('alice', 'A')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rank_join_docs (id, label) VALUES ('bob', 'B')")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();

    let rank_rows = server
        .query_text("GRAPH ALGO PAGERANK ON rank_join_docs ITERATIONS 5 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    let first = rank_rows
        .first()
        .expect("PAGERANK must return at least one row")
        .clone();
    assert!(
        !first.contains(':'),
        "PAGERANK node_id must be joinable with collection id; got scoped form {first:?}"
    );

    let joined = server
        .query_text(&format!(
            "SELECT * FROM rank_join_docs WHERE id = '{first}'"
        ))
        .await
        .expect("SELECT must succeed");
    assert_eq!(
        joined.len(),
        1,
        "PAGERANK node_id {first:?} must join against collection id column; got {joined:?}"
    );
    // Document projection returns the whole row as a JSON blob; check
    // the id the row resolved to is exactly what PAGERANK emitted.
    assert!(
        joined[0].contains(&format!("\"id\":\"{first}\"")),
        "row resolved for PAGERANK node_id {first:?} must contain matching id; got {joined:?}"
    );
}
