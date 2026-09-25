// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Graph engine overlay. Graph is a
//! cross-engine overlay — edges and nodes live inside any collection. Covers
//! MATCH pattern queries, GRAPH ALGO (SSSP, PageRank), and rejection of
//! `WITH (engine='graph')`.

use crate::harness::TestServer;

#[tokio::test]
async fn match_pattern_simple_path() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION graph_nodes WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO graph_nodes { id: 'alice', name: 'Alice' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_nodes { id: 'bob', name: 'Bob' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_nodes { id: 'carol', name: 'Carol' }")
        .await
        .unwrap();

    srv.exec("INSERT INTO graph_nodes { id: 'e1', _from: 'alice', _to: 'bob', _type: 'knows' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_nodes { id: 'e2', _from: 'bob', _to: 'carol', _type: 'knows' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "MATCH (a {id: 'alice'})-[:knows]->(b) IN 'graph_nodes' \
             RETURN b.id",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids.contains(&"bob"),
        "expected bob in MATCH result: {ids:?}"
    );
}

#[tokio::test]
async fn graph_algo_sssp() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION graph_sssp WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO graph_sssp { id: 'n1', label: 'start' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_sssp { id: 'n2', label: 'mid' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_sssp { id: 'n3', label: 'end' }")
        .await
        .unwrap();

    srv.exec(
        "INSERT INTO graph_sssp { id: 'e1', _from: 'n1', _to: 'n2', _type: 'road', weight: 1.0 }",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO graph_sssp { id: 'e2', _from: 'n2', _to: 'n3', _type: 'road', weight: 1.0 }",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("GRAPH ALGO SSSP ON graph_sssp SOURCE 'n1'")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "SSSP returned no rows");
}

#[tokio::test]
async fn graph_algo_pagerank() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION graph_pr WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO graph_pr { id: 'a', label: 'A' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_pr { id: 'b', label: 'B' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO graph_pr { id: 'e1', _from: 'a', _to: 'b', _type: 'link' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("GRAPH ALGO PAGERANK ON graph_pr")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "PageRank returned no rows");
}

#[tokio::test]
async fn engine_graph_flag_rejected_in_with_clause() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_graph WITH (engine='graph')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("match")
            || err.to_lowercase().contains("graph")
            || err.to_lowercase().contains("unsupported"),
        "expected graph-rejection error, got: {err}"
    );
}

/// The edge-bearing gate is read from a catalog keyed by the BARE collection
/// name, so a non-default database must still refuse an expression update to a
/// reserved edge field — the mirrored edge could not be reconciled against it.
#[tokio::test]
async fn edge_field_expression_update_rejected_in_non_default_database() {
    let (srv, _db) = TestServer::with_database("graph_edge_scope").await;
    srv.exec("CREATE COLLECTION graph_edges_nd WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO graph_edges_nd { id: 'e1', _from: 'alice', _to: 'bob', _type: 'knows' }")
        .await
        .unwrap();

    srv.expect_error(
        "UPDATE graph_edges_nd SET _from = _to WHERE id = 'e1'",
        "expression updates to reserved edge fields",
    )
    .await;
}
