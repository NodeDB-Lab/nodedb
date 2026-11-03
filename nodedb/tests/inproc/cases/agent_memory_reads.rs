// SPDX-License-Identifier: BUSL-1.1

//! Reads an agent's memory layer relies on: one-hop neighbors across homes
//! and a bare `SEARCH` as the list of an `IN` predicate.
//!
//! A cross-shard edge is stored on both endpoints' cores. A neighbor read
//! gathers every core, so each node's edges come from its home core only.

use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_neighbor_read_returns_each_edge_once_whatever_the_endpoint_homes() {
    let server = TestServer::start().await;
    // A chain long enough that endpoints land on different vShards and cores.
    for i in 0..24 {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'chain' FROM 'n{i}' TO 'n{}' TYPE 'next'",
                i + 1
            ))
            .await
            .unwrap();
    }
    for i in 0..24 {
        let out = server
            .query_text(&format!(
                "GRAPH NEIGHBORS IN 'chain' OF 'n{i}' DIRECTION out"
            ))
            .await
            .unwrap();
        let expected = format!(r#"[{{"label":"next","node":"n{}"}}]"#, i + 1);
        assert_eq!(out, vec![expected], "out edges of n{i}");

        let back = server
            .query_text(&format!(
                "GRAPH NEIGHBORS IN 'chain' OF 'n{}' DIRECTION in",
                i + 1
            ))
            .await
            .unwrap();
        let expected = format!(r#"[{{"label":"next","node":"n{i}"}}]"#);
        assert_eq!(back, vec![expected], "in edges of n{}", i + 1);
    }
}

async fn seed_facts(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION facts_in (\
               id STRING PRIMARY KEY, \
               fact STRING NOT NULL, \
               embedding VECTOR(4)) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_facts_in ON facts_in METRIC cosine DIM 4")
        .await
        .unwrap();
    for (id, vector) in [
        ("f1", "0.1, 0.9, 0.0, 0.1"),
        ("f2", "0.0, 0.2, 0.8, 0.1"),
        ("f3", "0.9, 0.1, 0.0, 0.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO facts_in (id, fact, embedding) VALUES ('{id}', 'fact {id}', ARRAY[{vector}])"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bare_search_is_the_list_of_an_in_predicate() {
    let server = TestServer::start().await;
    seed_facts(&server).await;

    let nearest = server
        .query_text(
            "SELECT id FROM facts_in WHERE id IN \
             (SEARCH facts_in USING VECTOR(embedding, ARRAY[0.1, 0.85, 0.0, 0.1], 1))",
        )
        .await
        .unwrap();
    assert_eq!(nearest, vec!["f1".to_string()]);

    let mut rest = server
        .query_text(
            "SELECT id FROM facts_in WHERE id NOT IN \
             (SEARCH facts_in USING VECTOR(embedding, ARRAY[0.1, 0.85, 0.0, 0.1], 1))",
        )
        .await
        .unwrap();
    rest.sort();
    assert_eq!(rest, vec!["f2".to_string(), "f3".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_search_subquery_joins_on_its_document_columns() {
    let server = TestServer::start().await;
    seed_facts(&server).await;

    let joined = server
        .query_text(
            "SELECT a.id FROM facts_in a JOIN \
             (SELECT * FROM facts_in ORDER BY vector_distance(embedding, ARRAY[0.1, 0.85, 0.0, 0.1]) LIMIT 2) b \
             ON a.id = b.id",
        )
        .await
        .unwrap();
    assert_eq!(joined.len(), 2, "the two nearest facts: {joined:?}");
    assert!(joined.contains(&"f1".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_reads_a_collection_made_only_by_edge_inserts() {
    let server = TestServer::start().await;
    server
        .exec("GRAPH INSERT EDGE IN 'edge_only' FROM 'a' TO 'b' TYPE 'owns'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'edge_only' FROM 'b' TO 'c' TYPE 'depends_on'")
        .await
        .unwrap();
    let rows = server
        .query_rows("MATCH (x)-[:owns]->(y)-[:depends_on]->(z) IN 'edge_only' RETURN x, y, z")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]]
    );
}
