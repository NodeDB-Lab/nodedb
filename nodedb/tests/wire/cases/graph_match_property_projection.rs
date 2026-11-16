// SPDX-License-Identifier: BUSL-1.1

//! `MATCH ... RETURN x.id` answers one row per match.
//!
//! The whole-node projection of the same pattern is the control. A property
//! projection must return the node ids, one row per match and bounded by
//! `LIMIT`, or refuse with an error. The nodes here hold no documents, so each
//! row reads NULL; the count is what the cells check.

use crate::harness::TestServer;

async fn six_edges(server: &TestServer) {
    server.exec("CREATE COLLECTION g_proj").await.unwrap();
    for i in 1..=6 {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'g_proj' FROM 'a{i}' TO 'b{i}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_whole_node_projection_answers_one_row_per_match() {
    let server = TestServer::start().await;
    six_edges(&server).await;
    let rows = server
        .query_rows("MATCH (x)-[:l]->(y) IN 'g_proj' RETURN x, y")
        .await
        .unwrap();
    assert_eq!(rows.len(), 6, "one row per edge: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_property_projection_answers_one_row_per_match_or_refuses() {
    let server = TestServer::start().await;
    six_edges(&server).await;
    if let Ok(rows) = server
        .query_rows("MATCH (x)-[:l]->(y) IN 'g_proj' RETURN x.id")
        .await
    {
        assert_eq!(rows.len(), 6, "one row per edge: {rows:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_property_projection_honours_limit_or_refuses() {
    let server = TestServer::start().await;
    six_edges(&server).await;
    if let Ok(rows) = server
        .query_rows("MATCH (x)-[:l]->(y) IN 'g_proj' RETURN x.id LIMIT 3")
        .await
    {
        assert_eq!(rows.len(), 3, "LIMIT 3 bounds the rows: {rows:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_property_projection_reads_a_strict_collection() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION g_strict (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for i in 1..=3 {
        server
            .exec(&format!(
                "INSERT INTO g_strict (id, name) VALUES ('n{i}', 'name{i}')"
            ))
            .await
            .unwrap();
        let to = i % 3 + 1;
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'g_strict' FROM 'n{i}' TO 'n{to}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }
    let rows = server
        .query_rows("MATCH (x)-[:l]->(y) IN 'g_strict' RETURN x.name")
        .await
        .unwrap();
    let mut names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["name1", "name2", "name3"], "rows: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_matches_that_project_to_equal_rows_stay_two_rows() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION g_equal (id TEXT PRIMARY KEY, kind TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for id in ["p1", "p2", "p3"] {
        server
            .exec(&format!(
                "INSERT INTO g_equal (id, kind) VALUES ('{id}', 'same')"
            ))
            .await
            .unwrap();
    }
    for (a, b) in [("p1", "p2"), ("p2", "p3")] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'g_equal' FROM '{a}' TO '{b}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }
    let rows = server
        .query_rows("MATCH (x)-[:l]->(y) IN 'g_equal' RETURN x.kind")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["same".to_string()]; 2], "one row per match");
}
