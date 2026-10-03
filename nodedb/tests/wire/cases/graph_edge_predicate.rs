// SPDX-License-Identifier: BUSL-1.1

//! `EDGE WHERE` on `GRAPH TRAVERSE` and `GRAPH PATH`, edge properties in the
//! traversal result, and the edges the last admitted level records.
//!
//! The seeded graphs give each predicate one edge it admits and one it
//! rejects on the same walk, so a predicate that is ignored, inverted, or
//! tested against the wrong orientation reaches a node it must not.

use std::collections::BTreeSet;

use crate::harness::TestServer;

/// `a -[LINK score 9]-> b -[LINK score 1]-> c` and
/// `a -[LINK score 1]-> d -[LINK score 9]-> e`.
async fn seed_scored(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (src, dst, score) in [("a", "b", 9), ("b", "c", 1), ("a", "d", 1), ("d", "e", 9)] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'LINK' \
                 PROPERTIES {{ score: {score} }}"
            ))
            .await
            .unwrap();
    }
}

/// The JSON of a one-cell `result` answer.
async fn result_json(server: &TestServer, sql: &str) -> serde_json::Value {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let [text] = rows.as_slice() else {
        panic!("{sql}: expected one result row, got {rows:?}");
    };
    serde_json::from_str(text).unwrap_or_else(|e| panic!("{sql}: result is not JSON: {e}"))
}

fn node_ids(subgraph: &serde_json::Value) -> BTreeSet<String> {
    subgraph["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|node| node["id"].as_str().expect("node id").to_string())
        .collect()
}

/// `(from, label, to)` of every result edge.
fn edges(subgraph: &serde_json::Value) -> BTreeSet<(String, String, String)> {
    subgraph["edges"]
        .as_array()
        .expect("edges array")
        .iter()
        .map(|edge| {
            (
                edge["from"].as_str().expect("from").to_string(),
                edge["label"].as_str().expect("label").to_string(),
                edge["to"].as_str().expect("to").to_string(),
            )
        })
        .collect()
}

fn edge(from: &str, label: &str, to: &str) -> (String, String, String) {
    (from.into(), label.into(), to.into())
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_crosses_only_edges_the_predicate_admits() {
    let server = TestServer::start().await;
    seed_scored(&server, "ep_trav").await;

    let got = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_trav' FROM 'a' DEPTH 2 EDGE WHERE score > 5",
    )
    .await;
    assert_eq!(node_ids(&got), set(&["a", "b"]), "{got}");
    assert_eq!(
        edges(&got),
        BTreeSet::from([edge("a", "LINK", "b")]),
        "{got}"
    );

    let unfiltered = result_json(&server, "GRAPH TRAVERSE IN 'ep_trav' FROM 'a' DEPTH 2").await;
    assert_eq!(
        node_ids(&unfiltered),
        set(&["a", "b", "c", "d", "e"]),
        "{unfiltered}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_edges_carry_their_properties() {
    let server = TestServer::start().await;
    seed_scored(&server, "ep_props").await;
    server
        .exec("GRAPH INSERT EDGE IN 'ep_props' FROM 'b' TO 'bare' TYPE 'LINK'")
        .await
        .unwrap();

    let got = result_json(&server, "GRAPH TRAVERSE IN 'ep_props' FROM 'a' DEPTH 2").await;
    for entry in got["edges"].as_array().expect("edges") {
        let (from, to) = (entry["from"].as_str(), entry["to"].as_str());
        let expected = match (from, to) {
            (Some("a"), Some("b")) | (Some("d"), Some("e")) => serde_json::json!({ "score": 9 }),
            (Some("b"), Some("c")) | (Some("a"), Some("d")) => serde_json::json!({ "score": 1 }),
            (Some("b"), Some("bare")) => serde_json::json!({}),
            other => panic!("unexpected edge {other:?} in {got}"),
        };
        assert_eq!(entry["properties"], expected, "{got}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incoming_and_both_directions_test_each_edge_in_its_stored_orientation() {
    let server = TestServer::start().await;
    seed_scored(&server, "ep_dir").await;

    // From `c` inward: `b -> c` (score 1) passes, `a -> b` (score 9) fails.
    let inward = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_dir' FROM 'c' DEPTH 2 DIRECTION in EDGE WHERE score < 5",
    )
    .await;
    assert_eq!(node_ids(&inward), set(&["b", "c"]), "{inward}");
    assert_eq!(edges(&inward), BTreeSet::from([edge("b", "LINK", "c")]));

    // From `b` both ways: `a -> b` (score 9) passes, `b -> c` (score 1) fails.
    let both = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_dir' FROM 'b' DEPTH 2 DIRECTION both EDGE WHERE score > 5",
    )
    .await;
    assert_eq!(node_ids(&both), set(&["a", "b"]), "{both}");
    assert_eq!(edges(&both), BTreeSet::from([edge("a", "LINK", "b")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_follows_only_admitted_edges() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ep_path").await.unwrap();
    for (src, dst, score) in [
        ("a", "x", 1),
        ("x", "z", 1),
        ("a", "y", 9),
        ("y", "w", 9),
        ("w", "z", 9),
    ] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'ep_path' FROM '{src}' TO '{dst}' TYPE 'ROAD' \
                 PROPERTIES '{{\"score\": {score}}}'"
            ))
            .await
            .unwrap();
    }

    let shortest = result_json(
        &server,
        "GRAPH PATH IN 'ep_path' FROM 'a' TO 'z' MAX_DEPTH 6",
    )
    .await;
    assert_eq!(shortest, serde_json::json!(["a", "x", "z"]));

    let filtered = result_json(
        &server,
        "GRAPH PATH IN 'ep_path' FROM 'a' TO 'z' MAX_DEPTH 6 EDGE WHERE score > 5",
    )
    .await;
    assert_eq!(filtered, serde_json::json!(["a", "y", "w", "z"]));

    let none = result_json(
        &server,
        "GRAPH PATH IN 'ep_path' FROM 'a' TO 'z' MAX_DEPTH 6 EDGE WHERE score > 100",
    )
    .await;
    assert_eq!(none, serde_json::json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_without_properties_evaluates_as_an_empty_object() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ep_bare").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'ep_bare' FROM 'p' TO 'q' TYPE 'L'")
        .await
        .unwrap();

    let null_test = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_bare' FROM 'p' DEPTH 1 EDGE WHERE missing IS NULL",
    )
    .await;
    assert_eq!(node_ids(&null_test), set(&["p", "q"]), "{null_test}");

    let ordered = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_bare' FROM 'p' DEPTH 1 EDGE WHERE score < 5",
    )
    .await;
    assert_eq!(node_ids(&ordered), set(&["p"]), "{ordered}");
}

/// A `_from` / `_to` document edge stores its `weight` in the same property
/// encoding as a `GRAPH INSERT EDGE`, so one predicate reads both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn implicit_and_dsl_edges_share_one_property_encoding() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ep_implicit").await.unwrap();
    server
        .exec("INSERT INTO ep_implicit { id: 'e1', _from: 'm', _to: 'n', _type: 'W', weight: 3.0 }")
        .await
        .unwrap();
    server
        .exec(
            "GRAPH INSERT EDGE IN 'ep_implicit' FROM 'm' TO 'o' TYPE 'W' \
             PROPERTIES '{\"weight\": 1.5}'",
        )
        .await
        .unwrap();

    let heavy = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_implicit' FROM 'm' DEPTH 1 EDGE WHERE weight > 2",
    )
    .await;
    assert_eq!(node_ids(&heavy), set(&["m", "n"]), "{heavy}");

    let light = result_json(
        &server,
        "GRAPH TRAVERSE IN 'ep_implicit' FROM 'm' DEPTH 1 EDGE WHERE weight < 2",
    )
    .await;
    assert_eq!(node_ids(&light), set(&["m", "o"]), "{light}");
}

/// The last admitted level is not expanded, yet its edges among admitted
/// nodes belong to the subgraph. An edge leaving the admitted set does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_level_records_its_edges_among_admitted_nodes() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ep_bound").await.unwrap();
    for (src, dst) in [("a", "b"), ("a", "c"), ("b", "c"), ("c", "a"), ("c", "x")] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'ep_bound' FROM '{src}' TO '{dst}' TYPE 'L'"
            ))
            .await
            .unwrap();
    }

    let got = result_json(&server, "GRAPH TRAVERSE IN 'ep_bound' FROM 'a' DEPTH 1").await;
    assert_eq!(node_ids(&got), set(&["a", "b", "c"]), "{got}");
    assert_eq!(
        edges(&got),
        BTreeSet::from([
            edge("a", "L", "b"),
            edge("a", "L", "c"),
            edge("b", "L", "c"),
            edge("c", "L", "a"),
        ]),
        "{got}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_where_is_refused_outside_traverse_and_path() {
    let server = TestServer::start().await;
    seed_scored(&server, "ep_refuse").await;

    server
        .expect_error(
            "GRAPH NEIGHBORS IN 'ep_refuse' OF 'a' EDGE WHERE score > 5",
            "does not accept EDGE WHERE",
        )
        .await;
    server
        .expect_error(
            "GRAPH TRAVERSE IN 'ep_refuse' FROM 'a' EDGE WHERE score > other",
            "EDGE WHERE",
        )
        .await;
}
