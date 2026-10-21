// SPDX-License-Identifier: BUSL-1.1

//! `AS OF SYSTEM TIME` on every graph read.
//!
//! Each test writes a graph, takes a timestamp, then deletes one edge and
//! adds another. A read at the timestamp answers the graph before the change.
//! A read without `AS OF` answers the graph after it.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// Let the wall clock move past the last write's millisecond.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// `a -l-> b`, `a -l-> c`, then a cut, then `a -l-> b` deleted and
/// `a -l-> d` added. Returns the cut's ms timestamp.
async fn changed_graph(server: &TestServer, coll: &str) -> i64 {
    server
        .exec(&format!("CREATE COLLECTION {coll}"))
        .await
        .expect("create collection");
    for dst in ["b", "c"] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{coll}' FROM 'a' TO '{dst}' TYPE 'l'"
            ))
            .await
            .expect("insert edge");
    }
    tick().await;
    let cut = now_ms();
    tick().await;
    server
        .exec(&format!(
            "GRAPH DELETE EDGE IN '{coll}' FROM 'a' TO 'b' TYPE 'l'"
        ))
        .await
        .expect("delete edge");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{coll}' FROM 'a' TO 'd' TYPE 'l'"
        ))
        .await
        .expect("insert edge");
    cut
}

fn has(blob: &str, node: &str) -> bool {
    blob.contains(&format!("\"{node}\""))
}

/// The blob names exactly the nodes in `present` of the nodes in `all`.
fn assert_nodes(blob: &str, present: &[&str], all: &[&str], what: &str) {
    for node in all {
        assert_eq!(
            has(blob, node),
            present.contains(node),
            "{what}: node {node} presence is wrong; got {blob}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbors_as_of_reads_the_graph_before_the_change() {
    let server = TestServer::start().await;
    let cut = changed_graph(&server, "bt_nbr").await;
    let all = ["b", "c", "d"];

    let before = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'bt_nbr' OF 'a' AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical neighbors")
        .join("");
    assert_nodes(&before, &["b", "c"], &all, "neighbors as of the cut");

    let now = server
        .query_text("GRAPH NEIGHBORS IN 'bt_nbr' OF 'a'")
        .await
        .expect("current neighbors")
        .join("");
    assert_nodes(&now, &["c", "d"], &all, "current neighbors");

    // A read at a time before every write sees no edge.
    let empty = server
        .query_text("GRAPH NEIGHBORS IN 'bt_nbr' OF 'a' AS OF SYSTEM TIME 1000")
        .await
        .expect("neighbors before every write")
        .join("");
    assert_nodes(&empty, &[], &all, "neighbors before every write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_and_path_as_of_read_the_graph_before_the_change() {
    let server = TestServer::start().await;
    let cut = changed_graph(&server, "bt_walk").await;
    server
        .exec("GRAPH INSERT EDGE IN 'bt_walk' FROM 'b' TO 'e' TYPE 'l'")
        .await
        .expect("insert edge after the cut");
    let all = ["b", "c", "d", "e"];

    // `AS OF` stands anywhere after the command words.
    let before = server
        .query_text(&format!(
            "GRAPH TRAVERSE FROM 'a' IN 'bt_walk' AS OF SYSTEM TIME {cut} DEPTH 2"
        ))
        .await
        .expect("historical traverse")
        .join("");
    assert_nodes(&before, &["b", "c"], &all, "traverse as of the cut");

    let now = server
        .query_text("GRAPH TRAVERSE FROM 'a' IN 'bt_walk' DEPTH 2")
        .await
        .expect("current traverse")
        .join("");
    assert_nodes(&now, &["c", "d"], &all, "current traverse");

    let path_before = server
        .query_text(&format!(
            "GRAPH PATH IN 'bt_walk' FROM 'a' TO 'b' MAX_DEPTH 3 AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical path")
        .join("");
    assert!(
        has(&path_before, "a") && has(&path_before, "b"),
        "a -> b existed at the cut; got {path_before}"
    );
    let path_now = server
        .query_text("GRAPH PATH IN 'bt_walk' FROM 'a' TO 'd' MAX_DEPTH 3")
        .await
        .expect("current path")
        .join("");
    assert!(has(&path_now, "d"), "a -> d exists now; got {path_now}");
    let path_d_before = server
        .query_text(&format!(
            "GRAPH PATH IN 'bt_walk' FROM 'a' TO 'd' MAX_DEPTH 3 AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical path to a later node")
        .join("");
    assert!(
        !has(&path_d_before, "d"),
        "d did not exist at the cut; got {path_d_before}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_as_of_reads_the_graph_before_the_change() {
    let server = TestServer::start().await;
    let cut = changed_graph(&server, "bt_match").await;

    let mut before = server
        .query_text(&format!(
            "MATCH (x)-[:l]->(y) IN 'bt_match' AS OF SYSTEM TIME {cut} RETURN y"
        ))
        .await
        .expect("historical match");
    before.sort();
    assert_eq!(before, vec!["b".to_string(), "c".to_string()]);

    let mut now = server
        .query_text("MATCH (x)-[:l]->(y) IN 'bt_match' RETURN y")
        .await
        .expect("current match");
    now.sort();
    assert_eq!(now, vec!["c".to_string(), "d".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn algorithms_as_of_run_over_the_graph_before_the_change() {
    let server = TestServer::start().await;
    let cut = changed_graph(&server, "bt_algo").await;

    let mut ranked_before = server
        .query_text(&format!(
            "GRAPH ALGO PAGERANK ON bt_algo ITERATIONS 10 AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical pagerank");
    ranked_before.sort();
    assert_eq!(ranked_before, vec!["a", "b", "c"]);

    let mut ranked_now = server
        .query_text("GRAPH ALGO PAGERANK ON bt_algo ITERATIONS 10")
        .await
        .expect("current pagerank");
    ranked_now.sort();
    assert_eq!(ranked_now, vec!["a", "c", "d"]);

    let mut components_before = server
        .query_text(&format!(
            "GRAPH ALGO WCC ON bt_algo AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical wcc");
    components_before.sort();
    assert_eq!(components_before, vec!["a", "b", "c"]);

    // A label-filtered historical run reads the edge store, not the cache.
    let mut degree_before = server
        .query_text(&format!(
            "GRAPH ALGO DEGREE ON bt_algo EDGE_LABEL 'l' AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("historical degree");
    degree_before.sort();
    assert_eq!(degree_before, vec!["a", "b", "c"]);
}

/// Repeated reads at one cut answer the same graph, and a write after the
/// cut leaves that answer as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_answers_the_same_graph_after_later_writes() {
    let server = TestServer::start().await;
    let cut = changed_graph(&server, "bt_stable").await;
    let query = format!("GRAPH NEIGHBORS IN 'bt_stable' OF 'a' AS OF SYSTEM TIME {cut}");
    let first = server
        .query_text(&query)
        .await
        .expect("first read")
        .join("");
    server
        .exec("GRAPH INSERT EDGE IN 'bt_stable' FROM 'a' TO 'z' TYPE 'l'")
        .await
        .expect("insert edge after the cut");
    server
        .exec("GRAPH DELETE EDGE IN 'bt_stable' FROM 'a' TO 'c' TYPE 'l'")
        .await
        .expect("delete edge after the cut");
    let second = server
        .query_text(&query)
        .await
        .expect("second read")
        .join("");
    assert_nodes(&second, &["b", "c"], &["b", "c", "d", "z"], "second read");
    assert_nodes(&first, &["b", "c"], &["b", "c", "d", "z"], "first read");
}
